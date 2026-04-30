from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import shutil
import tempfile
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Iterable
from zipfile import ZIP_DEFLATED, ZIP_STORED, ZipFile

from .config import Settings
from .models import FramePhase, JobPhase, JobRecord, RenderDevice, RenderMode, utc_now
from .store import JobStore

FRAME_RE = re.compile(r"Fra:\s*(\d+)")
SAMPLE_RE = re.compile(r"Sample\s+(\d+)\s*/\s*(\d+)")
PATH_SAMPLE_RE = re.compile(r"Path Tracing Sample\s+(\d+)\s*/\s*(\d+)")
TILE_RE = re.compile(r"Rendered\s+(\d+)\s*/\s*(\d+)\s+Tiles")
PATH_TILE_RE = re.compile(r"Path Tracing Tile\s+(\d+)\s*/\s*(\d+)")
SAVED_PATH_RE = re.compile(r"Saved:\s+'([^']+)'")
GPU_ERROR_RE = re.compile(
    r"(No compatible GPUs found|CUDA.+unavailable|OPTIX.+unavailable|is not a valid Cycles device|device type .* not available|Found no Cycles device of the specified type)",
    re.IGNORECASE,
)
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
RENDER_FARM_EVENT_PREFIX = "RENDER_FARM_EVENT "
LOG_FLUSH_INTERVAL_SECONDS = 0.5
LOG_FLUSH_LINE_COUNT = 25
VIDEO_DIRECTORY_NAME = "videos"
SCENE_INFO_FILENAME = "render-info.json"


@dataclass(slots=True)
class ProgressTracker:
    current_frame: int | None = None
    total_frames: int = 1
    current_sample: int | None = None
    total_samples: int | None = None
    current_tile: int | None = None
    total_tiles: int | None = None


class RenderRunner:
    def __init__(self, settings: Settings, store: JobStore) -> None:
        self.settings = settings
        self.store = store
        self._active_processes: dict[str, asyncio.subprocess.Process] = {}
        self._cancel_requested: set[str] = set()
        self._process_lock = asyncio.Lock()

    async def run(self, job_id: str) -> None:
        job_started_at = monotonic()
        try:
            job = await self.store.get(job_id)
            if not job or job.phase != JobPhase.queued or await self._is_cancel_requested(job_id):
                return

            job = await self.store.mutate(
                job_id,
                lambda item: self._mark_running(item),
            )
            if job.phase != JobPhase.running or await self._job_was_cancelled(job_id):
                return
            await self.store.append_log(job_id, self._perf_log("job.started", job_id=job_id))

            attempts = self._device_attempts(job.requested_device)
            collected_error = "Render failed."
            for index, device in enumerate(attempts):
                if await self._job_was_cancelled(job_id):
                    return

                job = await self.store.mutate(
                    job_id,
                    lambda item, current=device: self._prepare_attempt(item, current),
                )
                if job.phase != JobPhase.running or await self._job_was_cancelled(job_id):
                    return

                reset_started_at = monotonic()
                self._reset_output_dir(job.output_dir)
                await self.store.append_log(
                    job_id,
                    self._perf_log(
                        "outputs.reset",
                        duration_seconds=monotonic() - reset_started_at,
                        device=device,
                    ),
                )
                attempt_started_at = monotonic()
                success, combined_output, retryable_gpu_error = await self._run_attempt(job, device)
                await self.store.append_log(
                    job_id,
                    self._perf_log(
                        "attempt.finished",
                        duration_seconds=monotonic() - attempt_started_at,
                        device=device,
                        success=str(success).lower(),
                    ),
                )
                if await self._job_was_cancelled(job_id):
                    return

                if success:
                    output_scan_started_at = monotonic()
                    outputs = self._collect_outputs(job.output_dir)
                    await self.store.append_log(
                        job_id,
                        self._perf_log(
                            "outputs.scanned",
                            duration_seconds=monotonic() - output_scan_started_at,
                            count=len(outputs),
                        ),
                    )
                    packaging = await self.store.mutate(job_id, self._mark_packaging)
                    if packaging.phase != JobPhase.packaging:
                        return
                    try:
                        video_started_at = monotonic()
                        video_outputs = await self._create_videos(packaging)
                        await self.store.append_log(
                            job_id,
                            self._perf_log(
                                "videos.created",
                                duration_seconds=monotonic() - video_started_at,
                                count=len(video_outputs),
                            ),
                        )
                        archive_outputs = sorted([*outputs, *video_outputs])
                        archive_started_at = monotonic()
                        archive_path = await self._create_archive(packaging, archive_outputs)
                        await self.store.append_log(
                            job_id,
                            self._perf_log(
                                "archive.created",
                                duration_seconds=monotonic() - archive_started_at,
                                count=len(archive_outputs),
                            ),
                        )
                    except Exception as exc:
                        reason = str(exc) or "Packaging render outputs failed."
                        failed = await self.store.mutate(
                            job_id,
                            lambda item, message=reason: self._mark_failed(item, message),
                        )
                        await self.store.append_log(job_id, f"Packaging failed: {reason}")
                        await self.store.append_log(
                            job_id,
                            self._perf_log(
                                "job.finished",
                                duration_seconds=monotonic() - job_started_at,
                                phase=failed.phase.value,
                            ),
                        )
                        await self.store.create_activity(
                            event_type="render.failed",
                            description=f"Render {failed.id} failed.",
                            actor_user_id=failed.user_id,
                            subject_user_id=failed.user_id,
                            file_id=failed.file_id,
                            job_id=failed.id,
                            metadata={"error": failed.error},
                        )
                        return
                    completed = await self.store.mutate(
                        job_id,
                        lambda item,
                        output_names=[self._relative_output_path(job.output_dir, path) for path in outputs],
                        archive=archive_path: self._mark_completed(
                            item, output_names, archive
                        ),
                    )
                    if completed.phase != JobPhase.completed:
                        return
                    await self.store.append_log(
                        job_id,
                        self._perf_log(
                            "job.finished",
                            duration_seconds=monotonic() - job_started_at,
                            phase=completed.phase.value,
                        ),
                    )
                    await self.store.create_activity(
                        event_type="render.completed",
                        description=f"Render {completed.id} completed.",
                        actor_user_id=completed.user_id,
                        subject_user_id=completed.user_id,
                        file_id=completed.file_id,
                        job_id=completed.id,
                        metadata={"outputs": completed.outputs},
                    )
                    return

                collected_error = combined_output.strip() or "Blender exited with an error."
                if index == len(attempts) - 1 or not self._should_retry(device, retryable_gpu_error):
                    break
                await self.store.append_log(
                    job_id,
                    f"Retrying with the next device backend after {device} failed.",
                )

            failed = await self.store.mutate(
                job_id,
                lambda item, reason=collected_error: self._mark_failed(item, reason),
            )
            if failed.phase != JobPhase.failed:
                return
            await self.store.append_log(
                job_id,
                self._perf_log(
                    "job.finished",
                    duration_seconds=monotonic() - job_started_at,
                    phase=failed.phase.value,
                ),
            )
            await self.store.create_activity(
                event_type="render.failed",
                description=f"Render {failed.id} failed.",
                actor_user_id=failed.user_id,
                subject_user_id=failed.user_id,
                file_id=failed.file_id,
                job_id=failed.id,
                metadata={"error": failed.error},
            )
        finally:
            await self._unregister_process(job_id)
            await self._clear_cancel_request(job_id)

    async def cancel(self, job_id: str) -> None:
        process: asyncio.subprocess.Process | None = None
        async with self._process_lock:
            self._cancel_requested.add(job_id)
            process = self._active_processes.get(job_id)
        if process is not None:
            await self._terminate_process(process)

    def _mark_running(self, job: JobRecord) -> None:
        if job.phase != JobPhase.queued:
            return
        job.phase = JobPhase.running
        job.started_at = job.started_at or utc_now()
        job.last_progress_at = utc_now()
        job.progress = max(job.progress, 1.0)
        job.error = None
        job.outputs = self._relative_output_paths(job.output_dir, self._collect_outputs(job.output_dir))
        job.archive_path = None
        job.current_camera_name = None
        job.current_camera_index = None
        job.worker_assigned = job.worker_assigned or "local-worker"
        job.total_cameras = max(1, len(self._requested_cameras(job)))
        job.total_frames = self._total_frames(job)
        job.total_outputs_expected = max(1, job.total_cameras * job.total_frames)
        job.completed_frames = min(len(job.outputs), job.total_outputs_expected)
        job.failed_frames = len([frame for frame in job.frame_statuses if frame.status == FramePhase.failed])
        job.status_message = "Starting Blender render process."

    def _prepare_attempt(self, job: JobRecord, device: str) -> None:
        if job.phase != JobPhase.running:
            return
        job.resolved_device = device
        job.progress = max(job.progress, 2.0)
        job.status_message = f"Rendering on {device}."
        job.current_camera_name = None
        job.current_camera_index = None
        job.current_frame = None
        job.current_sample = None
        job.total_samples = None
        job.current_output = None
        job.last_progress_at = utc_now()
        job.environment_info = {
            "device": device,
            "blender_binary": self.settings.blender_binary,
            "gpu_order": self.settings.gpu_order,
        }

    def _mark_packaging(self, job: JobRecord) -> None:
        if job.phase != JobPhase.running:
            return
        job.phase = JobPhase.packaging
        job.progress = max(job.progress, 99.0)
        job.status_message = "Packaging render outputs."
        job.current_camera_name = None
        job.current_camera_index = None
        job.current_output = None
        job.last_progress_at = utc_now()

    def _mark_completed(self, job: JobRecord, outputs: list[str], archive_path: str | None) -> None:
        if job.phase not in {JobPhase.running, JobPhase.packaging}:
            return
        job.phase = JobPhase.completed
        job.progress = 100.0
        job.finished_at = utc_now()
        job.outputs = outputs
        job.archive_path = archive_path
        job.status_message = "Render complete."
        job.error = None
        job.current_camera_name = None
        job.current_camera_index = None
        job.current_output = outputs[-1] if outputs else None
        job.completed_frames = min(len(outputs), job.total_outputs_expected)
        job.failed_frames = len([frame for frame in job.frame_statuses if frame.status == FramePhase.failed])
        job.last_progress_at = utc_now()
        self._update_timing_metrics(job)
        if job.render_mode == RenderMode.still and job.frame is not None:
            job.current_frame = job.frame
        elif job.end_frame is not None:
            job.current_frame = job.end_frame

    def _mark_failed(self, job: JobRecord, reason: str) -> None:
        if job.phase not in {JobPhase.running, JobPhase.packaging}:
            return
        job.phase = JobPhase.failed
        job.finished_at = utc_now()
        job.status_message = "Render failed."
        job.error = reason
        job.failed_frames = max(
            len([frame for frame in job.frame_statuses if frame.status == FramePhase.failed]),
            1 if not job.outputs else 0,
        )
        job.last_progress_at = utc_now()
        self._update_timing_metrics(job)

    def _device_attempts(self, requested: RenderDevice) -> list[str]:
        if requested == RenderDevice.auto:
            seen: list[str] = []
            for candidate in self.settings.gpu_order:
                if candidate not in seen:
                    seen.append(candidate)
            if "CPU" not in seen:
                seen.append("CPU")
            return seen
        return [requested.value]

    async def _run_attempt(self, job: JobRecord, device: str) -> tuple[bool, str, bool]:
        requested_cameras = self._requested_cameras(job)
        total_cameras = len(requested_cameras)
        await self.store.append_log(
            job.id,
            self._perf_log("attempt.started", device=device, cameras=total_cameras),
        )
        return await self._run_batch_attempt(job, device, requested_cameras)

    async def _run_batch_attempt(
        self,
        job: JobRecord,
        device: str,
        requested_cameras: list[str | None],
    ) -> tuple[bool, str, bool]:
        total_cameras = len(requested_cameras)
        tracker = ProgressTracker(total_frames=self._total_frames(job))
        current_camera_name = requested_cameras[0] if requested_cameras else None
        current_camera_index = 0
        plan_path = self._write_render_plan(job, device, requested_cameras)
        command = self._build_batch_command(job, device, plan_path)
        quoted_command = " ".join(shlex.quote(part) for part in command)
        await self.store.append_log(job.id, f"Command: {quoted_command}")
        await self.store.mutate(
            job.id,
            lambda item, command_snapshot=command: self._record_command(item, command_snapshot),
        )
        process_started_at = monotonic()
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=self._blender_env(job=job),
        )
        await self._register_process(job.id, process)
        await self.store.append_log(
            job.id,
            self._perf_log(
                "blender.process_spawned",
                duration_seconds=monotonic() - process_started_at,
                camera="batch",
                device=device,
            ),
        )

        lines: deque[str] = deque(maxlen=80)
        pending_log_lines: list[str] = []
        known_outputs = list(job.outputs)
        last_log_flush_at = monotonic()
        first_stdout_logged = False
        first_progress_logged = False
        retryable_gpu_error = False
        assert process.stdout is not None

        async def flush_logs() -> None:
            nonlocal pending_log_lines, last_log_flush_at
            if not pending_log_lines:
                return
            lines_to_flush = pending_log_lines
            pending_log_lines = []
            last_log_flush_at = monotonic()
            await self.store.append_logs(job.id, lines_to_flush)

        async def flush_logs_with_mutation(mutator) -> JobRecord:
            nonlocal pending_log_lines, last_log_flush_at
            lines_to_flush = pending_log_lines
            pending_log_lines = []
            last_log_flush_at = monotonic()
            return await self.store.mutate_with_logs(job.id, mutator, lines_to_flush)

        try:
            async for raw_line in process.stdout:
                now = monotonic()
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                lines.append(line)
                if not first_stdout_logged:
                    pending_log_lines.append(
                        self._perf_log(
                            "blender.first_output",
                            duration_seconds=now - process_started_at,
                            camera="batch",
                            device=device,
                        )
                    )
                    first_stdout_logged = True
                pending_log_lines.append(line)
                retryable_gpu_error = retryable_gpu_error or bool(GPU_ERROR_RE.search(line))

                event = self._parse_render_farm_event(line)
                if event:
                    event_name = event.get("event")
                    if event_name == "batch_started":
                        pending_log_lines.append(
                            self._perf_log(
                                "blender.batch_started",
                                duration_seconds=now - process_started_at,
                                device=device,
                                cameras=event.get("cameras", total_cameras),
                            )
                        )
                    elif event_name == "batch_completed":
                        pending_log_lines.append(
                            self._perf_log(
                                "blender.batch_completed",
                                duration_seconds=now - process_started_at,
                                device=device,
                                cameras=event.get("cameras", total_cameras),
                            )
                        )
                    elif event_name == "camera_started":
                        tracker = ProgressTracker(total_frames=self._total_frames(job))
                        current_camera_index = max(0, int(event.get("camera_index", 1)) - 1)
                        current_camera_name = requested_cameras[current_camera_index]
                        await flush_logs_with_mutation(
                            lambda item,
                            camera=current_camera_name,
                            index=current_camera_index,
                            total=total_cameras,
                            current_device=device: self._mark_camera_started(
                                item,
                                camera,
                                index,
                                total,
                                current_device,
                            )
                        )
                    elif event_name == "camera_completed":
                        output_scan_started_at = monotonic()
                        outputs = self._collect_outputs(job.output_dir)
                        known_outputs = self._relative_output_paths(job.output_dir, outputs)
                        pending_log_lines.append(
                            self._perf_log(
                                "outputs.scanned",
                                duration_seconds=monotonic() - output_scan_started_at,
                                count=len(outputs),
                                trigger="camera_complete",
                            )
                        )
                        await flush_logs_with_mutation(
                            lambda item,
                            camera=current_camera_name,
                            index=current_camera_index + 1,
                            output_names=known_outputs: self._mark_camera_completed(
                                item,
                                camera,
                                index,
                                output_names,
                            )
                        )
                    continue

                progress, message = self._parse_progress(job, tracker, line, current_camera_name)
                if progress is not None:
                    if not first_progress_logged:
                        pending_log_lines.append(
                            self._perf_log(
                                "blender.first_progress",
                                duration_seconds=now - process_started_at,
                                camera="batch",
                                device=device,
                            )
                        )
                        first_progress_logged = True
                    overall_progress = self._overall_progress(current_camera_index, total_cameras, progress)
                    await flush_logs_with_mutation(
                        lambda item,
                        pct=overall_progress,
                        tracker_state=tracker,
                        status=message,
                        camera=current_camera_name,
                        index=current_camera_index + 1,
                        output_names=known_outputs: self._apply_progress(
                            item,
                            pct,
                            tracker_state,
                            status,
                            camera,
                            index,
                            output_names,
                        ),
                    )
                elif saved_output := self._saved_output_relative(job, line):
                    if saved_output not in known_outputs:
                        known_outputs = sorted([*known_outputs, saved_output])
                    pending_log_lines.append(
                        self._perf_log(
                            "outputs.noted",
                            count=len(known_outputs),
                            trigger="saved",
                        )
                    )
                    await flush_logs_with_mutation(
                        lambda item,
                        output_names=known_outputs,
                        camera=current_camera_name,
                        current_frame=tracker.current_frame,
                        current_output=saved_output: self._apply_output_snapshot(
                            item,
                            output_names,
                            camera,
                            current_frame,
                            current_output,
                        )
                    )
                elif (
                    len(pending_log_lines) >= LOG_FLUSH_LINE_COUNT
                    or now - last_log_flush_at >= LOG_FLUSH_INTERVAL_SECONDS
                ):
                    await flush_logs()

            await flush_logs()
            exit_code = await process.wait()
            await self.store.append_log(
                job.id,
                self._perf_log(
                    "blender.process_finished",
                    duration_seconds=monotonic() - process_started_at,
                    camera="batch",
                    device=device,
                    exit_code=exit_code,
                ),
            )
            if exit_code == 0:
                output_scan_started_at = monotonic()
                outputs = self._collect_outputs(job.output_dir)
                await self.store.append_log(
                    job.id,
                    self._perf_log(
                        "outputs.scanned",
                        duration_seconds=monotonic() - output_scan_started_at,
                        count=len(outputs),
                        trigger="batch_complete",
                    ),
                )
                return True, "\n".join(lines), retryable_gpu_error

            await self.store.mutate(
                job.id,
                lambda item,
                camera=current_camera_name,
                reason="\n".join(list(lines)[-12:]) or f"Blender exited with code {exit_code}.": self._mark_camera_failed(
                    item,
                    camera,
                    reason,
                ),
            )
            return False, "\n".join(lines), retryable_gpu_error
        finally:
            await flush_logs()
            await self._unregister_process(job.id, process)

    async def _run_camera_attempt(
        self,
        job: JobRecord,
        device: str,
        camera_name: str | None,
        camera_index: int,
        total_cameras: int,
    ) -> tuple[bool, list[str], bool]:
        tracker = ProgressTracker(total_frames=self._total_frames(job))
        command = self._build_command(job, device, camera_name, camera_index, total_cameras)
        quoted_command = " ".join(shlex.quote(part) for part in command)
        await self.store.append_log(job.id, f"Command: {quoted_command}")
        await self.store.mutate(
            job.id,
            lambda item, command_snapshot=command: self._record_command(item, command_snapshot),
        )
        process_started_at = monotonic()
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=self._blender_env(camera_name, job),
        )
        await self._register_process(job.id, process)
        await self.store.append_log(
            job.id,
            self._perf_log(
                "blender.process_spawned",
                duration_seconds=monotonic() - process_started_at,
                camera=self._camera_log_value(camera_name),
                device=device,
            ),
        )

        lines: deque[str] = deque(maxlen=80)
        pending_log_lines: list[str] = []
        known_outputs = list(job.outputs)
        last_log_flush_at = monotonic()
        first_stdout_logged = False
        first_progress_logged = False
        retryable_gpu_error = False
        assert process.stdout is not None

        async def flush_logs() -> None:
            nonlocal pending_log_lines, last_log_flush_at
            if not pending_log_lines:
                return
            lines_to_flush = pending_log_lines
            pending_log_lines = []
            last_log_flush_at = monotonic()
            await self.store.append_logs(job.id, lines_to_flush)

        async def flush_logs_with_mutation(mutator) -> JobRecord:
            nonlocal pending_log_lines, last_log_flush_at
            lines_to_flush = pending_log_lines
            pending_log_lines = []
            last_log_flush_at = monotonic()
            return await self.store.mutate_with_logs(job.id, mutator, lines_to_flush)

        try:
            async for raw_line in process.stdout:
                now = monotonic()
                line = raw_line.decode("utf-8", errors="replace").rstrip()
                lines.append(line)
                if not first_stdout_logged:
                    pending_log_lines.append(
                        self._perf_log(
                            "blender.first_output",
                            duration_seconds=now - process_started_at,
                            camera=self._camera_log_value(camera_name),
                            device=device,
                        )
                    )
                    first_stdout_logged = True
                pending_log_lines.append(line)
                retryable_gpu_error = retryable_gpu_error or bool(GPU_ERROR_RE.search(line))
                progress, message = self._parse_progress(job, tracker, line, camera_name)
                if progress is not None:
                    if not first_progress_logged:
                        pending_log_lines.append(
                            self._perf_log(
                                "blender.first_progress",
                                duration_seconds=now - process_started_at,
                                camera=self._camera_log_value(camera_name),
                                device=device,
                            )
                        )
                        first_progress_logged = True
                    overall_progress = self._overall_progress(camera_index, total_cameras, progress)
                    await flush_logs_with_mutation(
                        lambda item,
                        pct=overall_progress,
                        tracker_state=tracker,
                        status=message,
                        current_camera=camera_name,
                        current_camera_index=camera_index + 1,
                        output_names=known_outputs: self._apply_progress(
                            item,
                            pct,
                            tracker_state,
                            status,
                            current_camera,
                            current_camera_index,
                            output_names,
                        ),
                    )
                elif saved_output := self._saved_output_relative(job, line):
                    if saved_output not in known_outputs:
                        known_outputs = sorted([*known_outputs, saved_output])
                    pending_log_lines.append(
                        self._perf_log(
                            "outputs.noted",
                            count=len(known_outputs),
                            trigger="saved",
                        )
                    )
                    await flush_logs_with_mutation(
                        lambda item,
                        output_names=known_outputs,
                        current_camera=camera_name,
                        current_frame=tracker.current_frame,
                        current_output=saved_output: self._apply_output_snapshot(
                            item,
                            output_names,
                            current_camera,
                            current_frame,
                            current_output,
                        )
                    )
                elif (
                    len(pending_log_lines) >= LOG_FLUSH_LINE_COUNT
                    or now - last_log_flush_at >= LOG_FLUSH_INTERVAL_SECONDS
                ):
                    await flush_logs()

            await flush_logs()
            exit_code = await process.wait()
            await self.store.append_log(
                job.id,
                self._perf_log(
                    "blender.process_finished",
                    duration_seconds=monotonic() - process_started_at,
                    camera=self._camera_log_value(camera_name),
                    device=device,
                    exit_code=exit_code,
                ),
            )
            if exit_code == 0:
                output_scan_started_at = monotonic()
                outputs = self._collect_outputs(job.output_dir)
                await self.store.append_log(
                    job.id,
                    self._perf_log(
                        "outputs.scanned",
                        duration_seconds=monotonic() - output_scan_started_at,
                        count=len(outputs),
                        trigger="camera_complete",
                    ),
                )
                await self.store.mutate(
                    job.id,
                    lambda item,
                    current_camera=camera_name,
                    current_camera_index=camera_index + 1,
                    output_names=self._relative_output_paths(job.output_dir, outputs): self._mark_camera_completed(
                        item,
                        current_camera,
                        current_camera_index,
                        output_names,
                    ),
                )
                return True, list(lines), retryable_gpu_error

            await self.store.mutate(
                job.id,
                lambda item,
                current_camera=camera_name,
                reason="\n".join(list(lines)[-12:]) or f"Blender exited with code {exit_code}.": self._mark_camera_failed(
                    item,
                    current_camera,
                    reason,
                ),
            )
            return False, list(lines), retryable_gpu_error
        finally:
            await flush_logs()
            await self._unregister_process(job.id, process)

    def _record_command(self, job: JobRecord, command: list[str]) -> None:
        if job.phase != JobPhase.running:
            return
        job.command = list(command)

    def _build_command(
        self,
        job: JobRecord,
        device: str,
        camera_name: str | None,
        camera_index: int,
        total_cameras: int,
    ) -> list[str]:
        output_pattern = self._output_pattern(job, camera_name, camera_index, total_cameras)
        command = [
            self.settings.blender_binary,
            "-b",
            str(job.source_file),
            "-noaudio",
            "-P",
            str(self._script_path("prepare_render.py")),
            "-o",
            output_pattern,
            "-F",
            job.output_format.value,
            "-x",
            "1",
        ]
        if job.render_settings.render_engine:
            command.extend(["-E", job.render_settings.render_engine])
        if job.render_mode == RenderMode.still:
            frame = 1 if job.frame is None else job.frame
            command.extend(["-f", str(frame)])
        else:
            start_frame = 1 if job.start_frame is None else job.start_frame
            end_frame = start_frame if job.end_frame is None else job.end_frame
            command.extend(["-s", str(start_frame), "-e", str(end_frame)])
            if job.render_settings.frame_step and job.render_settings.frame_step > 1:
                command.extend(["-j", str(job.render_settings.frame_step)])
            command.append("-a")
        if self.settings.cycles_print_stats:
            command.append("--cycles-print-stats")
        command.extend(["--cycles-device", device])
        command.append("--")
        return command

    def _build_batch_command(self, job: JobRecord, device: str, plan_path: Path) -> list[str]:
        command = [
            self.settings.blender_binary,
            "-b",
            str(job.source_file),
            "-noaudio",
            "-F",
            job.output_format.value,
            "-x",
            "1",
        ]
        if self.settings.cycles_print_stats:
            command.append("--cycles-print-stats")
        command.extend(
            [
                "-P",
                str(self._script_path("prepare_render.py")),
                "-P",
                str(self._script_path("render_batch.py")),
                "--",
                "--render-plan",
                str(plan_path),
            ]
        )
        return command

    def _write_render_plan(
        self,
        job: JobRecord,
        device: str,
        requested_cameras: list[str | None],
    ) -> Path:
        plan_path = job.output_dir.parent / "render-plan.json"
        plan_path.parent.mkdir(parents=True, exist_ok=True)
        plan = {
            "device": device,
            "render_mode": job.render_mode.value,
            "output_format": job.output_format.value,
            "frame": 1 if job.frame is None else job.frame,
            "start_frame": 1 if job.start_frame is None else job.start_frame,
            "end_frame": job.end_frame,
            "frame_step": max(1, job.render_settings.frame_step or 1),
            "scene_info_path": str(self._scene_info_path(job)),
            "total_cameras": max(1, len(requested_cameras)),
            "cameras": [
                {
                    "camera_name": camera_name,
                    "camera_index": camera_index + 1,
                    "output_pattern": self._output_pattern(
                        job,
                        camera_name,
                        camera_index,
                        len(requested_cameras),
                    ),
                }
                for camera_index, camera_name in enumerate(requested_cameras)
            ],
        }
        if job.render_mode == RenderMode.animation and plan["end_frame"] is None:
            plan["end_frame"] = plan["start_frame"]
        plan_path.write_text(json.dumps(plan, indent=2, sort_keys=True), encoding="utf-8")
        return plan_path

    def _parse_render_farm_event(self, line: str) -> dict | None:
        if not line.startswith(RENDER_FARM_EVENT_PREFIX):
            return None
        try:
            event = json.loads(line.removeprefix(RENDER_FARM_EVENT_PREFIX))
        except json.JSONDecodeError:
            return None
        return event if isinstance(event, dict) else None

    async def inspect_blend(self, source_file: Path, scan_frame: int | None = None) -> dict:
        with tempfile.TemporaryDirectory(dir=self.settings.temp_root) as temp_dir:
            temp_root = Path(temp_dir)
            output_json = temp_root / "inspection.json"

            command = [
                self.settings.blender_binary,
                "-b",
                str(source_file),
                "-noaudio",
                "-P",
                str(self._script_path("prepare_render.py")),
                "-P",
                str(self._script_path("inspect_blend.py")),
                "--",
                "--output-json",
                str(output_json),
            ]
            if scan_frame is not None:
                command.extend(["--frame", str(scan_frame)])

            output = await self._run_command(
                command,
                env=self._blender_env(),
                capture_failure_output=True,
            )
            if not output_json.exists():
                message = output.splitlines()[-1] if output else "Failed to inspect blend file."
                raise RuntimeError(message)

            payload = json.loads(output_json.read_text("utf-8"))
            return payload

    def _parse_progress(
        self,
        job: JobRecord,
        tracker: ProgressTracker,
        line: str,
        camera_name: str | None,
    ) -> tuple[float | None, str | None]:
        message: str | None = None
        camera_prefix = self._camera_message_prefix(camera_name)
        frame_match = FRAME_RE.search(line)
        if frame_match:
            tracker.current_frame = int(frame_match.group(1))
            message = f"{camera_prefix} rendering frame {tracker.current_frame}."

        sample_match = SAMPLE_RE.search(line) or PATH_SAMPLE_RE.search(line)
        if sample_match:
            tracker.current_sample = int(sample_match.group(1))
            tracker.total_samples = int(sample_match.group(2))
            message = f"{camera_prefix} sample {tracker.current_sample}/{tracker.total_samples}."

        tile_match = TILE_RE.search(line) or PATH_TILE_RE.search(line)
        if tile_match:
            tracker.current_tile = int(tile_match.group(1))
            tracker.total_tiles = int(tile_match.group(2))
            message = f"{camera_prefix} tile {tracker.current_tile}/{tracker.total_tiles}."

        if tracker.total_frames <= 0:
            tracker.total_frames = 1

        frame_index = 1
        if job.render_mode == RenderMode.animation and tracker.current_frame is not None and job.start_frame is not None:
            frame_step = max(1, job.render_settings.frame_step or 1)
            frame_index = max(1, ((tracker.current_frame - job.start_frame) // frame_step) + 1)
        elif job.render_mode == RenderMode.still:
            frame_index = 1

        within_frame = 0.0
        if tracker.total_samples and tracker.current_sample:
            within_frame = tracker.current_sample / tracker.total_samples
        elif tracker.total_tiles and tracker.current_tile:
            within_frame = tracker.current_tile / tracker.total_tiles
        elif message:
            within_frame = 0.15

        if not message:
            return None, None

        if tracker.total_frames == 1:
            progress = max(2.0, min(within_frame * 100.0, 99.0))
        else:
            completed_frames = max(0, min(frame_index - 1, tracker.total_frames))
            progress = ((completed_frames + within_frame) / tracker.total_frames) * 100.0
            progress = max(2.0, min(progress, 99.0))
        return progress, message

    def _apply_progress(
        self,
        job: JobRecord,
        progress: float,
        tracker: ProgressTracker,
        message: str | None,
        camera_name: str | None,
        camera_index: int,
        outputs: list[str],
    ) -> None:
        if job.phase != JobPhase.running:
            return
        job.progress = progress
        job.current_camera_name = camera_name
        job.current_camera_index = camera_index
        job.current_frame = tracker.current_frame
        job.total_frames = tracker.total_frames
        job.current_sample = tracker.current_sample
        job.total_samples = tracker.total_samples
        job.outputs = outputs
        job.completed_frames = min(len(outputs), job.total_outputs_expected)
        job.failed_frames = len([frame for frame in job.frame_statuses if frame.status == FramePhase.failed])
        job.current_output = self._expected_output_relative(job, camera_name, tracker.current_frame)
        job.last_progress_at = utc_now()
        self._mark_frame_status(job, camera_name, tracker.current_frame, FramePhase.rendering)
        self._update_timing_metrics(job)
        if message:
            job.status_message = message

    def _apply_output_snapshot(
        self,
        job: JobRecord,
        outputs: list[str],
        camera_name: str | None,
        frame: int | None,
        current_output: str | None = None,
    ) -> None:
        if job.phase != JobPhase.running:
            return
        current_frame = frame
        if current_frame is None and current_output is not None:
            current_frame = self._frame_from_output_path(current_output)
        job.outputs = outputs
        job.completed_frames = min(len(outputs), job.total_outputs_expected)
        job.failed_frames = len([frame for frame in job.frame_statuses if frame.status == FramePhase.failed])
        job.current_output = current_output or (outputs[-1] if outputs else job.current_output)
        if current_frame is not None:
            job.current_frame = current_frame
            self._mark_frame_status(job, camera_name, current_frame, FramePhase.complete)
        job.progress = max(job.progress, self._progress_from_outputs(job))
        job.last_progress_at = utc_now()
        self._update_timing_metrics(job)
        if job.completed_frames > 0:
            job.status_message = (
                f"Rendered {job.completed_frames} / {job.total_outputs_expected} outputs."
            )

    def _mark_camera_completed(
        self,
        job: JobRecord,
        camera_name: str | None,
        camera_index: int,
        outputs: list[str],
    ) -> None:
        if job.phase != JobPhase.running:
            return
        job.outputs = outputs
        job.completed_frames = min(len(outputs), job.total_outputs_expected)
        job.failed_frames = len([frame for frame in job.frame_statuses if frame.status == FramePhase.failed])
        job.current_camera_name = camera_name
        job.current_camera_index = camera_index
        job.current_output = outputs[-1] if outputs else None
        job.progress = max(job.progress, self._progress_from_outputs(job))
        job.last_progress_at = utc_now()
        for frame_number in self._frame_numbers(job):
            self._mark_frame_status(job, camera_name, frame_number, FramePhase.complete)
        self._update_timing_metrics(job)

    def _mark_camera_failed(
        self,
        job: JobRecord,
        camera_name: str | None,
        reason: str,
    ) -> None:
        if job.phase != JobPhase.running:
            return
        for frame_number in self._frame_numbers(job):
            self._mark_frame_status(job, camera_name, frame_number, FramePhase.failed, reason=reason)
        job.failed_frames = len([frame for frame in job.frame_statuses if frame.status == FramePhase.failed])
        job.last_progress_at = utc_now()

    def _mark_camera_started(
        self,
        job: JobRecord,
        camera_name: str | None,
        camera_index: int,
        total_cameras: int,
        device: str,
    ) -> None:
        if job.phase != JobPhase.running:
            return
        job.current_camera_name = camera_name
        job.current_camera_index = camera_index + 1
        job.current_frame = None
        job.current_sample = None
        job.total_samples = None
        job.total_frames = self._total_frames(job)
        job.total_cameras = max(1, total_cameras)
        job.total_outputs_expected = max(1, job.total_frames * job.total_cameras)
        job.progress = self._overall_progress(camera_index, total_cameras, 2.0)
        camera_position = f" ({camera_index + 1}/{total_cameras})" if total_cameras > 1 else ""
        first_frame = self._frame_numbers(job)[0]
        job.current_frame = first_frame
        job.current_output = self._expected_output_relative(job, camera_name, first_frame)
        job.last_progress_at = utc_now()
        self._mark_frame_status(job, camera_name, first_frame, FramePhase.rendering)
        self._update_timing_metrics(job)
        job.status_message = f"Rendering {self._camera_message_prefix(camera_name)}{camera_position} on {device}."

    def _mark_frame_status(
        self,
        job: JobRecord,
        camera_name: str | None,
        frame_number: int | None,
        status: FramePhase,
        *,
        reason: str | None = None,
    ) -> None:
        if frame_number is None:
            return
        now = utc_now()
        for frame_status in job.frame_statuses:
            if frame_status.camera_name == camera_name and frame_status.frame == frame_number:
                if status == FramePhase.rendering and frame_status.status != FramePhase.rendering:
                    frame_status.started_at = frame_status.started_at or now
                    frame_status.attempts += 1
                if status in {FramePhase.complete, FramePhase.failed, FramePhase.skipped}:
                    frame_status.finished_at = now
                    if frame_status.started_at:
                        frame_status.seconds = max(0.0, (now - frame_status.started_at).total_seconds())
                frame_status.status = status
                frame_status.error = reason
                frame_status.output_path = self._expected_output_relative(job, camera_name, frame_number)
                return

    def _update_timing_metrics(self, job: JobRecord) -> None:
        if not job.started_at:
            return
        now = job.finished_at or utc_now()
        elapsed = max(0.0, (now - job.started_at).total_seconds())
        job.elapsed_seconds = elapsed
        completed_frames = max(
            job.completed_frames,
            len([frame for frame in job.frame_statuses if frame.status == FramePhase.complete]),
        )
        job.completed_frames = min(completed_frames, job.total_outputs_expected)
        if job.completed_frames > 0:
            average = elapsed / job.completed_frames
            job.average_seconds_per_frame = average
            remaining = max(0, job.total_outputs_expected - job.completed_frames)
            job.estimated_seconds_remaining = remaining * average if job.phase == JobPhase.running else 0.0
        elif job.progress > 2.0:
            projected = elapsed / max(job.progress / 100.0, 0.01)
            job.estimated_seconds_remaining = max(0.0, projected - elapsed)

    def _should_retry(self, device: str, retryable_gpu_error: bool) -> bool:
        return device != "CPU" and retryable_gpu_error

    def _collect_outputs(self, output_dir: Path) -> list[Path]:
        return sorted(
            path
            for path in output_dir.rglob("*")
            if path.is_file()
            and path.name not in {"metadata.json", "render-settings.json"}
            and not self._is_video_output(output_dir, path)
        )

    def _collect_video_outputs(self, output_dir: Path) -> list[Path]:
        video_root = output_dir / VIDEO_DIRECTORY_NAME
        if not video_root.exists():
            return []
        return sorted(path for path in video_root.rglob("*.mp4") if path.is_file())

    def _is_video_output(self, output_dir: Path, output: Path) -> bool:
        try:
            return output.relative_to(output_dir).parts[:1] == (VIDEO_DIRECTORY_NAME,)
        except ValueError:
            return False

    async def _create_videos(self, job: JobRecord) -> list[Path]:
        if job.render_mode != RenderMode.animation or self._total_frames(job) <= 1:
            return self._collect_video_outputs(job.output_dir)

        frame_rate = self._video_frame_rate(job)
        video_outputs: list[Path] = []
        for camera_name in self._requested_cameras(job):
            frames = self._camera_frame_sequence(job, camera_name)
            if len(frames) < 2:
                continue
            output_path = self._video_output_path(job, camera_name)
            if not output_path.exists():
                await self._create_camera_video(output_path, frames, frame_rate, job)
            video_outputs.append(output_path)
        return sorted(video_outputs)

    def _camera_frame_sequence(
        self,
        job: JobRecord,
        camera_name: str | None,
    ) -> list[tuple[int, Path]]:
        frames: list[tuple[int, Path]] = []
        missing_frames: list[int] = []
        for frame_number in self._frame_numbers(job):
            relative_path = self._expected_output_relative(job, camera_name, frame_number)
            if relative_path is None:
                continue
            output_path = job.output_dir / relative_path
            if output_path.exists() and output_path.is_file():
                frames.append((frame_number, output_path))
            else:
                missing_frames.append(frame_number)

        if missing_frames:
            camera_label = self._camera_log_value(camera_name)
            preview = ", ".join(str(frame) for frame in missing_frames[:5])
            if len(missing_frames) > 5:
                preview = f"{preview}, ..."
            raise RuntimeError(
                f"Cannot create video for {camera_label}; missing rendered frames: {preview}."
            )
        return frames

    async def _create_camera_video(
        self,
        output_path: Path,
        frames: list[tuple[int, Path]],
        frame_rate: float,
        job: JobRecord,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        concat_path = output_path.with_suffix(".ffconcat")
        concat_path.write_text(
            self._video_concat_manifest(job, frames, frame_rate),
            encoding="utf-8",
        )
        command = [
            self.settings.ffmpeg_binary,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_path),
            "-vf",
            "scale=ceil(iw/2)*2:ceil(ih/2)*2,format=yuv420p",
            "-c:v",
            "libx264",
            "-preset",
            "medium",
            "-crf",
            "18",
            "-r",
            self._ffmpeg_number(frame_rate),
            "-movflags",
            "+faststart",
            str(output_path),
        ]
        try:
            await self._run_video_command(command, output_path)
        finally:
            with suppress(FileNotFoundError):
                concat_path.unlink()

    async def _run_video_command(self, command: list[str], output_path: Path) -> None:
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"FFmpeg binary not found: {self.settings.ffmpeg_binary}.") from exc

        stdout, _ = await process.communicate()
        output = stdout.decode("utf-8", errors="replace").strip()
        if process.returncode == 0:
            return

        details = "\n".join(output.splitlines()[-8:]).strip()
        message = details or f"ffmpeg exited with code {process.returncode}."
        raise RuntimeError(f"FFmpeg failed creating {output_path.name}: {message}")

    def _video_concat_manifest(
        self,
        job: JobRecord,
        frames: list[tuple[int, Path]],
        frame_rate: float,
    ) -> str:
        lines: list[str] = ["ffconcat version 1.0"]
        durations = self._video_frame_durations(job, frames, frame_rate)
        for (_, frame_path), duration in zip(frames, durations, strict=True):
            lines.append(f"file {shlex.quote(str(frame_path))}")
            lines.append(f"duration {self._ffmpeg_number(duration)}")
        lines.append(f"file {shlex.quote(str(frames[-1][1]))}")
        return "\n".join(lines) + "\n"

    def _video_frame_durations(
        self,
        job: JobRecord,
        frames: list[tuple[int, Path]],
        frame_rate: float,
    ) -> list[float]:
        durations: list[float] = []
        for index, (frame_number, _) in enumerate(frames):
            if index < len(frames) - 1:
                frame_span = max(1, frames[index + 1][0] - frame_number)
            else:
                end_frame = frame_number if job.end_frame is None else job.end_frame
                frame_span = max(1, end_frame - frame_number + 1)
            durations.append(frame_span / frame_rate)
        return durations

    def _video_frame_rate(self, job: JobRecord) -> float:
        scene_info = self._read_scene_info(job)
        for source in (
            scene_info,
            job.render_settings.model_dump(mode="json", exclude_none=True),
        ):
            frame_rate = source.get("frame_rate")
            if isinstance(frame_rate, (int, float)) and frame_rate > 0:
                return float(frame_rate)
            fps = source.get("fps")
            fps_base = source.get("fps_base")
            if (
                isinstance(fps, (int, float))
                and isinstance(fps_base, (int, float))
                and fps > 0
                and fps_base > 0
            ):
                return float(fps) / float(fps_base)
        return 24.0

    def _read_scene_info(self, job: JobRecord) -> dict:
        scene_info_path = self._scene_info_path(job)
        if not scene_info_path.exists():
            return {}
        try:
            payload = json.loads(scene_info_path.read_text("utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    def _scene_info_path(self, job: JobRecord) -> Path:
        return job.output_dir.parent / SCENE_INFO_FILENAME

    def _video_output_path(self, job: JobRecord, camera_name: str | None) -> Path:
        safe_camera = self._safe_camera_output_name(camera_name)
        safe_job = self._safe_output_name(Path(job.source_filename).stem, "render")
        return job.output_dir / VIDEO_DIRECTORY_NAME / safe_camera / f"{safe_job}_{safe_camera}.mp4"

    def _ffmpeg_number(self, value: float) -> str:
        text = f"{value:.6f}".rstrip("0").rstrip(".")
        return text or "0"

    async def _create_archive(self, job: JobRecord, outputs: Iterable[Path]) -> str | None:
        files = list(outputs)
        if not files:
            return None
        archive_path = self.settings.jobs_root / job.id / "outputs.zip"
        await asyncio.to_thread(self._write_archive, archive_path, job, files)
        return str(archive_path)

    def _write_archive(self, archive_path: Path, job: JobRecord, outputs: list[Path]) -> None:
        root_name = self._archive_root_name(job)
        with ZipFile(archive_path, "w", compression=self._archive_compression(job)) as zip_file:
            for output in outputs:
                relative_path = self._relative_output_path(job.output_dir, output)
                zip_file.write(output, arcname=f"{root_name}/{relative_path}")
            zip_file.writestr(
                f"{root_name}/metadata.json",
                json.dumps(self._archive_metadata(job), indent=2, sort_keys=True, default=str),
            )
            zip_file.writestr(
                f"{root_name}/render-settings.json",
                json.dumps(job.render_settings.model_dump(mode="json"), indent=2, sort_keys=True),
            )

    def _archive_compression(self, job: JobRecord) -> int:
        if job.output_format.value in {"PNG", "JPEG"}:
            return ZIP_STORED
        return ZIP_DEFLATED

    async def create_archive_for_job(self, job: JobRecord) -> str | None:
        outputs = self._collect_outputs(job.output_dir)
        video_outputs = (
            await self._create_videos(job)
            if job.phase == JobPhase.completed
            else self._collect_video_outputs(job.output_dir)
        )
        return await self._create_archive(job, [*outputs, *video_outputs])

    def _total_frames(self, job: JobRecord) -> int:
        if job.render_mode == RenderMode.still:
            return 1
        start = 1 if job.start_frame is None else job.start_frame
        end = start if job.end_frame is None else job.end_frame
        step = max(1, job.render_settings.frame_step or 1)
        return max(1, len(range(start, end + 1, step)))

    def _frame_numbers(self, job: JobRecord) -> list[int]:
        if job.render_mode == RenderMode.still:
            return [1 if job.frame is None else job.frame]
        start = 1 if job.start_frame is None else job.start_frame
        end = start if job.end_frame is None else job.end_frame
        step = max(1, job.render_settings.frame_step or 1)
        return list(range(start, end + 1, step)) or [start]

    def _requested_cameras(self, job: JobRecord) -> list[str | None]:
        if job.camera_names:
            return list(job.camera_names)
        if job.camera_name:
            return [job.camera_name]
        return [None]

    def _overall_progress(self, camera_index: int, total_cameras: int, camera_progress: float) -> float:
        if total_cameras <= 1:
            return camera_progress
        scaled = ((camera_index + (camera_progress / 100.0)) / total_cameras) * 100.0
        return max(2.0, min(scaled, 99.0))

    def _progress_from_outputs(self, job: JobRecord) -> float:
        expected = max(1, job.total_outputs_expected)
        completed = max(0, min(job.completed_frames, expected))
        if completed == 0:
            return job.progress
        return max(2.0, min((completed / expected) * 100.0, 99.0))

    def _frame_from_output_path(self, output_path: str) -> int | None:
        match = re.search(r"_(\d+)\.[^.]+$", output_path)
        if not match:
            return None
        return int(match.group(1))

    def _camera_message_prefix(self, camera_name: str | None) -> str:
        if not camera_name:
            return "Default camera"
        return f"Camera {camera_name}"

    def _output_pattern(
        self,
        job: JobRecord,
        camera_name: str | None,
        camera_index: int,
        total_cameras: int,
    ) -> str:
        del camera_index, total_cameras
        safe_camera = self._safe_camera_output_name(camera_name)
        safe_job = self._safe_output_name(Path(job.source_filename).stem, "render")
        camera_dir = job.output_dir / safe_camera
        camera_dir.mkdir(parents=True, exist_ok=True)
        return str(camera_dir / f"{safe_job}_{safe_camera}_#####")

    def _expected_output_relative(
        self,
        job: JobRecord,
        camera_name: str | None,
        frame: int | None,
    ) -> str | None:
        if frame is None:
            return None
        safe_camera = self._safe_camera_output_name(camera_name)
        safe_job = self._safe_output_name(Path(job.source_filename).stem, "render")
        extension = {
            "PNG": "png",
            "JPEG": "jpg",
            "OPEN_EXR": "exr",
        }.get(job.output_format.value, job.output_format.value.lower())
        return f"{safe_camera}/{safe_job}_{safe_camera}_{frame:05d}.{extension}"

    def _relative_output_paths(self, output_dir: Path, outputs: list[Path]) -> list[str]:
        return [self._relative_output_path(output_dir, path) for path in outputs]

    def _relative_output_path(self, output_dir: Path, output: Path) -> str:
        try:
            return output.relative_to(output_dir).as_posix()
        except ValueError:
            return output.name

    def _saved_output_relative(self, job: JobRecord, line: str) -> str | None:
        match = SAVED_PATH_RE.search(line)
        if not match:
            return None
        return self._relative_output_path(job.output_dir, Path(match.group(1)))

    def _archive_root_name(self, job: JobRecord) -> str:
        return self._safe_output_name(Path(job.source_filename).stem, "render")

    def _archive_metadata(self, job: JobRecord) -> dict:
        return {
            "job_id": job.id,
            "file_id": job.file_id,
            "source_filename": job.source_filename,
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "finished_at": job.finished_at.isoformat() if job.finished_at else None,
            "render_mode": job.render_mode.value,
            "output_format": job.output_format.value,
            "camera_names": job.camera_names,
            "frame": job.frame,
            "start_frame": job.start_frame,
            "end_frame": job.end_frame,
            "frame_step": job.render_settings.frame_step,
            "total_cameras": job.total_cameras,
            "total_frames": job.total_frames,
            "total_outputs_expected": job.total_outputs_expected,
            "outputs": job.outputs,
        }

    def _safe_output_name(self, value: str, fallback: str) -> str:
        cleaned = SAFE_NAME_RE.sub("_", value).strip("._-")
        return cleaned or fallback

    def _safe_camera_output_name(self, camera_name: str | None) -> str:
        safe_camera = self._safe_output_name(camera_name or "Default Camera", "Default_Camera")
        if safe_camera.lower() == VIDEO_DIRECTORY_NAME:
            return f"{safe_camera}_Camera"
        return safe_camera

    def _camera_log_value(self, camera_name: str | None) -> str:
        return self._safe_camera_output_name(camera_name)

    def _perf_log(
        self,
        event: str,
        *,
        duration_seconds: float | None = None,
        **fields: object,
    ) -> str:
        parts = [f"event={event}"]
        if duration_seconds is not None:
            parts.append(f"duration={duration_seconds:.3f}s")
        for key, value in fields.items():
            parts.append(f"{key}={self._perf_value(value)}")
        return "perf | " + " ".join(parts)

    def _perf_value(self, value: object) -> str:
        text = str(value)
        return SAFE_NAME_RE.sub("_", text).strip("._-") or "none"

    def _reset_output_dir(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for path in output_dir.iterdir():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

    async def _register_process(self, job_id: str, process: asyncio.subprocess.Process) -> None:
        should_cancel = False
        async with self._process_lock:
            self._active_processes[job_id] = process
            should_cancel = job_id in self._cancel_requested
        if should_cancel:
            await self._terminate_process(process)

    async def _unregister_process(
        self,
        job_id: str,
        process: asyncio.subprocess.Process | None = None,
    ) -> None:
        async with self._process_lock:
            current = self._active_processes.get(job_id)
            if current is None:
                return
            if process is not None and current is not process:
                return
            self._active_processes.pop(job_id, None)

    async def _clear_cancel_request(self, job_id: str) -> None:
        async with self._process_lock:
            self._cancel_requested.discard(job_id)

    async def _is_cancel_requested(self, job_id: str) -> bool:
        async with self._process_lock:
            return job_id in self._cancel_requested

    async def _job_was_cancelled(self, job_id: str) -> bool:
        if await self._is_cancel_requested(job_id):
            return True
        snapshot = await self.store.get(job_id)
        return snapshot is not None and snapshot.phase == JobPhase.cancelled

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        with suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=5)
        except asyncio.TimeoutError:
            with suppress(ProcessLookupError):
                process.kill()
            with suppress(asyncio.TimeoutError, ProcessLookupError):
                await asyncio.wait_for(process.wait(), timeout=5)

    async def system_status(self) -> dict:
        blender_version = await self._run_command([self.settings.blender_binary, "--version"])
        gpu_status = await self._run_command(
            ["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader"]
        )
        cycles_devices = await self._probe_cycles_devices()
        return {
            "blender": blender_version.splitlines()[0] if blender_version else "Unavailable",
            "gpu": gpu_status.splitlines()[0] if gpu_status else "Unavailable",
            "device_policy": {
                "default": self.settings.default_device,
                "order": self.settings.gpu_order,
            },
            "cycles_devices": cycles_devices,
        }

    async def _probe_cycles_devices(self) -> dict:
        script = (
            "import bpy, json; "
            "prefs=bpy.context.preferences.addons['cycles'].preferences; "
            "prefs.refresh_devices(); "
            "available=[item[0] for item in prefs.get_device_types(bpy.context)]; "
            "payload={"
            "'available_types': available, "
            "'cuda': [device.name for device in prefs.get_devices_for_type('CUDA')], "
            "'optix': [device.name for device in prefs.get_devices_for_type('OPTIX')], "
            "'hip': [device.name for device in prefs.get_devices_for_type('HIP')], "
            "'cpu': [device.name for device in prefs.get_devices_for_type('CPU')]"
            "}; "
            "print(json.dumps(payload))"
        )
        output = await self._run_command([self.settings.blender_binary, "-b", "--python-expr", script])
        for line in reversed(output.splitlines()):
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
        return {"available_types": [], "cuda": [], "optix": [], "hip": [], "cpu": []}

    def _blender_env(self, camera_name: str | None = None, job: JobRecord | None = None) -> dict[str, str]:
        env = os.environ.copy()
        env.pop("RENDER_CAMERA_NAME", None)
        env.pop("RENDER_SETTINGS_JSON", None)
        if camera_name:
            env["RENDER_CAMERA_NAME"] = camera_name
        if job is not None:
            env["RENDER_SETTINGS_JSON"] = job.render_settings.model_dump_json(exclude_none=True)
        return env

    async def _run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        *,
        capture_failure_output: bool = False,
    ) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
            )
        except FileNotFoundError:
            return ""

        stdout, _ = await process.communicate()
        output = stdout.decode("utf-8", errors="replace").strip()
        if process.returncode != 0:
            return output if capture_failure_output else ""
        return output

    def _script_path(self, script_name: str) -> Path:
        return Path(__file__).with_name(script_name)
