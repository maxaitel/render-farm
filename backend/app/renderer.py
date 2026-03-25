from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import tempfile
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from zipfile import ZIP_DEFLATED, ZipFile

from .config import Settings
from .models import JobPhase, JobRecord, RenderDevice, RenderMode, utc_now
from .store import JobStore

FRAME_RE = re.compile(r"Fra:(\d+)")
SAMPLE_RE = re.compile(r"Sample (\d+)/(\d+)")
PATH_SAMPLE_RE = re.compile(r"Path Tracing Sample (\d+)/(\d+)")
TILE_RE = re.compile(r"Rendered (\d+)/(\d+) Tiles")
PATH_TILE_RE = re.compile(r"Path Tracing Tile (\d+)/(\d+)")
GPU_ERROR_RE = re.compile(
    r"(No compatible GPUs found|CUDA.+unavailable|OPTIX.+unavailable|is not a valid Cycles device|device type .* not available|Found no Cycles device of the specified type)",
    re.IGNORECASE,
)
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


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

    async def run(self, job_id: str) -> None:
        job = await self.store.get(job_id)
        if not job:
            return

        await self.store.mutate(
            job_id,
            lambda item: self._mark_running(item),
        )

        attempts = self._device_attempts(job.requested_device)
        collected_error = "Render failed."
        for index, device in enumerate(attempts):
            job = await self.store.mutate(
                job_id,
                lambda item, current=device: self._prepare_attempt(item, current),
            )
            self._reset_output_dir(job.output_dir)
            success, combined_output, retryable_gpu_error = await self._run_attempt(job, device)
            if success:
                outputs = self._collect_outputs(job.output_dir)
                archive_path = await self._create_archive(job.id, outputs)
                completed = await self.store.mutate(
                    job_id,
                    lambda item, output_names=[path.name for path in outputs], archive=archive_path: self._mark_completed(
                        item, output_names, archive
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
        await self.store.create_activity(
            event_type="render.failed",
            description=f"Render {failed.id} failed.",
            actor_user_id=failed.user_id,
            subject_user_id=failed.user_id,
            file_id=failed.file_id,
            job_id=failed.id,
            metadata={"error": failed.error},
        )

    def _mark_running(self, job: JobRecord) -> None:
        job.phase = JobPhase.running
        job.started_at = job.started_at or utc_now()
        job.progress = max(job.progress, 1.0)
        job.error = None
        job.outputs = []
        job.archive_path = None
        job.current_camera_name = None
        job.status_message = "Starting Blender render process."

    def _prepare_attempt(self, job: JobRecord, device: str) -> None:
        job.resolved_device = device
        job.progress = max(job.progress, 2.0)
        job.status_message = f"Rendering on {device}."
        job.current_camera_name = None
        job.current_frame = None
        job.current_sample = None
        job.total_samples = None
        job.logs_tail = []

    def _mark_completed(self, job: JobRecord, outputs: list[str], archive_path: str | None) -> None:
        job.phase = JobPhase.completed
        job.progress = 100.0
        job.finished_at = utc_now()
        job.outputs = outputs
        job.archive_path = archive_path
        job.status_message = "Render complete."
        job.error = None
        job.current_camera_name = None
        if job.render_mode == RenderMode.still and job.frame is not None:
            job.current_frame = job.frame
        elif job.end_frame is not None:
            job.current_frame = job.end_frame

    def _mark_failed(self, job: JobRecord, reason: str) -> None:
        job.phase = JobPhase.failed
        job.finished_at = utc_now()
        job.status_message = "Render failed."
        job.error = reason

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
        all_lines: deque[str] = deque(maxlen=80)

        for camera_index, camera_name in enumerate(requested_cameras):
            job = await self.store.mutate(
                job.id,
                lambda item,
                current_camera=camera_name,
                current_index=camera_index,
                current_device=device,
                current_total=total_cameras: self._mark_camera_started(
                    item,
                    current_camera,
                    current_index,
                    current_total,
                    current_device,
                ),
            )
            success, lines, retryable_gpu_error = await self._run_camera_attempt(
                job,
                device,
                camera_name,
                camera_index,
                total_cameras,
            )
            all_lines.extend(lines)
            if not success:
                return False, "\n".join(all_lines), retryable_gpu_error
        return True, "\n".join(all_lines), False

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
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=self._blender_env(camera_name),
        )

        lines: deque[str] = deque(maxlen=80)
        retryable_gpu_error = False
        assert process.stdout is not None
        async for raw_line in process.stdout:
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            lines.append(line)
            retryable_gpu_error = retryable_gpu_error or bool(GPU_ERROR_RE.search(line))
            await self.store.append_log(job.id, line)
            progress, message = self._parse_progress(job, tracker, line, camera_name)
            if progress is not None:
                overall_progress = self._overall_progress(camera_index, total_cameras, progress)
                await self.store.mutate(
                    job.id,
                    lambda item,
                    pct=overall_progress,
                    tracker_state=tracker,
                    status=message,
                    current_camera=camera_name: self._apply_progress(
                        item,
                        pct,
                        tracker_state,
                        status,
                        current_camera,
                    ),
                )

        exit_code = await process.wait()
        return exit_code == 0, list(lines), retryable_gpu_error

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
            "-E",
            "CYCLES",
            "-o",
            output_pattern,
            "-F",
            job.output_format.value,
            "-x",
            "1",
        ]
        if job.render_mode == RenderMode.still:
            frame = 1 if job.frame is None else job.frame
            command.extend(["-f", str(frame)])
        else:
            start_frame = 0 if job.start_frame is None else job.start_frame
            end_frame = start_frame if job.end_frame is None else job.end_frame
            command.extend(
                [
                    "-s",
                    str(start_frame),
                    "-e",
                    str(end_frame),
                    "-a",
                ]
            )
        command.extend(["--", "--cycles-print-stats", "--cycles-device", device])
        return command

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
            frame_index = max(1, tracker.current_frame - job.start_frame + 1)
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
    ) -> None:
        job.progress = progress
        job.current_camera_name = camera_name
        job.current_frame = tracker.current_frame
        job.total_frames = tracker.total_frames
        job.current_sample = tracker.current_sample
        job.total_samples = tracker.total_samples
        if message:
            job.status_message = message

    def _mark_camera_started(
        self,
        job: JobRecord,
        camera_name: str | None,
        camera_index: int,
        total_cameras: int,
        device: str,
    ) -> None:
        job.current_camera_name = camera_name
        job.current_frame = None
        job.current_sample = None
        job.total_samples = None
        job.total_frames = self._total_frames(job)
        job.progress = self._overall_progress(camera_index, total_cameras, 2.0)
        camera_position = f" ({camera_index + 1}/{total_cameras})" if total_cameras > 1 else ""
        job.status_message = f"Rendering {self._camera_message_prefix(camera_name)}{camera_position} on {device}."

    def _should_retry(self, device: str, retryable_gpu_error: bool) -> bool:
        return device != "CPU" and retryable_gpu_error

    def _collect_outputs(self, output_dir: Path) -> list[Path]:
        return sorted(path for path in output_dir.glob("*") if path.is_file())

    async def _create_archive(self, job_id: str, outputs: Iterable[Path]) -> str | None:
        files = list(outputs)
        if not files:
            return None
        archive_path = self.settings.jobs_root / job_id / "outputs.zip"
        await asyncio.to_thread(self._write_archive, archive_path, files)
        return str(archive_path)

    def _write_archive(self, archive_path: Path, outputs: list[Path]) -> None:
        with ZipFile(archive_path, "w", compression=ZIP_DEFLATED) as zip_file:
            for output in outputs:
                zip_file.write(output, arcname=output.name)

    def _total_frames(self, job: JobRecord) -> int:
        if job.render_mode == RenderMode.still:
            return 1
        start = 0 if job.start_frame is None else job.start_frame
        end = start if job.end_frame is None else job.end_frame
        return max(1, end - start + 1)

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
        if not camera_name:
            stem = "frame_#####"
        else:
            safe_camera = self._safe_output_name(camera_name, "camera")
            if total_cameras > 1:
                safe_camera = f"{camera_index + 1:02d}_{safe_camera}"
            stem = f"{safe_camera}_frame_#####"
        return str(job.output_dir / stem)

    def _safe_output_name(self, value: str, fallback: str) -> str:
        cleaned = SAFE_NAME_RE.sub("_", value).strip("._-")
        return cleaned or fallback

    def _reset_output_dir(self, output_dir: Path) -> None:
        output_dir.mkdir(parents=True, exist_ok=True)
        for path in output_dir.iterdir():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()

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

    def _blender_env(self, camera_name: str | None = None) -> dict[str, str]:
        env = os.environ.copy()
        env.pop("RENDER_CAMERA_NAME", None)
        if camera_name:
            env["RENDER_CAMERA_NAME"] = camera_name
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
