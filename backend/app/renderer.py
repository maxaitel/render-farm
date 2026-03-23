from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
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
            success, combined_output = await self._run_attempt(job, device)
            if success:
                outputs = self._collect_outputs(job.output_dir)
                archive_path = await self._create_archive(job.id, outputs)
                await self.store.mutate(
                    job_id,
                    lambda item, output_names=[path.name for path in outputs], archive=archive_path: self._mark_completed(
                        item, output_names, archive
                    ),
                )
                return
            collected_error = combined_output.strip() or "Blender exited with an error."
            if index == len(attempts) - 1 or not self._should_retry(device, combined_output):
                break
            await self.store.append_log(
                job_id,
                f"Retrying with the next device backend after {device} failed.",
            )

        await self.store.mutate(
            job_id,
            lambda item, reason=collected_error: self._mark_failed(item, reason),
        )

    def _mark_running(self, job: JobRecord) -> None:
        job.phase = JobPhase.running
        job.started_at = job.started_at or utc_now()
        job.progress = max(job.progress, 1.0)
        job.error = None
        job.status_message = "Starting Blender render process."

    def _prepare_attempt(self, job: JobRecord, device: str) -> None:
        job.resolved_device = device
        job.progress = max(job.progress, 2.0)
        job.status_message = f"Rendering on {device}."
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

    async def _run_attempt(self, job: JobRecord, device: str) -> tuple[bool, str]:
        tracker = ProgressTracker(total_frames=self._total_frames(job))
        command = self._build_command(job, device)
        env = os.environ.copy()
        if job.camera_name:
            env["RENDER_CAMERA_NAME"] = job.camera_name
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )

        lines: list[str] = []
        assert process.stdout is not None
        async for raw_line in process.stdout:
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            lines.append(line)
            await self.store.append_log(job.id, line)
            progress, message = self._parse_progress(job, tracker, line)
            if progress is not None:
                await self.store.mutate(
                    job.id,
                    lambda item, pct=progress, tracker_state=tracker, status=message: self._apply_progress(
                        item, pct, tracker_state, status
                    ),
                )

        exit_code = await process.wait()
        combined_output = "\n".join(lines[-80:])
        return exit_code == 0, combined_output

    def _build_command(self, job: JobRecord, device: str) -> list[str]:
        output_pattern = str(job.output_dir / "frame_#####")
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
            frame = job.frame or 1
            command.extend(["-f", str(frame)])
        else:
            command.extend(
                [
                    "-s",
                    str(job.start_frame or 1),
                    "-e",
                    str(job.end_frame or job.start_frame or 1),
                    "-a",
                ]
            )
        command.extend(["--", "--cycles-print-stats", "--cycles-device", device])
        return command

    async def inspect_blend(self, source_file: Path, preview_frame: int | None = None) -> dict:
        with tempfile.TemporaryDirectory(dir=self.settings.temp_root) as temp_dir:
            temp_root = Path(temp_dir)
            output_json = temp_root / "inspection.json"
            preview_dir = temp_root / "previews"
            preview_dir.mkdir(parents=True, exist_ok=True)

            command = [
                self.settings.blender_binary,
                "-b",
                str(source_file),
                "-noaudio",
                "-P",
                str(self._script_path("inspect_blend.py")),
                "--",
                "--output-json",
                str(output_json),
                "--preview-dir",
                str(preview_dir),
            ]
            if preview_frame is not None:
                command.extend(["--frame", str(preview_frame)])

            output = await self._run_command(command)
            if not output_json.exists():
                message = output.splitlines()[-1] if output else "Failed to inspect blend file."
                raise RuntimeError(message)

            payload = json.loads(output_json.read_text("utf-8"))
            cameras = payload.get("cameras", [])
            for camera in cameras:
                preview_path = camera.get("preview_path")
                if not preview_path:
                    continue
                path = Path(preview_path)
                if not path.exists():
                    continue
                camera["preview_data_url"] = self._preview_data_url(path)
                camera.pop("preview_path", None)
            return payload

    def _parse_progress(
        self, job: JobRecord, tracker: ProgressTracker, line: str
    ) -> tuple[float | None, str | None]:
        message: str | None = None
        frame_match = FRAME_RE.search(line)
        if frame_match:
            tracker.current_frame = int(frame_match.group(1))
            message = f"Rendering frame {tracker.current_frame}."

        sample_match = SAMPLE_RE.search(line) or PATH_SAMPLE_RE.search(line)
        if sample_match:
            tracker.current_sample = int(sample_match.group(1))
            tracker.total_samples = int(sample_match.group(2))
            message = f"Path tracing sample {tracker.current_sample}/{tracker.total_samples}."

        tile_match = TILE_RE.search(line) or PATH_TILE_RE.search(line)
        if tile_match:
            tracker.current_tile = int(tile_match.group(1))
            tracker.total_tiles = int(tile_match.group(2))
            message = f"Tile {tracker.current_tile}/{tracker.total_tiles}."

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
        self, job: JobRecord, progress: float, tracker: ProgressTracker, message: str | None
    ) -> None:
        job.progress = progress
        job.current_frame = tracker.current_frame
        job.total_frames = tracker.total_frames
        job.current_sample = tracker.current_sample
        job.total_samples = tracker.total_samples
        if message:
            job.status_message = message

    def _should_retry(self, device: str, output: str) -> bool:
        return device != "CPU" and bool(GPU_ERROR_RE.search(output))

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
        start = job.start_frame or 1
        end = job.end_frame or start
        return max(1, end - start + 1)

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

    async def _run_command(self, command: list[str]) -> str:
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            return ""

        stdout, _ = await process.communicate()
        if process.returncode != 0:
            return ""
        return stdout.decode("utf-8", errors="replace").strip()

    def _script_path(self, script_name: str) -> Path:
        return Path(__file__).with_name(script_name)

    def _preview_data_url(self, path: Path) -> str:
        import base64

        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:image/png;base64,{encoded}"
