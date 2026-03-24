from __future__ import annotations

import asyncio
from pathlib import Path
from zipfile import ZipFile

from app.config import Settings
from app.models import JobPhase, JobRecord, OutputFormat, RenderDevice, RenderMode, UserFileRecord, UserStatus
from app.renderer import RenderRunner
from app.store import JobStore


def test_multi_camera_render_creates_single_archive_with_camera_named_outputs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            storage_root=tmp_path,
            blender_binary="/bin/true",
            default_device="AUTO",
            gpu_order=["CPU"],
            disable_worker=True,
            session_cookie_name="renderfarm_session",
            session_ttl_hours=24,
            auth_cookie_secure="false",
            admin_panel_path="control-tower",
            admin_bootstrap_username=None,
            admin_bootstrap_password=None,
            allow_signups=True,
            trusted_proxies=[],
        )
        store = JobStore(settings.database_path)
        await store.load()
        try:
            user = await store.create_user(
                username="artist_renderer",
                password="artist-renderer-pass",
                status=UserStatus.approved,
            )
            file_id = "file001"
            file_root = settings.files_root / file_id / "source"
            file_root.mkdir(parents=True, exist_ok=True)
            source_path = file_root / "scene.blend"
            source_path.write_bytes(b"blend-data")
            await store.create_user_file(
                UserFileRecord(
                    id=file_id,
                    user_id=user.id,
                    created_at="2026-01-01T00:00:00+00:00",
                    updated_at="2026-01-01T00:00:00+00:00",
                    source_filename="scene.blend",
                    source_path=str(source_path),
                    source_root=str(file_root),
                    original_size_bytes=len(b"blend-data"),
                )
            )

            job_id = "multicam001"
            job_root = settings.jobs_root / job_id
            output_dir = job_root / "outputs"
            output_dir.mkdir(parents=True, exist_ok=True)

            job = JobRecord(
                id=job_id,
                user_id=user.id,
                file_id=file_id,
                source_filename="scene.blend",
                source_path=str(source_path),
                output_directory=str(output_dir),
                render_mode=RenderMode.still,
                output_format=OutputFormat.png,
                requested_device=RenderDevice.auto,
                camera_names=["Cam A", "Cam/B"],
                frame=3,
                total_frames=1,
            )
            await store.create(job)
            runner = RenderRunner(settings, store)

            async def fake_run_camera_attempt(
                current_job: JobRecord,
                device: str,
                camera_name: str | None,
                camera_index: int,
                total_cameras: int,
            ) -> tuple[bool, list[str]]:
                assert device == "CPU"
                output_pattern = runner._output_pattern(
                    current_job,
                    camera_name,
                    camera_index,
                    total_cameras,
                )
                output_path = Path(
                    output_pattern.replace("#####", f"{(current_job.frame or 1):05d}") + ".png"
                )
                output_path.write_bytes((camera_name or "default").encode("utf-8"))
                return True, [f"Rendered {camera_name or 'default'}"]

            monkeypatch.setattr(runner, "_run_camera_attempt", fake_run_camera_attempt)

            await runner.run(job_id)

            snapshot = await store.get(job_id)
            assert snapshot is not None
            assert snapshot.phase == JobPhase.completed
            assert snapshot.outputs == [
                "01_Cam_A_frame_00003.png",
                "02_Cam_B_frame_00003.png",
            ]
            assert snapshot.archive_path is not None

            archive_path = Path(snapshot.archive_path)
            assert archive_path.exists()
            with ZipFile(archive_path) as archive:
                assert sorted(archive.namelist()) == snapshot.outputs
        finally:
            await store.close()

    asyncio.run(scenario())
