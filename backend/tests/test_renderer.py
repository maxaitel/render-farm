from __future__ import annotations

import asyncio
import json
from pathlib import Path
from zipfile import ZipFile

from app.config import Settings
from app.models import (
    FramePhase,
    FrameRenderRecord,
    JobPhase,
    JobRecord,
    OutputFormat,
    RenderDevice,
    RenderMode,
    RenderSettings,
    UserFileRecord,
    UserStatus,
)
from app.renderer import ProgressTracker, RenderRunner
from app.store import JobStore


def _mark_cancelled(job: JobRecord) -> None:
    job.phase = JobPhase.cancelled
    job.finished_at = job.finished_at or job.created_at
    job.status_message = "Render cancelled."
    job.error = None


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

            async def fake_run_batch_attempt(
                current_job: JobRecord,
                device: str,
                requested_cameras: list[str | None],
            ) -> tuple[bool, str, bool]:
                assert device == "CPU"
                total_cameras = len(requested_cameras)
                for camera_index, camera_name in enumerate(requested_cameras):
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
                return True, "\n".join(f"Rendered {camera or 'default'}" for camera in requested_cameras), False

            monkeypatch.setattr(runner, "_run_batch_attempt", fake_run_batch_attempt)

            await runner.run(job_id)

            snapshot = await store.get(job_id)
            assert snapshot is not None
            assert snapshot.phase == JobPhase.completed
            assert snapshot.outputs == [
                "Cam_A/scene_Cam_A_00003.png",
                "Cam_B/scene_Cam_B_00003.png",
            ]
            assert snapshot.total_cameras == 2
            assert snapshot.total_outputs_expected == 2
            assert snapshot.archive_path is not None

            archive_path = Path(snapshot.archive_path)
            assert archive_path.exists()
            with ZipFile(archive_path) as archive:
                assert sorted(archive.namelist()) == [
                    "scene/Cam_A/scene_Cam_A_00003.png",
                    "scene/Cam_B/scene_Cam_B_00003.png",
                    "scene/metadata.json",
                    "scene/render-settings.json",
                ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_animation_archive_adds_per_camera_videos_without_counting_them_as_frames(
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
                username="artist_video",
                password="artist-video-pass",
                status=UserStatus.approved,
            )
            file_id = "file-video"
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

            job_id = "video001"
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
                render_mode=RenderMode.animation,
                output_format=OutputFormat.png,
                render_settings=RenderSettings(frame_step=1),
                requested_device=RenderDevice.auto,
                camera_names=["Cam A", "Cam/B"],
                start_frame=1,
                end_frame=3,
                total_frames=3,
                total_cameras=2,
                total_outputs_expected=6,
                frame_statuses=[
                    FrameRenderRecord(camera_name=camera_name, camera_index=camera_index, frame=frame)
                    for camera_index, camera_name in enumerate(["Cam A", "Cam/B"], start=1)
                    for frame in range(1, 4)
                ],
            )
            await store.create(job)
            runner = RenderRunner(settings, store)

            async def fake_run_batch_attempt(
                current_job: JobRecord,
                device: str,
                requested_cameras: list[str | None],
            ) -> tuple[bool, str, bool]:
                assert device == "CPU"
                runner._scene_info_path(current_job).write_text(
                    json.dumps({"fps": 30, "fps_base": 1.0, "frame_rate": 30.0}),
                    encoding="utf-8",
                )
                for camera_index, camera_name in enumerate(requested_cameras):
                    output_pattern = runner._output_pattern(
                        current_job,
                        camera_name,
                        camera_index,
                        len(requested_cameras),
                    )
                    for frame in range(1, 4):
                        output_path = Path(output_pattern.replace("#####", f"{frame:05d}") + ".png")
                        output_path.write_bytes(f"{camera_name}-{frame}".encode("utf-8"))
                return True, "Rendered animation", False

            created_videos: list[tuple[str, list[int], float]] = []

            async def fake_create_camera_video(
                output_path: Path,
                frames: list[tuple[int, Path]],
                frame_rate: float,
                current_job: JobRecord,
            ) -> None:
                del current_job
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(b"fake-mp4")
                created_videos.append((output_path.as_posix(), [frame for frame, _ in frames], frame_rate))

            monkeypatch.setattr(runner, "_run_batch_attempt", fake_run_batch_attempt)
            monkeypatch.setattr(runner, "_create_camera_video", fake_create_camera_video)

            await runner.run(job_id)

            snapshot = await store.get(job_id)
            assert snapshot is not None
            assert snapshot.phase == JobPhase.completed
            assert snapshot.outputs == [
                "Cam_A/scene_Cam_A_00001.png",
                "Cam_A/scene_Cam_A_00002.png",
                "Cam_A/scene_Cam_A_00003.png",
                "Cam_B/scene_Cam_B_00001.png",
                "Cam_B/scene_Cam_B_00002.png",
                "Cam_B/scene_Cam_B_00003.png",
            ]
            assert snapshot.completed_frames == 6
            assert [frames for _, frames, _ in created_videos] == [[1, 2, 3], [1, 2, 3]]
            assert [frame_rate for _, _, frame_rate in created_videos] == [30.0, 30.0]

            assert snapshot.archive_path is not None
            with ZipFile(Path(snapshot.archive_path)) as archive:
                assert sorted(archive.namelist()) == [
                    "scene/Cam_A/scene_Cam_A_00001.png",
                    "scene/Cam_A/scene_Cam_A_00002.png",
                    "scene/Cam_A/scene_Cam_A_00003.png",
                    "scene/Cam_B/scene_Cam_B_00001.png",
                    "scene/Cam_B/scene_Cam_B_00002.png",
                    "scene/Cam_B/scene_Cam_B_00003.png",
                    "scene/metadata.json",
                    "scene/render-settings.json",
                    "scene/videos/Cam_A/scene_Cam_A.mp4",
                    "scene/videos/Cam_B/scene_Cam_B.mp4",
                ]
        finally:
            await store.close()

    asyncio.run(scenario())


def test_video_frame_durations_keep_stepped_animation_realtime(tmp_path: Path) -> None:
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
    runner = RenderRunner(settings, store)
    output_dir = settings.jobs_root / "step-video" / "outputs"
    source_path = tmp_path / "scene.blend"
    source_path.write_bytes(b"blend-data")
    job = JobRecord(
        id="step-video",
        user_id=1,
        file_id="file001",
        source_filename="scene.blend",
        source_path=str(source_path),
        output_directory=str(output_dir),
        render_mode=RenderMode.animation,
        output_format=OutputFormat.png,
        requested_device=RenderDevice.auto,
        render_settings=RenderSettings(frame_step=2),
        start_frame=1,
        end_frame=24,
        total_frames=12,
    )
    frames = [(frame, output_dir / f"{frame:05d}.png") for frame in range(1, 24, 2)]

    durations = runner._video_frame_durations(job, frames, 24.0)

    assert len(durations) == 12
    assert abs(sum(durations) - 1.0) < 0.000001
    assert durations[0] == 2 / 24
    assert durations[-1] == 2 / 24


def test_camera_named_videos_does_not_collide_with_video_folder(tmp_path: Path) -> None:
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
    runner = RenderRunner(settings, store)
    output_dir = settings.jobs_root / "reserved-video" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "scene.blend"
    source_path.write_bytes(b"blend-data")
    job = JobRecord(
        id="reserved-video",
        user_id=1,
        file_id="file001",
        source_filename="scene.blend",
        source_path=str(source_path),
        output_directory=str(output_dir),
        render_mode=RenderMode.still,
        output_format=OutputFormat.png,
        requested_device=RenderDevice.auto,
        camera_names=["videos"],
        frame=1,
        total_frames=1,
    )

    output_pattern = runner._output_pattern(job, "videos", 0, 1)
    output_path = Path(output_pattern.replace("#####", "00001") + ".png")
    output_path.write_bytes(b"image")

    assert output_path.relative_to(output_dir).as_posix() == "videos_Camera/scene_videos_Camera_00001.png"
    assert runner._relative_output_paths(output_dir, runner._collect_outputs(output_dir)) == [
        "videos_Camera/scene_videos_Camera_00001.png"
    ]


def test_auto_device_retries_cpu_when_cuda_error_scrolls_out_of_tail(
    tmp_path: Path,
    monkeypatch,
) -> None:
    async def scenario() -> None:
        settings = Settings(
            storage_root=tmp_path,
            blender_binary="/bin/true",
            default_device="AUTO",
            gpu_order=["CUDA", "CPU"],
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
                username="artist_fallback",
                password="artist-fallback-pass",
                status=UserStatus.approved,
            )
            file_id = "file002"
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

            job_id = "fallback001"
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
                frame=1,
                total_frames=1,
            )
            await store.create(job)
            runner = RenderRunner(settings, store)

            attempts: list[str] = []

            async def fake_run_batch_attempt(
                current_job: JobRecord,
                device: str,
                requested_cameras: list[str | None],
            ) -> tuple[bool, str, bool]:
                del requested_cameras
                attempts.append(device)
                if device == "CUDA":
                    return False, "\n".join(f"log {index}" for index in range(120, 200)), True

                output_pattern = runner._output_pattern(current_job, None, 0, 1)
                output_path = Path(output_pattern.replace("#####", "00001") + ".png")
                output_path.write_bytes(b"cpu-render")
                return True, "Rendered on CPU", False

            monkeypatch.setattr(runner, "_run_batch_attempt", fake_run_batch_attempt)

            await runner.run(job_id)

            snapshot = await store.get(job_id)
            assert snapshot is not None
            assert snapshot.phase == JobPhase.completed
            assert snapshot.resolved_device == "CPU"
            assert snapshot.outputs == ["Default_Camera/scene_Default_Camera_00001.png"]
            assert attempts == ["CUDA", "CPU"]
            assert any("Retrying with the next device backend after CUDA failed." in line for line in snapshot.logs_tail)
        finally:
            await store.close()

    asyncio.run(scenario())


def test_animation_command_preserves_zero_start_frame(tmp_path: Path) -> None:
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
    runner = RenderRunner(settings, store)

    job_root = settings.jobs_root / "anim000"
    output_dir = job_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "scene.blend"
    source_path.write_bytes(b"blend-data")

    job = JobRecord(
        id="anim000",
        user_id=1,
        file_id="file001",
        source_filename="scene.blend",
        source_path=str(source_path),
        output_directory=str(output_dir),
        render_mode=RenderMode.animation,
        output_format=OutputFormat.png,
        requested_device=RenderDevice.auto,
        start_frame=0,
        end_frame=24,
        total_frames=25,
    )

    command = runner._build_command(job, "CPU", None, 0, 1)

    assert command[command.index("-s") + 1] == "0"
    assert command[command.index("-e") + 1] == "24"


def test_cycles_print_stats_is_opt_in(tmp_path: Path) -> None:
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
    runner = RenderRunner(settings, store)

    job_root = settings.jobs_root / "stats000"
    output_dir = job_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "scene.blend"
    source_path.write_bytes(b"blend-data")

    job = JobRecord(
        id="stats000",
        user_id=1,
        file_id="file001",
        source_filename="scene.blend",
        source_path=str(source_path),
        output_directory=str(output_dir),
        render_mode=RenderMode.still,
        output_format=OutputFormat.png,
        requested_device=RenderDevice.auto,
        frame=1,
        total_frames=1,
    )

    assert "--cycles-print-stats" not in runner._build_command(job, "CPU", None, 0, 1)

    settings.cycles_print_stats = True
    assert "--cycles-print-stats" in runner._build_command(job, "CPU", None, 0, 1)


def test_progress_parser_handles_blender_stat_spacing(tmp_path: Path) -> None:
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
    runner = RenderRunner(settings, store)

    source_path = tmp_path / "scene.blend"
    source_path.write_bytes(b"blend-data")
    job = JobRecord(
        id="anim-spacing",
        user_id=1,
        file_id="file001",
        source_filename="scene.blend",
        source_path=str(source_path),
        output_directory=str(tmp_path / "outputs"),
        render_mode=RenderMode.animation,
        output_format=OutputFormat.png,
        requested_device=RenderDevice.auto,
        start_frame=0,
        end_frame=24,
        total_frames=25,
    )
    tracker = ProgressTracker(total_frames=runner._total_frames(job))

    progress, message = runner._parse_progress(
        job,
        tracker,
        "Fra: 17 | Remaining: 10:27.76 | Mem: 1978M | Sample 64/2048",
        None,
    )

    assert tracker.current_frame == 17
    assert tracker.current_sample == 64
    assert tracker.total_samples == 2048
    assert message == "Default camera sample 64/2048."
    assert progress is not None
    assert progress > 68.0


def test_saved_output_snapshots_advance_progress_without_sample_lines(tmp_path: Path) -> None:
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
    runner = RenderRunner(settings, store)

    output_dir = settings.jobs_root / "saved-progress" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "scene.blend"
    source_path.write_bytes(b"blend-data")
    job = JobRecord(
        id="saved-progress",
        user_id=1,
        file_id="file001",
        source_filename="scene.blend",
        source_path=str(source_path),
        output_directory=str(output_dir),
        render_mode=RenderMode.animation,
        output_format=OutputFormat.png,
        requested_device=RenderDevice.auto,
        start_frame=1,
        end_frame=10,
        total_frames=10,
        total_outputs_expected=10,
        frame_statuses=[
            FrameRenderRecord(camera_name=None, camera_index=1, frame=frame)
            for frame in range(1, 11)
        ],
    )
    job.phase = JobPhase.running
    job.started_at = job.created_at
    job.progress = 2.0

    outputs = [
        f"Default_Camera/scene_Default_Camera_{frame:05d}.png"
        for frame in range(1, 5)
    ]
    runner._apply_output_snapshot(job, outputs, None, None, outputs[-1])

    assert job.completed_frames == 4
    assert job.progress == 40.0
    assert job.current_frame == 4
    assert job.current_output == "Default_Camera/scene_Default_Camera_00004.png"
    assert job.status_message == "Rendered 4 / 10 outputs."
    frame_four = next(frame for frame in job.frame_statuses if frame.frame == 4)
    assert frame_four.status == FramePhase.complete
    assert frame_four.output_path == "Default_Camera/scene_Default_Camera_00004.png"


def test_saved_output_progress_waits_for_completion_before_showing_100(tmp_path: Path) -> None:
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
    runner = RenderRunner(settings, store)

    output_dir = settings.jobs_root / "saved-progress-complete" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    source_path = tmp_path / "scene.blend"
    source_path.write_bytes(b"blend-data")
    output = "Default_Camera/scene_Default_Camera_00001.png"
    job = JobRecord(
        id="saved-progress-complete",
        user_id=1,
        file_id="file001",
        source_filename="scene.blend",
        source_path=str(source_path),
        output_directory=str(output_dir),
        render_mode=RenderMode.still,
        output_format=OutputFormat.png,
        requested_device=RenderDevice.auto,
        frame=1,
        total_frames=1,
        total_outputs_expected=1,
        frame_statuses=[FrameRenderRecord(camera_name=None, camera_index=1, frame=1)],
    )
    job.phase = JobPhase.running
    job.started_at = job.created_at
    job.progress = 2.0

    runner._apply_output_snapshot(job, [output], None, None, output)

    assert job.progress == 99.0
    assert job.phase == JobPhase.running

    runner._mark_completed(job, [output], None)

    assert job.progress == 100.0


def test_cancel_running_job_stops_process_and_preserves_cancelled_phase(
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
                username="artist_cancel",
                password="artist-cancel-pass",
                status=UserStatus.approved,
            )
            file_id = "file003"
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

            job_id = "cancel001"
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
                frame=1,
                total_frames=1,
            )
            await store.create(job)
            runner = RenderRunner(settings, store)

            started = asyncio.Event()
            terminated = asyncio.Event()

            class FakeStdout:
                def __init__(self) -> None:
                    self._sent_first_line = False

                def __aiter__(self):
                    return self

                async def __anext__(self) -> bytes:
                    if not self._sent_first_line:
                        self._sent_first_line = True
                        started.set()
                        return b"Fra:1 Sample 1/8\n"
                    await terminated.wait()
                    raise StopAsyncIteration

            class FakeProcess:
                def __init__(self) -> None:
                    self.stdout = FakeStdout()
                    self.returncode: int | None = None

                async def wait(self) -> int:
                    await terminated.wait()
                    assert self.returncode is not None
                    return self.returncode

                def terminate(self) -> None:
                    self.returncode = 143
                    terminated.set()

                def kill(self) -> None:
                    self.returncode = 137
                    terminated.set()

            async def fake_create_subprocess_exec(*args, **kwargs):
                del args, kwargs
                return FakeProcess()

            monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

            run_task = asyncio.create_task(runner.run(job_id))
            await started.wait()
            await runner.cancel(job_id)
            await store.mutate(job_id, _mark_cancelled)
            await run_task

            snapshot = await store.get(job_id)
            assert snapshot is not None
            assert snapshot.phase == JobPhase.cancelled
            assert snapshot.status_message == "Render cancelled."
        finally:
            await store.close()

    asyncio.run(scenario())
