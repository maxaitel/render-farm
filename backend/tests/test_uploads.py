from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import app, inspect_session_cleanup_loop
from app.models import JobRecord, RenderDevice, RenderMode, OutputFormat, utc_now
from app.renderer import RenderRunner
from app.store import JobStore


def _set_test_env(storage_root: Path) -> dict[str, str | None]:
    previous = {
        "RENDER_STORAGE_ROOT": os.environ.get("RENDER_STORAGE_ROOT"),
        "DISABLE_RENDER_WORKER": os.environ.get("DISABLE_RENDER_WORKER"),
        "BLENDER_BINARY": os.environ.get("BLENDER_BINARY"),
    }
    os.environ["RENDER_STORAGE_ROOT"] = str(storage_root)
    os.environ["DISABLE_RENDER_WORKER"] = "1"
    os.environ["BLENDER_BINARY"] = "/bin/true"
    return previous


def _restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _client_for(storage_root: Path) -> TestClient:
    return TestClient(app)


def test_upload_creates_job_persists_file_and_db_row(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client_for(tmp_path) as client:
            response = client.post(
                "/api/jobs",
                data={
                    "render_mode": "still",
                    "output_format": "PNG",
                    "device_preference": "AUTO",
                    "frame": "12",
                },
                files={"blend_file": ("scene.blend", b"fake-blend-data", "application/octet-stream")},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["source_filename"] == "scene.blend"
        assert payload["phase"] == "queued"
        assert payload["frame"] == 12

        uploaded_file = tmp_path / "jobs" / payload["id"] / "input" / "scene.blend"
        assert uploaded_file.exists()
        assert uploaded_file.read_bytes() == b"fake-blend-data"

        conn = sqlite3.connect(tmp_path / "renderfarm.sqlite3")
        row = conn.execute(
            "SELECT id, phase, source_filename FROM jobs WHERE id = ?",
            (payload["id"],),
        ).fetchone()
        conn.close()

        assert row == (payload["id"], "queued", "scene.blend")
    finally:
        _restore_env(previous)


def test_non_blend_upload_is_rejected(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client_for(tmp_path) as client:
            response = client.post(
                "/api/jobs",
                data={"render_mode": "still"},
                files={"blend_file": ("notes.txt", b"not-a-blend", "text/plain")},
            )

        assert response.status_code == 400
        assert response.json()["detail"] == "Only .blend files are accepted."
    finally:
        _restore_env(previous)


def test_animation_with_invalid_frame_range_is_rejected(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client_for(tmp_path) as client:
            response = client.post(
                "/api/jobs",
                data={
                    "render_mode": "animation",
                    "start_frame": "20",
                    "end_frame": "10",
                },
                files={"blend_file": ("sequence.blend", b"blend-bytes", "application/octet-stream")},
            )

        assert response.status_code == 400
        assert response.json()["detail"] == "End frame must be greater than or equal to start frame."
    finally:
        _restore_env(previous)


def test_jobs_reload_from_sqlite_across_app_restart(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client_for(tmp_path) as client:
            created = client.post(
                "/api/jobs",
                data={
                    "render_mode": "animation",
                    "output_format": "PNG",
                    "device_preference": "AUTO",
                    "start_frame": "1",
                    "end_frame": "3",
                },
                files={"blend_file": ("shot.blend", b"blend-bytes", "application/octet-stream")},
            )
            assert created.status_code == 200
            created_job = created.json()

        with _client_for(tmp_path) as client:
            jobs_response = client.get("/api/jobs")
            system_response = client.get("/api/system")
            single_response = client.get(f"/api/jobs/{created_job['id']}")

        assert jobs_response.status_code == 200
        jobs = jobs_response.json()
        assert len(jobs) == 1
        assert jobs[0]["id"] == created_job["id"]
        assert jobs[0]["total_frames"] == 3

        assert single_response.status_code == 200
        assert single_response.json()["source_filename"] == "shot.blend"

        assert system_response.status_code == 200
        system_payload = system_response.json()
        assert system_payload["job_count"] == 1
        assert system_payload["active_jobs"] == 1
    finally:
        _restore_env(previous)


def test_batch_upload_creates_one_job_per_camera(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client_for(tmp_path) as client:
            response = client.post(
                "/api/jobs/batch",
                files=[
                    ("blend_file", ("scene.blend", b"camera-job-data", "application/octet-stream")),
                    ("render_mode", (None, "still")),
                    ("frame", (None, "5")),
                    ("camera_names", (None, "Cam_A")),
                    ("camera_names", (None, "Cam_B")),
                ],
            )

        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 2
        assert {job["camera_name"] for job in payload} == {"Cam_A", "Cam_B"}
        for job in payload:
            source_file = tmp_path / "jobs" / job["id"] / "input" / "scene.blend"
            assert source_file.exists()
            assert source_file.read_bytes() == b"camera-job-data"
    finally:
        _restore_env(previous)


def test_blend_inspect_returns_camera_payload(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client_for(tmp_path) as client:
            async def fake_inspect(source_path: Path, preview_frame: int | None = None) -> dict:
                assert source_path.exists()
                assert preview_frame == 7
                return {
                    "default_camera": "Camera_Main",
                    "frame": 7,
                    "cameras": [
                        {
                            "name": "Camera_Main",
                            "preview_data_url": "data:image/png;base64,preview",
                        }
                    ],
                }

            client.app.state.runtime.runner.inspect_blend = fake_inspect
            response = client.post(
                "/api/blend-inspect",
                data={"frame": "7"},
                files={"blend_file": ("scene.blend", b"inspect-me", "application/octet-stream")},
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["inspection_token"]
        assert payload["default_camera"] == "Camera_Main"
        assert payload["frame"] == 7
        assert payload["cameras"][0]["preview_data_url"].startswith("data:image/png;base64,")
    finally:
        _restore_env(previous)


def test_batch_job_can_reuse_saved_inspection_upload(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        inspect_token = "inspect123abc"
        inspect_root = tmp_path / "tmp" / "inspect" / inspect_token
        source_dir = inspect_root / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        source_path = source_dir / "scene.blend"
        source_path.write_bytes(b"reused-upload")
        (inspect_root / "session.json").write_text(
            json.dumps(
                {
                    "source_filename": "scene.blend",
                    "source_path": str(source_path),
                }
            ),
            encoding="utf-8",
        )

        with _client_for(tmp_path) as client:
            response = client.post(
                "/api/jobs/batch",
                data={
                    "inspect_token": inspect_token,
                    "render_mode": "still",
                    "frame": "2",
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert len(payload) == 1
        source_file = tmp_path / "jobs" / payload[0]["id"] / "input" / "scene.blend"
        assert source_file.exists()
        assert source_file.read_bytes() == b"reused-upload"
        assert not inspect_root.exists()
    finally:
        _restore_env(previous)


def test_saved_inspection_upload_is_kept_when_batch_job_creation_fails(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        inspect_token = "inspect-failure"
        inspect_root = tmp_path / "tmp" / "inspect" / inspect_token
        source_dir = inspect_root / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        source_path = source_dir / "scene.blend"
        source_path.write_bytes(b"reused-upload")
        (inspect_root / "session.json").write_text(
            json.dumps(
                {
                    "source_filename": "scene.blend",
                    "source_path": str(source_path),
                }
            ),
            encoding="utf-8",
        )

        with TestClient(app, raise_server_exceptions=False) as client:
            async def flaky_create_many(jobs: list[JobRecord]) -> list[JobRecord]:
                raise RuntimeError("database write failed")

            client.app.state.runtime.store.create_many = flaky_create_many
            response = client.post(
                "/api/jobs/batch",
                files=[
                    ("inspect_token", (None, inspect_token)),
                    ("render_mode", (None, "still")),
                    ("frame", (None, "2")),
                    ("camera_names", (None, "Cam_A")),
                    ("camera_names", (None, "Cam_B")),
                ],
            )

        assert response.status_code == 500
        assert inspect_root.exists()
    finally:
        _restore_env(previous)


def test_store_create_many_is_atomic(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JobStore(tmp_path / "renderfarm.sqlite3")
        await store.load()
        first_job = JobRecord(
            id="job-a",
            source_filename="scene.blend",
            source_path=str(tmp_path / "jobs" / "job-a" / "input" / "scene.blend"),
            output_directory=str(tmp_path / "jobs" / "job-a" / "outputs"),
            render_mode=RenderMode.still,
            output_format=OutputFormat.png,
            requested_device=RenderDevice.auto,
            frame=1,
            total_frames=1,
        )
        second_job = JobRecord(
            id="job-b",
            source_filename="scene.blend",
            source_path=str(tmp_path / "jobs" / "job-b" / "input" / "scene.blend"),
            output_directory=str(tmp_path / "jobs" / "job-b" / "outputs"),
            render_mode=RenderMode.still,
            output_format=OutputFormat.png,
            requested_device=RenderDevice.auto,
            frame=1,
            total_frames=1,
        )
        original_write_job_sync = store._write_job_sync
        write_calls = 0

        def flaky_write_job_sync(snapshot: JobRecord) -> None:
            nonlocal write_calls
            write_calls += 1
            original_write_job_sync(snapshot)
            if write_calls == 2:
                raise sqlite3.OperationalError("disk I/O error")

        store._write_job_sync = flaky_write_job_sync
        try:
            with pytest.raises(sqlite3.OperationalError):
                await store.create_many([first_job, second_job])

            assert await store.list_jobs() == []
            conn = sqlite3.connect(tmp_path / "renderfarm.sqlite3")
            count = conn.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
            conn.close()
            assert count == 0
        finally:
            await store.close()

    asyncio.run(scenario())


def test_release_blend_inspection_deletes_saved_upload(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        inspect_token = "inspect-delete"
        inspect_root = tmp_path / "tmp" / "inspect" / inspect_token
        source_dir = inspect_root / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / "scene.blend").write_bytes(b"remove-me")
        (inspect_root / "session.json").write_text(
            json.dumps(
                {
                    "source_filename": "scene.blend",
                    "source_path": str(source_dir / "scene.blend"),
                }
            ),
            encoding="utf-8",
        )

        with _client_for(tmp_path) as client:
            response = client.delete(f"/api/blend-inspect/{inspect_token}")

        assert response.status_code == 200
        assert response.json()["ok"] is True
        assert not inspect_root.exists()
    finally:
        _restore_env(previous)


def test_invalid_inspect_token_is_rejected(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client_for(tmp_path) as client:
            response = client.post(
                "/api/jobs",
                data={
                    "inspect_token": "../../outside",
                    "render_mode": "still",
                    "frame": "1",
                },
            )

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid camera scan token."
    finally:
        _restore_env(previous)


def test_render_runner_clears_inherited_camera_env(tmp_path: Path) -> None:
    previous_camera_name = os.environ.get("RENDER_CAMERA_NAME")
    os.environ["RENDER_CAMERA_NAME"] = "InheritedCamera"
    try:
        settings = Settings(
            storage_root=tmp_path,
            blender_binary="/bin/true",
            default_device="AUTO",
            gpu_order=["CPU"],
            disable_worker=True,
        )
        runner = RenderRunner(settings, JobStore(tmp_path / "renderfarm.sqlite3"))

        inherited_free_env = runner._blender_env()
        explicit_camera_env = runner._blender_env("ShotCam")

        assert "RENDER_CAMERA_NAME" not in inherited_free_env
        assert explicit_camera_env["RENDER_CAMERA_NAME"] == "ShotCam"
    finally:
        if previous_camera_name is None:
            os.environ.pop("RENDER_CAMERA_NAME", None)
        else:
            os.environ["RENDER_CAMERA_NAME"] = previous_camera_name


def test_blend_inspection_runs_prepare_render_before_camera_scan(tmp_path: Path) -> None:
    previous_camera_name = os.environ.get("RENDER_CAMERA_NAME")
    os.environ["RENDER_CAMERA_NAME"] = "InheritedCamera"
    try:
        settings = Settings(
            storage_root=tmp_path,
            blender_binary="/bin/true",
            default_device="AUTO",
            gpu_order=["CPU"],
            disable_worker=True,
        )
        runner = RenderRunner(settings, JobStore(tmp_path / "renderfarm.sqlite3"))
        source_path = tmp_path / "scene.blend"
        source_path.write_bytes(b"blend")
        settings.temp_root.mkdir(parents=True, exist_ok=True)
        captured: dict[str, object] = {}

        async def fake_run_command(command: list[str], env: dict[str, str] | None = None) -> str:
            captured["command"] = command
            captured["env"] = env
            output_json = Path(command[command.index("--output-json") + 1])
            output_json.write_text(
                json.dumps({"default_camera": None, "frame": 4, "cameras": []}),
                encoding="utf-8",
            )
            return "ok"

        runner._run_command = fake_run_command  # type: ignore[method-assign]

        payload = asyncio.run(runner.inspect_blend(source_path, preview_frame=4))

        command = captured["command"]
        assert isinstance(command, list)
        prepare_script = str(runner._script_path("prepare_render.py"))
        inspect_script = str(runner._script_path("inspect_blend.py"))
        assert command.count("-P") == 2
        assert prepare_script in command
        assert inspect_script in command
        assert command.index(prepare_script) < command.index(inspect_script)
        assert payload["frame"] == 4
        assert "RENDER_CAMERA_NAME" not in (captured["env"] or {})
    finally:
        if previous_camera_name is None:
            os.environ.pop("RENDER_CAMERA_NAME", None)
        else:
            os.environ["RENDER_CAMERA_NAME"] = previous_camera_name


def test_expired_inspection_upload_is_reaped_on_next_scan(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        expired_token = "expiredtoken"
        expired_root = tmp_path / "tmp" / "inspect" / expired_token
        source_dir = expired_root / "source"
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / "stale.blend").write_bytes(b"stale")
        session_path = expired_root / "session.json"
        session_path.write_text(
            json.dumps(
                {
                    "source_filename": "stale.blend",
                    "source_path": str(source_dir / "stale.blend"),
                }
            ),
            encoding="utf-8",
        )
        old_timestamp = time.time() - (2 * 60 * 60)
        os.utime(expired_root, (old_timestamp, old_timestamp))
        os.utime(session_path, (old_timestamp, old_timestamp))

        with _client_for(tmp_path) as client:
            async def fake_inspect(source_path: Path, preview_frame: int | None = None) -> dict:
                return {"default_camera": None, "frame": 1, "cameras": []}

            client.app.state.runtime.runner.inspect_blend = fake_inspect
            response = client.post(
                "/api/blend-inspect",
                files={"blend_file": ("fresh.blend", b"fresh", "application/octet-stream")},
            )

        assert response.status_code == 200
        assert not expired_root.exists()
    finally:
        _restore_env(previous)


def test_inspect_session_cleanup_loop_reaps_expired_uploads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expired_root = tmp_path / "tmp" / "inspect" / "expiredtoken"
    source_dir = expired_root / "source"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "stale.blend").write_bytes(b"stale")
    session_path = expired_root / "session.json"
    session_path.write_text(
        json.dumps(
            {
                "source_filename": "stale.blend",
                "source_path": str(source_dir / "stale.blend"),
            }
        ),
        encoding="utf-8",
    )
    old_timestamp = time.time() - (2 * 60 * 60)
    os.utime(expired_root, (old_timestamp, old_timestamp))
    os.utime(session_path, (old_timestamp, old_timestamp))

    async def cancel_after_first_iteration(_seconds: int) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("app.main.asyncio.sleep", cancel_after_first_iteration)
    settings = Settings(
        storage_root=tmp_path,
        blender_binary="/bin/true",
        default_device="AUTO",
        gpu_order=["CPU"],
        disable_worker=True,
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(inspect_session_cleanup_loop(settings, interval_seconds=1))

    assert not expired_root.exists()


def test_existing_job_json_is_imported_into_sqlite(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        legacy_job = JobRecord(
            id="legacyjob001",
            created_at=utc_now(),
            source_filename="legacy.blend",
            source_path=str(tmp_path / "jobs" / "legacyjob001" / "input" / "legacy.blend"),
            output_directory=str(tmp_path / "jobs" / "legacyjob001" / "outputs"),
            render_mode=RenderMode.still,
            output_format=OutputFormat.png,
            requested_device=RenderDevice.auto,
            frame=3,
            total_frames=1,
        )
        legacy_dir = tmp_path / "jobs" / legacy_job.id
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / "job.json").write_text(
            json.dumps(legacy_job.model_dump(mode="json"), indent=2),
            encoding="utf-8",
        )

        with _client_for(tmp_path) as client:
            response = client.get("/api/jobs")

        assert response.status_code == 200
        jobs = response.json()
        assert len(jobs) == 1
        assert jobs[0]["id"] == legacy_job.id
        assert jobs[0]["source_filename"] == "legacy.blend"

        conn = sqlite3.connect(tmp_path / "renderfarm.sqlite3")
        row = conn.execute(
            "SELECT id, source_filename FROM jobs WHERE id = ?",
            (legacy_job.id,),
        ).fetchone()
        conn.close()

        assert row == (legacy_job.id, "legacy.blend")
    finally:
        _restore_env(previous)


def test_legacy_import_continues_when_database_already_has_jobs(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        existing_job = JobRecord(
            id="existing001",
            created_at=utc_now(),
            source_filename="existing.blend",
            source_path=str(tmp_path / "jobs" / "existing001" / "input" / "existing.blend"),
            output_directory=str(tmp_path / "jobs" / "existing001" / "outputs"),
            render_mode=RenderMode.still,
            output_format=OutputFormat.png,
            requested_device=RenderDevice.auto,
            frame=1,
            total_frames=1,
        )
        legacy_job = JobRecord(
            id="legacy002",
            created_at=utc_now(),
            source_filename="legacy.blend",
            source_path=str(tmp_path / "jobs" / "legacy002" / "input" / "legacy.blend"),
            output_directory=str(tmp_path / "jobs" / "legacy002" / "outputs"),
            render_mode=RenderMode.still,
            output_format=OutputFormat.png,
            requested_device=RenderDevice.auto,
            frame=2,
            total_frames=1,
        )

        conn = sqlite3.connect(tmp_path / "renderfarm.sqlite3")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                created_at TEXT NOT NULL,
                phase TEXT NOT NULL,
                source_filename TEXT NOT NULL,
                payload TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT OR IGNORE INTO users (id, username, created_at) VALUES (1, ?, ?)",
            ("local", utc_now().isoformat()),
        )
        payload = existing_job.model_dump(mode="json")
        conn.execute(
            "INSERT INTO jobs (id, user_id, created_at, phase, source_filename, payload) VALUES (?, ?, ?, ?, ?, ?)",
            (
                existing_job.id,
                1,
                payload["created_at"],
                payload["phase"],
                payload["source_filename"],
                json.dumps(payload, indent=2),
            ),
        )
        conn.commit()
        conn.close()

        corrupt_dir = tmp_path / "jobs" / "broken003"
        corrupt_dir.mkdir(parents=True, exist_ok=True)
        (corrupt_dir / "job.json").write_text("{not valid json", encoding="utf-8")

        legacy_dir = tmp_path / "jobs" / legacy_job.id
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / "job.json").write_text(
            json.dumps(legacy_job.model_dump(mode="json"), indent=2),
            encoding="utf-8",
        )

        with _client_for(tmp_path) as client:
            response = client.get("/api/jobs")

        assert response.status_code == 200
        jobs = response.json()
        assert {job["id"] for job in jobs} == {existing_job.id, legacy_job.id}
    finally:
        _restore_env(previous)
