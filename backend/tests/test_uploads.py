from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app
from app.models import JobRecord, RenderDevice, RenderMode, OutputFormat, utc_now


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
