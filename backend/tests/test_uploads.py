from __future__ import annotations

import os
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import app


def _set_test_env(storage_root: Path) -> dict[str, str | None]:
    previous = {
        "RENDER_STORAGE_ROOT": os.environ.get("RENDER_STORAGE_ROOT"),
        "DISABLE_RENDER_WORKER": os.environ.get("DISABLE_RENDER_WORKER"),
        "BLENDER_BINARY": os.environ.get("BLENDER_BINARY"),
        "ADMIN_BOOTSTRAP_USERNAME": os.environ.get("ADMIN_BOOTSTRAP_USERNAME"),
        "ADMIN_BOOTSTRAP_PASSWORD": os.environ.get("ADMIN_BOOTSTRAP_PASSWORD"),
        "AUTH_COOKIE_SECURE": os.environ.get("AUTH_COOKIE_SECURE"),
    }
    os.environ["RENDER_STORAGE_ROOT"] = str(storage_root)
    os.environ["DISABLE_RENDER_WORKER"] = "1"
    os.environ["BLENDER_BINARY"] = "/bin/true"
    os.environ["ADMIN_BOOTSTRAP_USERNAME"] = "admin"
    os.environ["ADMIN_BOOTSTRAP_PASSWORD"] = "admin-password-123"
    os.environ["AUTH_COOKIE_SECURE"] = "false"
    return previous


def _restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _client() -> TestClient:
    return TestClient(app)


def _sign_in(
    client: TestClient,
    username: str,
    password: str,
) -> None:
    response = client.post(
        "/api/auth/sign-in",
        json={"username": username, "password": password},
    )
    assert response.status_code == 200, response.text


def test_sign_up_creates_pending_account_and_session_reflects_status(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client() as client:
            create_response = client.post(
                "/api/auth/sign-up",
                json={"username": "artist_one", "password": "artist-password-123"},
            )
            assert create_response.status_code == 200
            assert create_response.json()["user"]["status"] == "pending"

            sign_in_response = client.post(
                "/api/auth/sign-in",
                json={"username": "artist_one", "password": "artist-password-123"},
            )
            assert sign_in_response.status_code == 200

            session_response = client.get("/api/auth/session")
            assert session_response.status_code == 200
            assert session_response.json()["user"]["status"] == "pending"

            files_response = client.get("/api/files")
            assert files_response.status_code == 403
            assert files_response.json()["detail"] == "Your account is awaiting approval."
    finally:
        _restore_env(previous)


def test_admin_can_approve_user_upload_scene_and_queue_run(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client() as client:
            create_user_response = client.post(
                "/api/auth/sign-up",
                json={"username": "artist_two", "password": "artist-password-456"},
            )
            assert create_user_response.status_code == 200
            pending_user_id = create_user_response.json()["user"]["id"]

            _sign_in(client, "admin", "admin-password-123")
            approve_response = client.post(
                f"/api/admin/users/{pending_user_id}/status",
                json={"status": "approved"},
                headers={"x-forwarded-for": "127.0.0.1"},
            )
            assert approve_response.status_code == 200
            assert approve_response.json()["status"] == "approved"

        with _client() as user_client:
            _sign_in(user_client, "artist_two", "artist-password-456")

            upload_response = user_client.post(
                "/api/files",
                data=[
                    ("blend_file_path", "Project Files/scenes/Scene 1.blend"),
                    ("project_paths", "Project Files/textures/Wood Floor.png"),
                ],
                files=[
                    ("blend_file", ("Scene 1.blend", b"blend-bytes", "application/octet-stream")),
                    ("project_files", ("Wood Floor.png", b"png-bytes", "application/octet-stream")),
                ],
            )
            assert upload_response.status_code == 200, upload_response.text
            file_payload = upload_response.json()
            assert file_payload["source_filename"] == "Project Files/scenes/Scene 1.blend"
            assert file_payload["jobs"] == []

            source_root = tmp_path / "files" / file_payload["id"] / "source"
            assert (
                source_root / "Project Files" / "scenes" / "Scene 1.blend"
            ).read_bytes() == b"blend-bytes"
            assert (
                source_root / "Project Files" / "textures" / "Wood Floor.png"
            ).read_bytes() == b"png-bytes"

            run_response = user_client.post(
                f"/api/files/{file_payload['id']}/runs",
                data={
                    "render_mode": "still",
                    "output_format": "PNG",
                    "device_preference": "AUTO",
                    "frame": "12",
                    "camera_names": ["Cam_A", "Cam_B"],
                },
            )
            assert run_response.status_code == 200, run_response.text
            run_payload = run_response.json()
            assert run_payload["phase"] == "queued"
            assert run_payload["file_id"] == file_payload["id"]
            assert run_payload["camera_names"] == ["Cam_A", "Cam_B"]

            files_response = user_client.get("/api/files")
            assert files_response.status_code == 200
            listed_files = files_response.json()
            assert len(listed_files) == 1
            assert listed_files[0]["jobs"][0]["id"] == run_payload["id"]

            conn = sqlite3.connect(tmp_path / "renderfarm.sqlite3")
            row = conn.execute(
                "SELECT user_id, file_id, phase FROM jobs WHERE id = ?",
                (run_payload["id"],),
            ).fetchone()
            conn.close()
            assert row == (run_payload["user_id"], file_payload["id"], "queued")
    finally:
        _restore_env(previous)


def test_non_admin_user_cannot_access_admin_routes(tmp_path: Path) -> None:
    previous = _set_test_env(tmp_path)
    try:
        with _client() as client:
            create_user_response = client.post(
                "/api/auth/sign-up",
                json={"username": "artist_three", "password": "artist-password-789"},
            )
            pending_user_id = create_user_response.json()["user"]["id"]

            _sign_in(client, "admin", "admin-password-123")
            approve_response = client.post(
                f"/api/admin/users/{pending_user_id}/status",
                json={"status": "approved"},
                headers={"x-forwarded-for": "127.0.0.1"},
            )
            assert approve_response.status_code == 200

        with _client() as user_client:
            _sign_in(user_client, "artist_three", "artist-password-789")
            response = user_client.get(
                "/api/admin/users",
                headers={"x-forwarded-for": "127.0.0.1"},
            )
            assert response.status_code == 404
    finally:
        _restore_env(previous)
