from __future__ import annotations

import asyncio
import json
import re
import shutil
import sqlite3
import uuid
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath

import aiofiles
from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile, status
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field

from .config import Settings, load_settings
from .models import (
    AuthSessionPayload,
    FrameRenderRecord,
    JobPhase,
    JobRecord,
    OutputFormat,
    RenderDevice,
    RenderMode,
    RenderSettings,
    UserFileRecord,
    UserRecord,
    UserRole,
    UserStatus,
    utc_now,
)
from .renderer import RenderRunner
from .security import (
    hash_session_token,
    is_private_ip,
    is_trusted_proxy,
    new_session_token,
    normalize_username as normalize_username_value,
)
from .store import JobStore

FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024


class SignUpRequest(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    password: str = Field(min_length=12, max_length=256)


class SignInRequest(BaseModel):
    username: str
    password: str


class UserModerationRequest(BaseModel):
    status: UserStatus


class AppState:
    def __init__(self, settings: Settings, store: JobStore, runner: RenderRunner, queue: asyncio.Queue[str]) -> None:
        self.settings = settings
        self.store = store
        self.runner = runner
        self.queue = queue
        self.worker_task: asyncio.Task[None] | None = None


def runtime_state() -> AppState:
    return app.state.runtime  # type: ignore[return-value]


def sanitize_filename(filename: str) -> str:
    cleaned = FILENAME_RE.sub("-", filename).strip("-.")
    return cleaned or "project.blend"


def sanitize_relative_path(path_value: str) -> Path:
    cleaned = path_value.strip().replace("\\", "/")
    if not cleaned:
        raise HTTPException(status_code=400, detail="Invalid project file path.")

    path = PurePosixPath(cleaned)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HTTPException(status_code=400, detail="Invalid project file path.")

    return Path(*path.parts)


def normalize_username(username: str) -> str:
    try:
        return normalize_username_value(username)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


def unique_camera_names(camera_names: list[str] | None) -> list[str]:
    if not camera_names:
        return []

    seen: set[str] = set()
    names: list[str] = []
    for camera_name in camera_names:
        cleaned = camera_name.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        names.append(cleaned)
    return names


def positive_or_none(value: int | None) -> int | None:
    if value is None:
        return None
    return max(1, value)


def positive_float_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return value if value > 0 else None


def bounded_or_none(value: int | None, low: int, high: int) -> int | None:
    if value is None:
        return None
    return max(low, min(high, value))


def safe_render_engine(value: str | None) -> str | None:
    if not value or not value.strip():
        return None
    engine = value.strip().upper()
    allowed = {"CYCLES", "BLENDER_EEVEE_NEXT", "BLENDER_WORKBENCH"}
    if engine not in allowed:
        raise HTTPException(status_code=400, detail="Unsupported render engine.")
    return engine


def compact_render_settings(settings: RenderSettings) -> RenderSettings:
    payload = settings.model_dump(mode="json")
    return RenderSettings.model_validate({key: value for key, value in payload.items() if value is not None})


def render_settings_payload(settings: RenderSettings) -> dict:
    return settings.model_dump(mode="json", exclude_none=True)


def frame_numbers_for_run(render_mode: RenderMode, frame: int | None, start_frame: int | None, end_frame: int | None, frame_step: int | None) -> list[int]:
    step = max(1, frame_step or 1)
    if render_mode == RenderMode.still:
        return [1 if frame is None else frame]
    start = 1 if start_frame is None else start_frame
    end = start if end_frame is None else end_frame
    return list(range(start, end + 1, step)) or [start]


def frame_statuses_for_run(
    *,
    camera_names: list[str],
    render_mode: RenderMode,
    frame: int | None,
    start_frame: int | None,
    end_frame: int | None,
    frame_step: int | None,
) -> list[FrameRenderRecord]:
    cameras: list[str | None] = camera_names or [None]
    frames = frame_numbers_for_run(render_mode, frame, start_frame, end_frame, frame_step)
    return [
        FrameRenderRecord(camera_name=camera_name, camera_index=camera_index, frame=frame_number)
        for camera_index, camera_name in enumerate(cameras, start=1)
        for frame_number in frames
    ]


def cookie_secure_setting(settings: Settings, request: Request) -> bool:
    if settings.auth_cookie_secure == "true":
        return True
    if settings.auth_cookie_secure == "false":
        return False
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",")[0].strip().lower()
    return forwarded_proto == "https" or request.url.scheme == "https"


def client_ip(request: Request, settings: Settings) -> str | None:
    remote_host = request.client.host if request.client else None
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        if not is_trusted_proxy(remote_host, settings.trusted_proxies):
            return remote_host
        candidate = forwarded_for.split(",")[0].strip()
        if candidate:
            return candidate
    return remote_host


def lan_admin_access(request: Request, settings: Settings) -> bool:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    remote_host = request.client.host if request.client else None
    if forwarded_for and not is_trusted_proxy(remote_host, settings.trusted_proxies):
        return False
    return is_private_ip(client_ip(request, settings))


async def start_user_session(state: AppState, user: UserRecord, request: Request, response: Response) -> None:
    session_token = new_session_token()
    request_ip = client_ip(request, state.settings)
    await state.store.create_session(
        user_id=user.id,
        token_hash=hash_session_token(session_token),
        expires_in_hours=state.settings.session_ttl_hours,
        ip_address=request_ip,
        user_agent=request.headers.get("user-agent"),
    )
    response.set_cookie(
        key=state.settings.session_cookie_name,
        value=session_token,
        httponly=True,
        secure=cookie_secure_setting(state.settings, request),
        samesite="strict",
        path="/",
        max_age=state.settings.session_ttl_hours * 60 * 60,
    )


async def save_upload(upload: UploadFile, destination: Path, request: Request) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    try:
        async with aiofiles.open(destination, "wb") as out_file:
            while True:
                if await request.is_disconnected():
                    raise asyncio.CancelledError()
                chunk = await upload.read(UPLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                written += len(chunk)
                await out_file.write(chunk)
    finally:
        await upload.close()
    return written


async def worker_loop(state: AppState) -> None:
    while True:
        job_id = await state.queue.get()
        try:
            await state.runner.run(job_id)
        except Exception as exc:
            current = await state.store.get(job_id)
            if current is not None and current.phase == JobPhase.cancelled:
                continue
            snapshot = await state.store.mutate(
                job_id,
                lambda item, message=str(exc): mark_internal_failure(item, message),
            )
            await state.store.create_activity(
                event_type="render.failed",
                description=f"Render {snapshot.id} failed unexpectedly.",
                actor_user_id=snapshot.user_id,
                subject_user_id=snapshot.user_id,
                file_id=snapshot.file_id,
                job_id=snapshot.id,
                metadata={"error": snapshot.error},
            )
        finally:
            state.queue.task_done()


def mark_internal_failure(job: JobRecord, message: str) -> None:
    if job.phase == JobPhase.cancelled:
        return
    job.phase = JobPhase.failed
    job.finished_at = utc_now()
    job.status_message = "Render failed."
    job.error = message


def mark_cancelled(job: JobRecord) -> None:
    if job.phase not in {JobPhase.queued, JobPhase.running, JobPhase.stalled}:
        return
    job.phase = JobPhase.cancelled
    job.finished_at = utc_now()
    job.status_message = "Render cancelled."
    job.error = None
    job.current_camera_name = None
    job.current_camera_index = None
    job.current_sample = None
    job.total_samples = None


def ensure_job_access(job: JobRecord | None, user: UserRecord) -> JobRecord:
    if not job or (job.user_id != user.id and user.role != UserRole.admin):
        raise HTTPException(status_code=404, detail="Run not found.")
    return job


def safe_child_path(root: Path, relative_path: str) -> Path:
    candidate = (root / relative_path).resolve()
    root_resolved = root.resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail="File not found.") from exc
    return candidate


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    settings.jobs_root.mkdir(parents=True, exist_ok=True)
    settings.files_root.mkdir(parents=True, exist_ok=True)
    settings.temp_root.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.database_path)
    await store.load()
    await store.ensure_bootstrap_admin(
        settings.admin_bootstrap_username,
        settings.admin_bootstrap_password,
    )
    await store.prune_expired_sessions()
    queue: asyncio.Queue[str] = asyncio.Queue()
    for job_id in await store.queued_job_ids():
        queue.put_nowait(job_id)
    runner = RenderRunner(settings, store)
    state = AppState(settings, store, runner, queue)
    if not settings.disable_worker:
        state.worker_task = asyncio.create_task(worker_loop(state))
    app.state.runtime = state
    try:
        yield
    finally:
        if state.worker_task:
            state.worker_task.cancel()
            try:
                await state.worker_task
            except asyncio.CancelledError:
                pass
        await store.close()


app = FastAPI(title="Render Farm", lifespan=lifespan)


async def current_session(
    request: Request,
) -> tuple[UserRecord, str] | None:
    state = runtime_state()
    session_token = request.cookies.get(state.settings.session_cookie_name)
    if not session_token:
        return None

    session_payload = await state.store.get_session_with_user(hash_session_token(session_token))
    if session_payload is None:
        return None

    session, user = session_payload
    if session.expires_at <= utc_now():
        await state.store.revoke_session(hash_session_token(session_token))
        return None

    await state.store.touch_session(session.id)
    return user, session.id


async def require_user(
    request: Request,
    session_data: tuple[UserRecord, str] | None = Depends(current_session),
) -> UserRecord:
    del request
    if session_data is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    return session_data[0]


async def require_approved_user(
    request: Request,
    session_data: tuple[UserRecord, str] | None = Depends(current_session),
) -> UserRecord:
    del request
    if session_data is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required.")
    user = session_data[0]
    if user.status != UserStatus.approved:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your account is awaiting approval.",
        )
    return user


async def require_admin_user(
    request: Request,
    session_data: tuple[UserRecord, str] | None = Depends(current_session),
) -> UserRecord:
    state = runtime_state()
    if session_data is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found.")
    user = session_data[0]
    if (
        user.role != UserRole.admin
        or user.status != UserStatus.approved
        or not lan_admin_access(request, state.settings)
    ):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not found.")
    return user


def session_payload_for_user(user: UserRecord, request: Request) -> AuthSessionPayload:
    state = runtime_state()
    return AuthSessionPayload(
        user=user,
        session=None,
        admin_panel_path=state.settings.admin_panel_path if user.role == UserRole.admin else None,
        lan_admin_access=user.role == UserRole.admin and lan_admin_access(request, state.settings),
    )


async def build_user_file_payload(user_id: int, file_id: str) -> dict:
    state = runtime_state()
    record = await state.store.get_user_file(user_id, file_id)
    if record is None:
        raise HTTPException(status_code=404, detail="File not found.")
    return record.model_dump(mode="json")


async def create_render_run(
    *,
    user: UserRecord,
    file_record: UserFileRecord,
    render_mode: RenderMode,
    output_format: OutputFormat,
    device_preference: RenderDevice,
    render_settings: RenderSettings,
    camera_names: list[str] | None,
    frame: int | None,
    start_frame: int | None,
    end_frame: int | None,
) -> JobRecord:
    state = runtime_state()
    render_settings.output_format = output_format
    render_settings = compact_render_settings(render_settings)
    render_settings.frame_step = positive_or_none(render_settings.frame_step)

    if render_mode == RenderMode.still:
        frame = 1 if frame is None else frame
        start_frame = None
        end_frame = None
    else:
        start_frame = 1 if start_frame is None else start_frame
        end_frame = start_frame if end_frame is None else end_frame
        if end_frame < start_frame:
            raise HTTPException(status_code=400, detail="End frame must be greater than or equal to start frame.")
        frame = None

    requested_cameras = unique_camera_names(camera_names)
    frames = frame_numbers_for_run(render_mode, frame, start_frame, end_frame, render_settings.frame_step)
    total_frames = len(frames)
    total_cameras = max(1, len(requested_cameras))
    total_outputs_expected = total_frames * total_cameras
    job_id = uuid.uuid4().hex[:12]
    job_root = state.settings.jobs_root / job_id
    output_dir = job_root / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = job_root / "render.log"
    job = JobRecord(
        id=job_id,
        user_id=user.id,
        file_id=file_record.id,
        source_filename=file_record.source_filename,
        source_path=file_record.source_path,
        output_directory=str(output_dir),
        render_mode=render_mode,
        output_format=output_format,
        render_settings=render_settings,
        requested_device=device_preference,
        camera_name=requested_cameras[0] if len(requested_cameras) == 1 else None,
        camera_names=requested_cameras,
        frame=frame,
        start_frame=start_frame,
        end_frame=end_frame,
        total_frames=total_frames,
        total_cameras=total_cameras,
        total_outputs_expected=total_outputs_expected,
        log_path=str(log_path),
        frame_statuses=frame_statuses_for_run(
            camera_names=requested_cameras,
            render_mode=render_mode,
            frame=frame,
            start_frame=start_frame,
            end_frame=end_frame,
            frame_step=render_settings.frame_step,
        ),
    )

    try:
        snapshot = await state.store.create(job)
        await state.store.update_user_file_render_settings(
            file_id=file_record.id,
            render_settings=render_settings_payload(snapshot.render_settings),
        )
    except Exception:
        shutil.rmtree(job_root, ignore_errors=True)
        raise

    state.queue.put_nowait(snapshot.id)
    await state.store.create_activity(
        event_type="render.queued",
        description=f"{user.username} queued render {snapshot.id} for {file_record.source_filename}.",
        actor_user_id=user.id,
        subject_user_id=user.id,
        file_id=file_record.id,
        job_id=snapshot.id,
        metadata={
            "render_mode": snapshot.render_mode.value,
            "camera_names": snapshot.camera_names,
            "frame": snapshot.frame,
            "start_frame": snapshot.start_frame,
            "end_frame": snapshot.end_frame,
            "render_settings": snapshot.render_settings.model_dump(mode="json"),
            "total_outputs_expected": snapshot.total_outputs_expected,
        },
    )
    return snapshot


async def retry_render_run(
    *,
    original: JobRecord,
    actor: UserRecord,
    admin_retry: bool = False,
) -> JobRecord:
    state = runtime_state()
    owner = await state.store.get_user_by_id(original.user_id)
    if owner is None:
        raise HTTPException(status_code=404, detail="Original run owner not found.")
    file_record = await state.store.get_file_by_id(original.file_id)
    if file_record is None:
        raise HTTPException(status_code=404, detail="Original source file not found.")
    if not admin_retry and owner.id != actor.id:
        raise HTTPException(status_code=404, detail="Run not found.")
    snapshot = await create_render_run(
        user=owner,
        file_record=file_record,
        render_mode=original.render_mode,
        output_format=original.output_format,
        device_preference=RenderDevice.auto,
        render_settings=original.render_settings,
        camera_names=original.camera_names if original.camera_names else ([original.camera_name] if original.camera_name else None),
        frame=original.frame,
        start_frame=original.start_frame,
        end_frame=original.end_frame,
    )
    await state.store.create_activity(
        event_type="admin.render_retried" if admin_retry else "render.retried",
        description=f"{actor.username} queued retry {snapshot.id} from render {original.id}.",
        actor_user_id=actor.id,
        subject_user_id=owner.id,
        file_id=original.file_id,
        job_id=snapshot.id,
        metadata={"original_job_id": original.id},
    )
    return snapshot


@app.get("/api/health")
async def healthcheck() -> dict:
    return {"ok": True}


@app.post("/api/auth/sign-up")
async def sign_up(payload: SignUpRequest, request: Request, response: Response) -> dict:
    state = runtime_state()
    if not state.settings.allow_signups:
        raise HTTPException(status_code=403, detail="Sign-ups are disabled.")

    username = normalize_username(payload.username)
    try:
        user = await state.store.create_user(username=username, password=payload.password)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="That username is already registered.") from exc

    await start_user_session(state, user, request, response)

    await state.store.create_activity(
        event_type="auth.sign_up",
        description=f"{user.username} created an account and is awaiting approval.",
        actor_user_id=user.id,
        subject_user_id=user.id,
        metadata={"ip_address": client_ip(request, state.settings)},
    )
    return session_payload_for_user(user, request).model_dump(mode="json")


@app.post("/api/auth/sign-in")
async def sign_in(payload: SignInRequest, request: Request, response: Response) -> dict:
    state = runtime_state()
    username = normalize_username(payload.username)
    user = await state.store.authenticate_user(username, payload.password)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid username or password.")

    await start_user_session(state, user, request, response)
    await state.store.create_activity(
        event_type="auth.sign_in",
        description=f"{user.username} signed in.",
        actor_user_id=user.id,
        subject_user_id=user.id,
        metadata={"ip_address": client_ip(request, state.settings)},
    )
    return session_payload_for_user(user, request).model_dump(mode="json")


@app.post("/api/auth/sign-out")
async def sign_out(
    request: Request,
    response: Response,
    session_data: tuple[UserRecord, str] | None = Depends(current_session),
) -> dict:
    state = runtime_state()
    session_token = request.cookies.get(state.settings.session_cookie_name)
    if session_token:
        await state.store.revoke_session(hash_session_token(session_token))
    response.delete_cookie(key=state.settings.session_cookie_name, path="/")
    if session_data is not None:
        await state.store.create_activity(
            event_type="auth.sign_out",
            description=f"{session_data[0].username} signed out.",
            actor_user_id=session_data[0].id,
            subject_user_id=session_data[0].id,
        )
    return {"ok": True}


@app.get("/api/auth/session")
async def auth_session(
    request: Request,
    session_data: tuple[UserRecord, str] | None = Depends(current_session),
) -> dict:
    if session_data is None:
        raise HTTPException(status_code=401, detail="Not signed in.")
    return session_payload_for_user(session_data[0], request).model_dump(mode="json")


@app.get("/api/system")
async def system_status(user: UserRecord = Depends(require_approved_user)) -> dict:
    state = runtime_state()
    jobs = await state.store.list_jobs(user.id)
    return {
        **await state.runner.system_status(),
        "job_count": len(jobs),
        "active_jobs": len([job for job in jobs if job.phase in {JobPhase.queued, JobPhase.running}]),
    }


@app.get("/api/files")
async def list_files(user: UserRecord = Depends(require_approved_user)) -> list[dict]:
    state = runtime_state()
    files = await state.store.list_user_files(user.id)
    return [item.model_dump(mode="json") for item in files]


@app.post("/api/files")
async def create_file(
    request: Request,
    blend_file: UploadFile = File(...),
    blend_file_path: str | None = Form(None),
    project_files: list[UploadFile] | None = File(None),
    project_paths: list[str] | None = Form(None),
    user: UserRecord = Depends(require_approved_user),
) -> dict:
    state = runtime_state()
    relative_source_path = (
        sanitize_relative_path(blend_file_path)
        if blend_file_path and blend_file_path.strip()
        else Path(sanitize_filename(blend_file.filename or "project.blend"))
    )
    filename = relative_source_path.as_posix()
    if not filename.lower().endswith(".blend"):
        raise HTTPException(status_code=400, detail="Only .blend files are accepted.")

    normalized_project_files = project_files or []
    normalized_project_paths = project_paths or []
    if len(normalized_project_files) != len(normalized_project_paths):
        raise HTTPException(status_code=400, detail="Project files are missing relative paths.")

    file_id = uuid.uuid4().hex[:12]
    file_root = state.settings.files_root / file_id
    source_root = file_root / "source"
    source_path = source_root / relative_source_path

    total_size = 0
    try:
        total_size += await save_upload(blend_file, source_path, request)
        for upload, path_value in zip(normalized_project_files, normalized_project_paths, strict=True):
            relative_project_path = sanitize_relative_path(path_value)
            if relative_project_path == relative_source_path:
                await upload.close()
                continue
            total_size += await save_upload(upload, source_root / relative_project_path, request)
    except asyncio.CancelledError:
        shutil.rmtree(file_root, ignore_errors=True)
        raise
    except Exception:
        shutil.rmtree(file_root, ignore_errors=True)
        raise

    record = UserFileRecord(
        id=file_id,
        user_id=user.id,
        created_at=utc_now(),
        updated_at=utc_now(),
        source_filename=filename,
        source_path=str(source_path),
        source_root=str(source_root),
        original_size_bytes=total_size,
    )
    try:
        await state.store.create_user_file(record)
    except Exception:
        shutil.rmtree(file_root, ignore_errors=True)
        raise

    await state.store.create_activity(
        event_type="file.uploaded",
        description=f"{user.username} uploaded {record.source_filename}.",
        actor_user_id=user.id,
        subject_user_id=user.id,
        file_id=record.id,
        metadata={"size_bytes": total_size},
    )
    return await build_user_file_payload(user.id, record.id)


@app.post("/api/files/{file_id}/inspect")
async def inspect_file(
    file_id: str,
    frame: int | None = Form(None),
    user: UserRecord = Depends(require_approved_user),
) -> dict:
    state = runtime_state()
    file_record = await state.store.get_user_file(user.id, file_id)
    if file_record is None:
        raise HTTPException(status_code=404, detail="File not found.")
    if not file_record.source_file.exists():
        raise HTTPException(status_code=404, detail="Stored source file is missing.")
    try:
        payload = await state.runner.inspect_blend(file_record.source_file, scan_frame=frame)
        saved_settings = render_settings_payload(file_record.render_settings)
        if saved_settings:
            blend_settings = payload.get("render_settings", {})
            payload["blend_render_settings"] = blend_settings
            payload["render_settings"] = {**blend_settings, **saved_settings}
            payload["render_settings_source"] = "saved"
        else:
            payload["render_settings_source"] = "blend"
        payload["file_size_bytes"] = file_record.original_size_bytes
        payload["source_filename"] = file_record.source_filename
        payload["processing_status"] = "complete"
        return payload
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/files/{file_id}/runs")
async def create_file_run(
    file_id: str,
    render_mode: RenderMode = Form(RenderMode.still),
    output_format: OutputFormat = Form(OutputFormat.png),
    camera_name: str | None = Form(None),
    camera_names: list[str] | None = Form(None),
    frame: int | None = Form(None),
    start_frame: int | None = Form(None),
    end_frame: int | None = Form(None),
    render_engine: str | None = Form(None),
    samples: int | None = Form(None),
    use_denoising: bool | None = Form(None),
    resolution_x: int | None = Form(None),
    resolution_y: int | None = Form(None),
    resolution_percentage: int | None = Form(None),
    frame_step: int | None = Form(None),
    fps: int | None = Form(None),
    fps_base: float | None = Form(None),
    frame_rate: float | None = Form(None),
    film_transparent: bool | None = Form(None),
    view_transform: str | None = Form(None),
    look: str | None = Form(None),
    exposure: float | None = Form(None),
    gamma: float | None = Form(None),
    image_quality: int | None = Form(None),
    compression: int | None = Form(None),
    use_motion_blur: bool | None = Form(None),
    use_simplify: bool | None = Form(None),
    simplify_subdivision: int | None = Form(None),
    simplify_child_particles: float | None = Form(None),
    simplify_volumes: float | None = Form(None),
    seed: int | None = Form(None),
    user: UserRecord = Depends(require_approved_user),
) -> dict:
    state = runtime_state()
    file_record = await state.store.get_user_file(user.id, file_id)
    if file_record is None:
        raise HTTPException(status_code=404, detail="File not found.")
    render_settings = RenderSettings(
        render_engine=safe_render_engine(render_engine),
        samples=positive_or_none(samples),
        use_denoising=use_denoising,
        resolution_x=positive_or_none(resolution_x),
        resolution_y=positive_or_none(resolution_y),
        resolution_percentage=bounded_or_none(resolution_percentage, 1, 100),
        frame_step=positive_or_none(frame_step),
        fps=positive_or_none(fps),
        fps_base=positive_float_or_none(fps_base),
        frame_rate=positive_float_or_none(frame_rate),
        film_transparent=film_transparent,
        view_transform=view_transform.strip() if view_transform and view_transform.strip() else None,
        look=look.strip() if look and look.strip() else None,
        exposure=exposure,
        gamma=gamma,
        image_quality=bounded_or_none(image_quality, 1, 100),
        compression=bounded_or_none(compression, 0, 100),
        use_motion_blur=use_motion_blur,
        use_simplify=use_simplify,
        simplify_subdivision=bounded_or_none(simplify_subdivision, 0, 16),
        simplify_child_particles=simplify_child_particles,
        simplify_volumes=simplify_volumes,
        seed=seed,
    )
    snapshot = await create_render_run(
        user=user,
        file_record=file_record,
        render_mode=render_mode,
        output_format=output_format,
        device_preference=RenderDevice.auto,
        render_settings=render_settings,
        camera_names=camera_names if camera_names else ([camera_name] if camera_name else None),
        frame=frame,
        start_frame=start_frame,
        end_frame=end_frame,
    )
    return snapshot.model_dump(mode="json")


@app.get("/api/jobs")
async def list_jobs(user: UserRecord = Depends(require_approved_user)) -> list[dict]:
    state = runtime_state()
    jobs = await state.store.list_jobs(user.id)
    return [job.model_dump(mode="json") for job in jobs]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str, user: UserRecord = Depends(require_approved_user)) -> dict:
    state = runtime_state()
    job = await state.store.get(job_id)
    job = ensure_job_access(job, user)
    return job.model_dump(mode="json")


@app.post("/api/jobs/{job_id}/cancel")
async def cancel_job(job_id: str, user: UserRecord = Depends(require_approved_user)) -> dict:
    state = runtime_state()
    job = await state.store.get(job_id)
    job = ensure_job_access(job, user)
    if job.phase not in {JobPhase.queued, JobPhase.running, JobPhase.stalled}:
        raise HTTPException(status_code=409, detail="Only queued or running jobs can be cancelled.")

    await state.runner.cancel(job_id)
    snapshot = await state.store.mutate(job_id, mark_cancelled)
    if snapshot.phase != JobPhase.cancelled:
        raise HTTPException(status_code=409, detail="This run can no longer be cancelled.")

    await state.store.create_activity(
        event_type="render.cancelled",
        description=f"{user.username} cancelled render {snapshot.id}.",
        actor_user_id=user.id,
        subject_user_id=snapshot.user_id,
        file_id=snapshot.file_id,
        job_id=snapshot.id,
    )
    return snapshot.model_dump(mode="json")


@app.post("/api/jobs/{job_id}/retry")
async def retry_job(job_id: str, user: UserRecord = Depends(require_approved_user)) -> dict:
    state = runtime_state()
    job = ensure_job_access(await state.store.get(job_id), user)
    if job.phase in {JobPhase.queued, JobPhase.running, JobPhase.packaging}:
        raise HTTPException(status_code=409, detail="Active jobs cannot be retried.")
    snapshot = await retry_render_run(original=job, actor=user)
    return snapshot.model_dump(mode="json")


@app.get("/api/jobs/{job_id}/events")
async def stream_job(
    job_id: str,
    user: UserRecord = Depends(require_approved_user),
) -> StreamingResponse:
    state = runtime_state()
    job = await state.store.get(job_id)
    ensure_job_access(job, user)

    async def event_stream():
        queue = await state.store.subscribe(job_id)
        try:
            while True:
                payload = await queue.get()
                yield f"data: {json.dumps(payload)}\n\n"
        finally:
            await state.store.unsubscribe(job_id, queue)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.get("/api/jobs/{job_id}/download")
async def download_outputs(job_id: str, user: UserRecord = Depends(require_approved_user)) -> FileResponse:
    state = runtime_state()
    job = await state.store.get(job_id)
    job = ensure_job_access(job, user)
    archive_path_value = job.archive_path
    if job.phase != JobPhase.completed or not archive_path_value:
        archive_path = await state.runner.create_archive_for_job(job)
        if archive_path:
            archive_path_value = archive_path
            if job.phase == JobPhase.completed:
                await state.store.mutate(
                    job.id,
                    lambda item, archive=archive_path: setattr(item, "archive_path", archive),
                )
        else:
            raise HTTPException(status_code=404, detail="Archive not available for this run.")
    archive_path = Path(archive_path_value)
    if not archive_path.exists():
        raise HTTPException(status_code=404, detail="Archive file missing.")
    return FileResponse(
        archive_path,
        media_type="application/zip",
        filename=f"{job.id}-outputs.zip",
    )


@app.get("/api/jobs/{job_id}/download/videos")
async def download_video_outputs(job_id: str, user: UserRecord = Depends(require_approved_user)) -> FileResponse:
    state = runtime_state()
    job = await state.store.get(job_id)
    job = ensure_job_access(job, user)
    archive_path_value = await state.runner.create_video_archive_for_job(job)
    if not archive_path_value:
        raise HTTPException(status_code=404, detail="Video archive not available for this run.")
    archive_path = Path(archive_path_value)
    if not archive_path.exists():
        raise HTTPException(status_code=404, detail="Video archive file missing.")
    return FileResponse(
        archive_path,
        media_type="application/zip",
        filename=f"{job.id}-videos.zip",
    )


@app.get("/api/jobs/{job_id}/outputs/{output_path:path}")
async def get_output_file(
    job_id: str,
    output_path: str,
    user: UserRecord = Depends(require_approved_user),
) -> FileResponse:
    state = runtime_state()
    job = ensure_job_access(await state.store.get(job_id), user)
    output_file = safe_child_path(job.output_dir, output_path)
    if not output_file.exists() or not output_file.is_file():
        raise HTTPException(status_code=404, detail="Output not found.")
    return FileResponse(output_file)


@app.get("/api/jobs/{job_id}/logs")
async def get_job_logs(job_id: str, user: UserRecord = Depends(require_approved_user)) -> FileResponse:
    state = runtime_state()
    job = ensure_job_access(await state.store.get(job_id), user)
    if not job.log_path:
        raise HTTPException(status_code=404, detail="Log not available.")
    log_path = Path(job.log_path)
    if not log_path.exists():
        raise HTTPException(status_code=404, detail="Log not available.")
    return FileResponse(log_path, media_type="text/plain", filename=f"{job.id}.log")


@app.get("/api/admin/overview")
async def admin_overview(user: UserRecord = Depends(require_admin_user)) -> dict:
    del user
    state = runtime_state()
    payload = await state.store.admin_overview()
    return payload.model_dump(mode="json")


@app.get("/api/admin/users")
async def admin_users(user: UserRecord = Depends(require_admin_user)) -> list[dict]:
    del user
    state = runtime_state()
    users = await state.store.list_users()
    return [item.model_dump(mode="json") for item in users]


@app.post("/api/admin/users/{user_id}/status")
async def admin_set_user_status(
    user_id: int,
    payload: UserModerationRequest,
    admin_user: UserRecord = Depends(require_admin_user),
) -> dict:
    state = runtime_state()
    if user_id == admin_user.id and payload.status != UserStatus.approved:
        raise HTTPException(status_code=400, detail="You cannot suspend your own admin account.")

    user = await state.store.set_user_status(
        user_id=user_id,
        status=payload.status,
        actor_user_id=admin_user.id,
    )
    if user is None:
        raise HTTPException(status_code=404, detail="User not found.")
    await state.store.create_activity(
        event_type="admin.user_status_changed",
        description=f"{admin_user.username} set {user.username} to {user.status.value}.",
        actor_user_id=admin_user.id,
        subject_user_id=user.id,
        metadata={"status": user.status.value},
    )
    return user.model_dump(mode="json")


@app.get("/api/admin/activity")
async def admin_activity(user: UserRecord = Depends(require_admin_user)) -> list[dict]:
    del user
    state = runtime_state()
    activity = await state.store.list_activity()
    return [item.model_dump(mode="json") for item in activity]


@app.get("/api/admin/runs")
async def admin_runs(user: UserRecord = Depends(require_admin_user)) -> list[dict]:
    del user
    state = runtime_state()
    jobs = await state.store.list_jobs()
    return [job.model_dump(mode="json") for job in jobs]


@app.get("/api/admin/files")
async def admin_files(user: UserRecord = Depends(require_admin_user)) -> list[dict]:
    del user
    state = runtime_state()
    files = await state.store.list_files()
    return [item.model_dump(mode="json") for item in files]


@app.get("/api/admin/files/{file_id}/download")
async def admin_download_source_file(
    file_id: str,
    user: UserRecord = Depends(require_admin_user),
) -> FileResponse:
    del user
    state = runtime_state()
    file_record = await state.store.get_file_by_id(file_id)
    if file_record is None:
        raise HTTPException(status_code=404, detail="File not found.")
    if not file_record.source_file.exists():
        raise HTTPException(status_code=404, detail="Stored source file is missing.")
    return FileResponse(
        file_record.source_file,
        media_type="application/octet-stream",
        filename=Path(file_record.source_filename).name,
    )


@app.post("/api/admin/runs/{job_id}/cancel")
async def admin_cancel_job(job_id: str, admin_user: UserRecord = Depends(require_admin_user)) -> dict:
    state = runtime_state()
    job = await state.store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    if job.phase not in {JobPhase.queued, JobPhase.running, JobPhase.stalled}:
        raise HTTPException(status_code=409, detail="Only queued or running jobs can be cancelled.")

    await state.runner.cancel(job_id)
    snapshot = await state.store.mutate(job_id, mark_cancelled)
    await state.store.create_activity(
        event_type="admin.render_cancelled",
        description=f"{admin_user.username} cancelled render {snapshot.id}.",
        actor_user_id=admin_user.id,
        subject_user_id=snapshot.user_id,
        file_id=snapshot.file_id,
        job_id=snapshot.id,
    )
    return snapshot.model_dump(mode="json")


@app.post("/api/admin/runs/{job_id}/retry")
async def admin_retry_job(job_id: str, admin_user: UserRecord = Depends(require_admin_user)) -> dict:
    state = runtime_state()
    job = await state.store.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Run not found.")
    if job.phase in {JobPhase.queued, JobPhase.running, JobPhase.packaging}:
        raise HTTPException(status_code=409, detail="Active jobs cannot be retried.")
    snapshot = await retry_render_run(original=job, actor=admin_user, admin_retry=True)
    return snapshot.model_dump(mode="json")
