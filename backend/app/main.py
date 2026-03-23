from __future__ import annotations

import asyncio
import json
import re
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiofiles
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse

from .config import Settings, load_settings
from .models import JobPhase, JobRecord, OutputFormat, RenderDevice, RenderMode, utc_now
from .renderer import RenderRunner
from .store import JobStore

FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


class AppState:
    def __init__(self, settings: Settings, store: JobStore, runner: RenderRunner, queue: asyncio.Queue[str]) -> None:
        self.settings = settings
        self.store = store
        self.runner = runner
        self.queue = queue
        self.worker_task: asyncio.Task[None] | None = None


def sanitize_filename(filename: str) -> str:
    cleaned = FILENAME_RE.sub("-", filename).strip("-.")
    return cleaned or "project.blend"


async def save_upload(upload: UploadFile, destination: Path) -> None:
    async with aiofiles.open(destination, "wb") as out_file:
        while True:
            chunk = await upload.read(1024 * 1024)
            if not chunk:
                break
            await out_file.write(chunk)
    await upload.close()


async def worker_loop(state: AppState) -> None:
    while True:
        job_id = await state.queue.get()
        try:
            await state.runner.run(job_id)
        except Exception as exc:
            await state.store.mutate(
                job_id,
                lambda item, message=str(exc): mark_internal_failure(item, message),
            )
        finally:
            state.queue.task_done()


def mark_internal_failure(job: JobRecord, message: str) -> None:
    job.phase = JobPhase.failed
    job.finished_at = utc_now()
    job.status_message = "Render failed."
    job.error = message


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = load_settings()
    settings.jobs_root.mkdir(parents=True, exist_ok=True)
    settings.temp_root.mkdir(parents=True, exist_ok=True)
    store = JobStore(settings.jobs_root)
    await store.load()
    queue: asyncio.Queue[str] = asyncio.Queue()
    for job_id in await store.queued_job_ids():
        queue.put_nowait(job_id)
    runner = RenderRunner(settings, store)
    state = AppState(settings, store, runner, queue)
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


app = FastAPI(title="Render Farm", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def runtime_state() -> AppState:
    return app.state.runtime  # type: ignore[return-value]


@app.get("/api/health")
async def healthcheck() -> dict:
    return {"ok": True}


@app.get("/api/system")
async def system_status() -> dict:
    state = runtime_state()
    jobs = await state.store.list_jobs()
    return {
        **await state.runner.system_status(),
        "job_count": len(jobs),
        "active_jobs": len([job for job in jobs if job.phase in {"queued", "running"}]),
    }


@app.get("/api/jobs")
async def list_jobs() -> list[dict]:
    state = runtime_state()
    jobs = await state.store.list_jobs()
    return [job.model_dump(mode="json") for job in jobs]


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> dict:
    state = runtime_state()
    job = await state.store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")
    return job.model_dump(mode="json")


@app.post("/api/jobs")
async def create_job(
    blend_file: UploadFile = File(...),
    render_mode: RenderMode = Form(RenderMode.still),
    output_format: OutputFormat = Form(OutputFormat.png),
    device_preference: RenderDevice = Form(RenderDevice.auto),
    frame: int | None = Form(None),
    start_frame: int | None = Form(None),
    end_frame: int | None = Form(None),
) -> dict:
    state = runtime_state()
    filename = sanitize_filename(blend_file.filename or "project.blend")
    if not filename.lower().endswith(".blend"):
        raise HTTPException(status_code=400, detail="Only .blend files are accepted.")

    if render_mode == RenderMode.still:
        frame = frame or 1
        start_frame = None
        end_frame = None
        total_frames = 1
    else:
        start_frame = start_frame or 1
        end_frame = end_frame or start_frame
        if end_frame < start_frame:
            raise HTTPException(status_code=400, detail="End frame must be greater than or equal to start frame.")
        frame = None
        total_frames = end_frame - start_frame + 1

    job_id = uuid.uuid4().hex[:12]
    job_root = state.settings.jobs_root / job_id
    input_dir = job_root / "input"
    output_dir = job_root / "outputs"
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    source_path = input_dir / filename
    await save_upload(blend_file, source_path)

    job = JobRecord(
        id=job_id,
        source_filename=filename,
        source_path=str(source_path),
        output_directory=str(output_dir),
        render_mode=render_mode,
        output_format=output_format,
        requested_device=device_preference,
        frame=frame,
        start_frame=start_frame,
        end_frame=end_frame,
        total_frames=total_frames,
    )
    snapshot = await state.store.create(job)
    state.queue.put_nowait(job_id)
    return snapshot.model_dump(mode="json")


@app.get("/api/jobs/{job_id}/events")
async def stream_job(job_id: str) -> StreamingResponse:
    state = runtime_state()
    job = await state.store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found.")

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
async def download_outputs(job_id: str) -> FileResponse:
    state = runtime_state()
    job = await state.store.get(job_id)
    if not job or not job.archive_path:
        raise HTTPException(status_code=404, detail="Archive not available for this job.")
    archive_path = Path(job.archive_path)
    if not archive_path.exists():
        raise HTTPException(status_code=404, detail="Archive file missing.")
    return FileResponse(
        archive_path,
        media_type="application/zip",
        filename=f"{job.id}-outputs.zip",
    )
