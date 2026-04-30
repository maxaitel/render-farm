from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class JobPhase(str, Enum):
    queued = "queued"
    running = "running"
    packaging = "packaging"
    completed = "completed"
    failed = "failed"
    stalled = "stalled"
    cancelled = "cancelled"


class RenderMode(str, Enum):
    still = "still"
    animation = "animation"


class OutputFormat(str, Enum):
    png = "PNG"
    jpeg = "JPEG"
    open_exr = "OPEN_EXR"


class RenderDevice(str, Enum):
    auto = "AUTO"
    cuda = "CUDA"
    optix = "OPTIX"
    cpu = "CPU"


class UserRole(str, Enum):
    user = "user"
    admin = "admin"


class UserStatus(str, Enum):
    pending = "pending"
    approved = "approved"
    suspended = "suspended"


class FramePhase(str, Enum):
    pending = "pending"
    rendering = "rendering"
    complete = "complete"
    failed = "failed"
    retrying = "retrying"
    skipped = "skipped"


class RenderSettings(BaseModel):
    render_engine: str | None = None
    output_format: OutputFormat | None = None
    samples: int | None = None
    use_denoising: bool | None = None
    resolution_x: int | None = None
    resolution_y: int | None = None
    resolution_percentage: int | None = None
    frame_step: int | None = None
    fps: int | None = None
    fps_base: float | None = None
    frame_rate: float | None = None
    film_transparent: bool | None = None
    view_transform: str | None = None
    look: str | None = None
    exposure: float | None = None
    gamma: float | None = None
    image_quality: int | None = None
    compression: int | None = None
    use_motion_blur: bool | None = None
    use_simplify: bool | None = None
    simplify_subdivision: int | None = None
    simplify_child_particles: float | None = None
    simplify_volumes: float | None = None
    seed: int | None = None


class FrameRenderRecord(BaseModel):
    camera_name: str | None = None
    camera_index: int = 1
    frame: int = 1
    status: FramePhase = FramePhase.pending
    output_path: str | None = None
    attempts: int = 0
    error: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    seconds: float | None = None


class JobRecord(BaseModel):
    id: str
    user_id: int
    file_id: str
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    phase: JobPhase = JobPhase.queued
    progress: float = 0.0
    status_message: str = "Queued for rendering."
    source_filename: str
    source_path: str
    output_directory: str
    archive_path: str | None = None
    render_mode: RenderMode
    output_format: OutputFormat
    render_settings: RenderSettings = Field(default_factory=RenderSettings)
    requested_device: RenderDevice = RenderDevice.auto
    resolved_device: str | None = None
    worker_assigned: str | None = None
    queue_position: int | None = None
    priority: int = 0
    camera_name: str | None = None
    camera_names: list[str] = Field(default_factory=list)
    current_camera_name: str | None = None
    current_camera_index: int | None = None
    total_cameras: int = 1
    frame: int | None = None
    start_frame: int | None = None
    end_frame: int | None = None
    current_frame: int | None = None
    total_frames: int = 1
    total_outputs_expected: int = 1
    completed_frames: int = 0
    failed_frames: int = 0
    current_output: str | None = None
    elapsed_seconds: float | None = None
    estimated_seconds_remaining: float | None = None
    average_seconds_per_frame: float | None = None
    last_progress_at: datetime | None = None
    current_sample: int | None = None
    total_samples: int | None = None
    outputs: list[str] = Field(default_factory=list)
    frame_statuses: list[FrameRenderRecord] = Field(default_factory=list)
    logs_tail: list[str] = Field(default_factory=list)
    log_path: str | None = None
    command: list[str] = Field(default_factory=list)
    environment_info: dict = Field(default_factory=dict)
    error: str | None = None

    @property
    def source_file(self) -> Path:
        return Path(self.source_path)

    @property
    def output_dir(self) -> Path:
        return Path(self.output_directory)


class UserRecord(BaseModel):
    id: int
    username: str
    role: UserRole
    status: UserStatus
    created_at: datetime
    approved_at: datetime | None = None
    approved_by_user_id: int | None = None
    last_login_at: datetime | None = None
    render_file_count: int = 0
    run_count: int = 0


class UserSessionRecord(BaseModel):
    id: str
    user_id: int
    created_at: datetime
    expires_at: datetime
    last_seen_at: datetime
    ip_address: str | None = None
    user_agent: str | None = None


class UserFileRecord(BaseModel):
    id: str
    user_id: int
    created_at: datetime
    updated_at: datetime
    source_filename: str
    source_path: str
    source_root: str
    original_size_bytes: int
    render_settings: RenderSettings = Field(default_factory=RenderSettings)
    latest_job: JobRecord | None = None
    jobs: list[JobRecord] = Field(default_factory=list)

    @property
    def source_file(self) -> Path:
        return Path(self.source_path)

    @property
    def source_tree(self) -> Path:
        return Path(self.source_root)


class ActivityRecord(BaseModel):
    id: int
    created_at: datetime
    event_type: str
    description: str
    actor_user_id: int | None = None
    actor_username: str | None = None
    subject_user_id: int | None = None
    subject_username: str | None = None
    file_id: str | None = None
    job_id: str | None = None
    metadata: dict = Field(default_factory=dict)


class AuthSessionPayload(BaseModel):
    user: UserRecord
    session: UserSessionRecord | None = None
    admin_panel_path: str | None = None
    lan_admin_access: bool = False


class AdminOverview(BaseModel):
    pending_users: int
    approved_users: int
    suspended_users: int
    total_files: int
    total_runs: int
    active_runs: int
