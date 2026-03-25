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
    completed = "completed"
    failed = "failed"


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
    requested_device: RenderDevice = RenderDevice.auto
    resolved_device: str | None = None
    camera_name: str | None = None
    camera_names: list[str] = Field(default_factory=list)
    current_camera_name: str | None = None
    frame: int | None = None
    start_frame: int | None = None
    end_frame: int | None = None
    current_frame: int | None = None
    current_frame_started_at: datetime | None = None
    current_frame_elapsed_seconds: float | None = None
    total_frames: int = 1
    last_frame_duration_seconds: float | None = None
    average_frame_duration_seconds: float | None = None
    current_sample: int | None = None
    total_samples: int | None = None
    outputs: list[str] = Field(default_factory=list)
    logs_tail: list[str] = Field(default_factory=list)
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
