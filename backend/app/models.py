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


class JobRecord(BaseModel):
    id: str
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
    frame: int | None = None
    start_frame: int | None = None
    end_frame: int | None = None
    current_frame: int | None = None
    total_frames: int = 1
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
