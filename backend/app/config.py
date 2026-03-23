from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    storage_root: Path
    blender_binary: str
    default_device: str
    gpu_order: list[str]

    @property
    def jobs_root(self) -> Path:
        return self.storage_root / "jobs"

    @property
    def temp_root(self) -> Path:
        return self.storage_root / "tmp"


def load_settings() -> Settings:
    storage_root = Path(os.getenv("RENDER_STORAGE_ROOT", "/data")).resolve()
    blender_binary = os.getenv("BLENDER_BINARY", "/usr/bin/blender")
    default_device = os.getenv("BLENDER_CYCLES_DEVICE", "AUTO").upper()
    gpu_order_raw = os.getenv("BLENDER_GPU_ORDER", "CUDA,OPTIX,CPU")
    gpu_order = [item.strip().upper() for item in gpu_order_raw.split(",") if item.strip()]
    return Settings(
        storage_root=storage_root,
        blender_binary=blender_binary,
        default_device=default_device,
        gpu_order=gpu_order or ["CUDA", "CPU"],
    )

