from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _parse_bool(value: str | None, *, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    storage_root: Path
    blender_binary: str
    default_device: str
    gpu_order: list[str]
    disable_worker: bool
    session_cookie_name: str
    session_ttl_hours: int
    auth_cookie_secure: str
    admin_panel_path: str
    admin_bootstrap_username: str | None
    admin_bootstrap_password: str | None
    allow_signups: bool
    trusted_proxies: list[str]
    cycles_print_stats: bool = False

    @property
    def database_path(self) -> Path:
        return self.storage_root / "renderfarm.sqlite3"

    @property
    def jobs_root(self) -> Path:
        return self.storage_root / "jobs"

    @property
    def temp_root(self) -> Path:
        return self.storage_root / "tmp"

    @property
    def files_root(self) -> Path:
        return self.storage_root / "files"


def load_settings() -> Settings:
    storage_root = Path(os.getenv("RENDER_STORAGE_ROOT", "/data")).resolve()
    blender_binary = os.getenv("BLENDER_BINARY", "/usr/bin/blender")
    default_device = os.getenv("BLENDER_CYCLES_DEVICE", "AUTO").upper()
    gpu_order_raw = os.getenv("BLENDER_GPU_ORDER", "CUDA,OPTIX,CPU")
    gpu_order = [item.strip().upper() for item in gpu_order_raw.split(",") if item.strip()]
    trusted_proxies_raw = os.getenv("TRUSTED_PROXIES", "")
    trusted_proxies = [item.strip() for item in trusted_proxies_raw.split(",") if item.strip()]
    disable_worker = _parse_bool(os.getenv("DISABLE_RENDER_WORKER"))
    admin_panel_path = os.getenv("ADMIN_PANEL_PATH", "control-tower").strip().strip("/")
    return Settings(
        storage_root=storage_root,
        blender_binary=blender_binary,
        default_device=default_device,
        gpu_order=gpu_order or ["CUDA", "CPU"],
        disable_worker=disable_worker,
        session_cookie_name=os.getenv("SESSION_COOKIE_NAME", "renderfarm_session"),
        session_ttl_hours=int(os.getenv("SESSION_TTL_HOURS", "336")),
        auth_cookie_secure=os.getenv("AUTH_COOKIE_SECURE", "auto").strip().lower() or "auto",
        admin_panel_path=admin_panel_path or "control-tower",
        admin_bootstrap_username=os.getenv("ADMIN_BOOTSTRAP_USERNAME", "").strip() or None,
        admin_bootstrap_password=os.getenv("ADMIN_BOOTSTRAP_PASSWORD", "").strip() or None,
        allow_signups=_parse_bool(os.getenv("ALLOW_SIGNUPS"), default=True),
        trusted_proxies=trusted_proxies,
        cycles_print_stats=_parse_bool(os.getenv("BLENDER_CYCLES_PRINT_STATS")),
    )
