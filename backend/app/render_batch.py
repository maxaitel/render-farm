from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import bpy

MARKER_PREFIX = "RENDER_FARM_EVENT "


def parse_args() -> argparse.Namespace:
    argv = []
    if "--" in sys.argv:
        argv = sys.argv[sys.argv.index("--") + 1 :]

    parser = argparse.ArgumentParser()
    parser.add_argument("--render-plan", required=True)
    return parser.parse_args(argv)


def emit(event: str, **payload: Any) -> None:
    print(f"{MARKER_PREFIX}{json.dumps({'event': event, **payload}, sort_keys=True)}", flush=True)


def configure_cycles_device(device: str) -> None:
    device = device.upper()
    scene = bpy.context.scene
    cycles = getattr(scene, "cycles", None)
    if cycles is None:
        return

    if device == "CPU":
        cycles.device = "CPU"
        return

    preferences = bpy.context.preferences.addons.get("cycles")
    if preferences is None:
        return

    cycles.device = "GPU"
    prefs = preferences.preferences
    if hasattr(prefs, "compute_device_type"):
        prefs.compute_device_type = device
    if hasattr(prefs, "refresh_devices"):
        prefs.refresh_devices()
    if hasattr(prefs, "get_devices_for_type"):
        for candidate in prefs.get_devices_for_type(device):
            candidate.use = True
        for candidate in prefs.get_devices_for_type("CPU"):
            candidate.use = False


def set_output_format(output_format: str) -> None:
    render = bpy.context.scene.render
    render.image_settings.file_format = output_format
    render.use_file_extension = True


def scene_info_payload() -> dict[str, Any]:
    scene = bpy.context.scene
    render = scene.render
    fps = int(render.fps)
    fps_base = float(render.fps_base or 1.0)
    return {
        "fps": fps,
        "fps_base": fps_base,
        "frame_rate": fps / fps_base,
        "frame_step": int(scene.frame_step),
    }


def write_scene_info(plan: dict[str, Any], payload: dict[str, Any]) -> None:
    scene_info_path = plan.get("scene_info_path")
    if not isinstance(scene_info_path, str) or not scene_info_path:
        return
    path = Path(scene_info_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def set_camera(camera_name: str | None) -> str | None:
    if not camera_name:
        camera = bpy.context.scene.camera
        return camera.name if camera else None

    camera = bpy.data.objects.get(camera_name)
    if camera is None or camera.type != "CAMERA":
        raise SystemExit(f"Requested camera '{camera_name}' was not found in the blend file.")

    bpy.context.scene.camera = camera
    return camera.name


def render_camera(plan: dict[str, Any], camera: dict[str, Any]) -> None:
    scene = bpy.context.scene
    camera_name = camera.get("camera_name")
    resolved_camera_name = set_camera(camera_name if isinstance(camera_name, str) else None)
    camera_index = int(camera["camera_index"])
    total_cameras = int(plan["total_cameras"])
    output_pattern = str(camera["output_pattern"])

    scene.render.filepath = output_pattern
    emit(
        "camera_started",
        camera_name=resolved_camera_name,
        requested_camera_name=camera_name,
        camera_index=camera_index,
        total_cameras=total_cameras,
    )

    if plan["render_mode"] == "animation":
        scene.frame_start = int(plan["start_frame"])
        scene.frame_end = int(plan["end_frame"])
        scene.frame_step = max(1, int(plan.get("frame_step") or 1))
        bpy.ops.render.render(animation=True)
    else:
        frame = int(plan["frame"])
        scene.frame_set(frame)
        scene.render.filepath = output_pattern.replace("#####", f"{frame:05d}")
        bpy.ops.render.render(write_still=True)

    emit(
        "camera_completed",
        camera_name=resolved_camera_name,
        requested_camera_name=camera_name,
        camera_index=camera_index,
        total_cameras=total_cameras,
    )


def main() -> None:
    args = parse_args()
    plan = json.loads(Path(args.render_plan).read_text("utf-8"))
    configure_cycles_device(str(plan["device"]))
    set_output_format(str(plan["output_format"]))
    scene_info = scene_info_payload()
    write_scene_info(plan, scene_info)

    emit(
        "batch_started",
        device=plan["device"],
        cameras=len(plan["cameras"]),
        fps=scene_info["fps"],
        fps_base=scene_info["fps_base"],
        frame_rate=scene_info["frame_rate"],
        render_mode=plan["render_mode"],
    )
    for camera in plan["cameras"]:
        render_camera(plan, camera)
    emit("batch_completed", device=plan["device"], cameras=len(plan["cameras"]))


if __name__ == "__main__":
    main()
