from __future__ import annotations

import argparse
import json
from pathlib import Path

import bpy


def parse_args() -> argparse.Namespace:
    argv = []
    if "--" in __import__("sys").argv:
        argv = __import__("sys").argv[__import__("sys").argv.index("--") + 1 :]

    parser = argparse.ArgumentParser()
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--preview-dir", required=True)
    parser.add_argument("--frame", type=int, default=None)
    return parser.parse_args(argv)


def choose_preview_engine() -> str:
    engines = bpy.context.scene.render.bl_rna.properties["engine"].enum_items.keys()
    for candidate in ("BLENDER_WORKBENCH", "BLENDER_EEVEE_NEXT", "BLENDER_EEVEE", "CYCLES"):
        if candidate in engines:
            return candidate
    return bpy.context.scene.render.engine


def safe_name(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in {"-", "_"} else "-" for char in value)
    return cleaned.strip("-_") or "camera"


def camera_payload(camera: bpy.types.Object, preview_path: Path | None) -> dict:
    return {
        "name": camera.name,
        "preview_path": str(preview_path) if preview_path else None,
    }


def render_preview(
    scene: bpy.types.Scene,
    camera: bpy.types.Object,
    preview_dir: Path,
    preview_stem: str,
) -> Path | None:
    output_path = preview_dir / f"{preview_stem}.png"

    original_camera = scene.camera
    original_engine = scene.render.engine
    original_filepath = scene.render.filepath
    original_format = scene.render.image_settings.file_format
    original_color_mode = scene.render.image_settings.color_mode
    original_res_x = scene.render.resolution_x
    original_res_y = scene.render.resolution_y
    original_res_pct = scene.render.resolution_percentage
    original_use_ext = scene.render.use_file_extension

    try:
        scene.camera = camera
        scene.render.engine = choose_preview_engine()
        scene.render.filepath = str(output_path)
        scene.render.image_settings.file_format = "PNG"
        scene.render.image_settings.color_mode = "RGB"
        scene.render.use_file_extension = True

        width = max(320, min(scene.render.resolution_x, 640))
        height = max(180, int(width * (scene.render.resolution_y / max(scene.render.resolution_x, 1))))
        scene.render.resolution_x = width
        scene.render.resolution_y = min(height, 360)
        scene.render.resolution_percentage = 100

        cycles = getattr(scene, "cycles", None)
        if cycles and hasattr(cycles, "samples"):
            cycles.samples = min(int(cycles.samples), 8) if int(cycles.samples) > 0 else 8
        bpy.ops.render.render(write_still=True)
    except Exception:
        return None
    finally:
        scene.camera = original_camera
        scene.render.engine = original_engine
        scene.render.filepath = original_filepath
        scene.render.image_settings.file_format = original_format
        scene.render.image_settings.color_mode = original_color_mode
        scene.render.resolution_x = original_res_x
        scene.render.resolution_y = original_res_y
        scene.render.resolution_percentage = original_res_pct
        scene.render.use_file_extension = original_use_ext

    return output_path if output_path.exists() else None


def main() -> None:
    args = parse_args()
    scene = bpy.context.scene
    if args.frame is not None:
        scene.frame_set(args.frame)

    preview_dir = Path(args.preview_dir)
    preview_dir.mkdir(parents=True, exist_ok=True)

    cameras = [obj for obj in scene.objects if obj.type == "CAMERA"]
    payload = {
        "default_camera": scene.camera.name if scene.camera else None,
        "frame": scene.frame_current,
        "cameras": [],
    }
    for index, camera in enumerate(cameras, start=1):
        preview_path = render_preview(
            scene,
            camera,
            preview_dir,
            f"{index:03d}-{safe_name(camera.name)}",
        )
        payload["cameras"].append(camera_payload(camera, preview_path))

    Path(args.output_json).write_text(json.dumps(payload), encoding="utf-8")


main()
