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
    parser.add_argument("--frame", type=int, default=None)
    return parser.parse_args(argv)


def camera_payload(camera: bpy.types.Object) -> dict:
    return {"name": camera.name}


def cycles_samples(scene: bpy.types.Scene) -> int | None:
    cycles = getattr(scene, "cycles", None)
    if cycles is None:
        return None
    return getattr(cycles, "samples", None)


def cycles_seed(scene: bpy.types.Scene) -> int | None:
    cycles = getattr(scene, "cycles", None)
    if cycles is None:
        return None
    return getattr(cycles, "seed", None)


def cycles_denoising(scene: bpy.types.Scene) -> bool | None:
    cycles = getattr(scene, "cycles", None)
    if cycles is not None and hasattr(cycles, "use_denoising"):
        return bool(cycles.use_denoising)
    view_layer = bpy.context.view_layer
    layer_cycles = getattr(view_layer, "cycles", None)
    if layer_cycles is not None and hasattr(layer_cycles, "use_denoising"):
        return bool(layer_cycles.use_denoising)
    return None


def image_settings_payload(scene: bpy.types.Scene) -> dict:
    image_settings = scene.render.image_settings
    return {
        "file_format": image_settings.file_format,
        "quality": getattr(image_settings, "quality", None),
        "compression": getattr(image_settings, "compression", None),
    }


def render_settings_payload(scene: bpy.types.Scene) -> dict:
    render = scene.render
    cycles = getattr(scene, "cycles", None)
    simplify = render
    view_settings = scene.view_settings
    fps = int(render.fps)
    fps_base = float(render.fps_base or 1.0)
    return {
        "render_engine": render.engine,
        "output_format": render.image_settings.file_format,
        "samples": cycles_samples(scene),
        "use_denoising": cycles_denoising(scene),
        "resolution_x": render.resolution_x,
        "resolution_y": render.resolution_y,
        "resolution_percentage": render.resolution_percentage,
        "frame_step": scene.frame_step,
        "fps": fps,
        "fps_base": fps_base,
        "frame_rate": fps / fps_base,
        "film_transparent": render.film_transparent,
        "view_transform": view_settings.view_transform,
        "look": view_settings.look,
        "exposure": view_settings.exposure,
        "gamma": view_settings.gamma,
        "image_quality": getattr(render.image_settings, "quality", None),
        "compression": getattr(render.image_settings, "compression", None),
        "use_motion_blur": getattr(render, "use_motion_blur", None),
        "use_simplify": getattr(simplify, "use_simplify", None),
        "simplify_subdivision": getattr(simplify, "simplify_subdivision", None),
        "simplify_child_particles": getattr(simplify, "simplify_child_particles", None),
        "simplify_volumes": getattr(simplify, "simplify_volumes", None),
        "seed": cycles_seed(scene),
    }


def dependency_warnings() -> list[str]:
    warnings: list[str] = []
    for library in bpy.data.libraries:
        path = bpy.path.abspath(library.filepath)
        if path and not Path(path).exists():
            warnings.append(f"Missing linked library: {library.filepath}")
    for image in bpy.data.images:
        if not image.filepath:
            continue
        path = bpy.path.abspath(image.filepath)
        if path and not Path(path).exists():
            warnings.append(f"Missing image: {image.filepath}")
    return warnings


def main() -> None:
    args = parse_args()
    scene = bpy.context.scene
    if args.frame is not None:
        scene.frame_set(args.frame)

    cameras = [obj for obj in scene.objects if obj.type == "CAMERA"]
    payload = {
        "default_camera": scene.camera.name if scene.camera else None,
        "frame": scene.frame_current,
        "frame_start": scene.frame_start,
        "frame_end": scene.frame_end,
        "frame_step": scene.frame_step,
        "cameras": [],
        "resolution": {
            "x": scene.render.resolution_x,
            "y": scene.render.resolution_y,
            "percentage": scene.render.resolution_percentage,
        },
        "render_engine": scene.render.engine,
        "samples": cycles_samples(scene),
        "output_format": scene.render.image_settings.file_format,
        "image_settings": image_settings_payload(scene),
        "render_settings": render_settings_payload(scene),
        "estimated_output_files": max(1, len(cameras))
        * len(range(scene.frame_start, scene.frame_end + 1, max(1, scene.frame_step))),
        "scene_collections": [collection.name for collection in bpy.data.collections],
        "asset_warnings": dependency_warnings(),
    }
    for camera in cameras:
        payload["cameras"].append(camera_payload(camera))

    Path(args.output_json).write_text(json.dumps(payload), encoding="utf-8")

if __name__ == "__main__":
    main()
