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


def main() -> None:
    args = parse_args()
    scene = bpy.context.scene
    if args.frame is not None:
        scene.frame_set(args.frame)

    cameras = [obj for obj in scene.objects if obj.type == "CAMERA"]
    payload = {
        "default_camera": scene.camera.name if scene.camera else None,
        "frame": scene.frame_current,
        "cameras": [],
    }
    for camera in cameras:
        payload["cameras"].append(camera_payload(camera))

    Path(args.output_json).write_text(json.dumps(payload), encoding="utf-8")

if __name__ == "__main__":
    main()
