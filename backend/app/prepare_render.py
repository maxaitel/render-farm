import os

import bpy


def disable_unavailable_denoisers() -> None:
    scene = bpy.context.scene

    if hasattr(scene, "cycles"):
        if hasattr(scene.cycles, "use_preview_denoising"):
            scene.cycles.use_preview_denoising = False
        if hasattr(scene.cycles, "use_denoising"):
            scene.cycles.use_denoising = False

    for view_layer in scene.view_layers:
        cycles_settings = getattr(view_layer, "cycles", None)
        if cycles_settings and hasattr(cycles_settings, "use_denoising"):
            cycles_settings.use_denoising = False

    use_nodes = bool(getattr(scene, "use_nodes", False))
    node_tree = getattr(scene, "node_tree", None)
    if use_nodes and node_tree:
        for node in node_tree.nodes:
            if getattr(node, "type", "") == "DENOISE":
                node.mute = True


def apply_requested_camera() -> None:
    camera_name = os.getenv("RENDER_CAMERA_NAME", "").strip()
    if not camera_name:
        return

    camera = bpy.data.objects.get(camera_name)
    if camera is None or camera.type != "CAMERA":
        raise SystemExit(f"Requested camera '{camera_name}' was not found in the blend file.")

    bpy.context.scene.camera = camera


disable_unavailable_denoisers()
apply_requested_camera()
