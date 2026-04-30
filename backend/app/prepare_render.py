import json
import os

import bpy


def render_settings() -> dict:
    raw = os.getenv("RENDER_SETTINGS_JSON", "").strip()
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def apply_render_settings(settings: dict) -> None:
    scene = bpy.context.scene
    render = scene.render

    render_engine = settings.get("render_engine")
    if isinstance(render_engine, str) and render_engine:
        render.engine = render_engine

    resolution_x = settings.get("resolution_x")
    if isinstance(resolution_x, int) and resolution_x > 0:
        render.resolution_x = resolution_x

    resolution_y = settings.get("resolution_y")
    if isinstance(resolution_y, int) and resolution_y > 0:
        render.resolution_y = resolution_y

    resolution_percentage = settings.get("resolution_percentage")
    if isinstance(resolution_percentage, int) and 1 <= resolution_percentage <= 100:
        render.resolution_percentage = resolution_percentage

    frame_step = settings.get("frame_step")
    if isinstance(frame_step, int) and frame_step > 0:
        scene.frame_step = frame_step

    fps = settings.get("fps")
    if isinstance(fps, int) and fps > 0:
        render.fps = fps

    fps_base = settings.get("fps_base")
    if isinstance(fps_base, (int, float)) and fps_base > 0:
        render.fps_base = fps_base

    film_transparent = settings.get("film_transparent")
    if isinstance(film_transparent, bool):
        render.film_transparent = film_transparent

    image_quality = settings.get("image_quality")
    if isinstance(image_quality, int) and 1 <= image_quality <= 100:
        render.image_settings.quality = image_quality

    compression = settings.get("compression")
    if isinstance(compression, int) and 0 <= compression <= 100:
        render.image_settings.compression = compression

    use_motion_blur = settings.get("use_motion_blur")
    if isinstance(use_motion_blur, bool) and hasattr(render, "use_motion_blur"):
        render.use_motion_blur = use_motion_blur

    use_simplify = settings.get("use_simplify")
    if isinstance(use_simplify, bool) and hasattr(render, "use_simplify"):
        render.use_simplify = use_simplify

    simplify_subdivision = settings.get("simplify_subdivision")
    if isinstance(simplify_subdivision, int) and hasattr(render, "simplify_subdivision"):
        render.simplify_subdivision = simplify_subdivision

    simplify_child_particles = settings.get("simplify_child_particles")
    if isinstance(simplify_child_particles, (int, float)) and hasattr(render, "simplify_child_particles"):
        render.simplify_child_particles = simplify_child_particles

    simplify_volumes = settings.get("simplify_volumes")
    if isinstance(simplify_volumes, (int, float)) and hasattr(render, "simplify_volumes"):
        render.simplify_volumes = simplify_volumes

    view_transform = settings.get("view_transform")
    if isinstance(view_transform, str) and view_transform:
        scene.view_settings.view_transform = view_transform

    look = settings.get("look")
    if isinstance(look, str):
        scene.view_settings.look = look

    exposure = settings.get("exposure")
    if isinstance(exposure, (int, float)):
        scene.view_settings.exposure = exposure

    gamma = settings.get("gamma")
    if isinstance(gamma, (int, float)) and gamma > 0:
        scene.view_settings.gamma = gamma

    cycles_settings = getattr(scene, "cycles", None)
    if cycles_settings:
        samples = settings.get("samples")
        if isinstance(samples, int) and samples > 0 and hasattr(cycles_settings, "samples"):
            cycles_settings.samples = samples

        seed = settings.get("seed")
        if isinstance(seed, int) and hasattr(cycles_settings, "seed"):
            cycles_settings.seed = seed

        use_denoising = settings.get("use_denoising")
        if isinstance(use_denoising, bool):
            if hasattr(cycles_settings, "use_preview_denoising"):
                cycles_settings.use_preview_denoising = use_denoising
            if hasattr(cycles_settings, "use_denoising"):
                cycles_settings.use_denoising = use_denoising

    for view_layer in scene.view_layers:
        cycles_settings = getattr(view_layer, "cycles", None)
        use_denoising = settings.get("use_denoising")
        if isinstance(use_denoising, bool) and cycles_settings and hasattr(cycles_settings, "use_denoising"):
            cycles_settings.use_denoising = use_denoising

    if settings.get("use_denoising") is False:
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


apply_render_settings(render_settings())
apply_requested_camera()
