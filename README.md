# Render Farm

Containerized Blender render queue for a DGX Spark. The stack exposes:

- `web`: Next.js dashboard on host port `3100`
- `api`: FastAPI render service on port `8000`

## What It Does

- Accepts `.blend` uploads from the browser
- Queues stills or frame ranges
- Runs Blender headless with Cycles inside the API container
- Streams live job updates over SSE
- Archives finished output frames for download

## Run It

```bash
docker compose up --build
```

Then open `http://localhost:3100`.

The first API build is heavy because it compiles Blender from source for `linux/arm64`.

## GPU Runtime Notes

The compose file requests all NVIDIA GPUs and defaults the Cycles device policy to:

- `AUTO`
- fallback order `CUDA,OPTIX,CPU`

You can change that in [compose.yaml](/home/maxaitel/render-farm/compose.yaml) with:

- `BLENDER_CYCLES_DEVICE`
- `BLENDER_GPU_ORDER`
- `WEB_PORT`
- `API_PORT`
- `LOCAL_UID`
- `LOCAL_GID`

The backend image builds a headless Blender from the upstream `main` source archive and exposes it at `/usr/local/bin/blender`.

## Storage Layout

Uploaded projects and renders are stored under `./data/jobs/<job-id>/`.
