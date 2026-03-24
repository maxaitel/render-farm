# Render Farm

Containerized Blender render queue. The stack exposes:

- `web`: Next.js dashboard on host port `3100`
- `api`: FastAPI render service on port `8000`

## What It Does

- Creates account-based access with pending approval
- Stores uploaded `.blend` projects in a per-user file library
- Lets users rerun the same source scene with different cameras or frame ranges
- Runs Blender headless with Cycles inside the API container
- Streams live job updates over SSE
- Bundles finished output frames for download
- Exposes a LAN-only admin panel behind a non-obvious URL slug

## Run It

```bash
docker compose up --build
```

Before doing that for a real deployment, copy [.env.example](/home/maxaitel/render-farm/.env.example) to `.env` and set:

- `ADMIN_BOOTSTRAP_USERNAME`
- `ADMIN_BOOTSTRAP_PASSWORD`
- `ADMIN_PANEL_PATH`

Then open `http://localhost:3100`.

The first admin login uses the bootstrap credentials above. New sign-ups stay in
the `pending` state until that admin approves them from the hidden admin route:

- `http://<host>:3100/<ADMIN_PANEL_PATH>`

That route is also restricted to private/LAN client IPs.
The API trusts forwarded client IP headers only from `TRUSTED_PROXIES`, which
defaults to the local host and Docker bridge addresses used by the bundled
`web -> api` proxy path.

## Requirements

- NVIDIA GPU runtime available to Docker
- A headless Blender build at `backend/.blender-build_release_headless`

The API container does not build Blender itself. It mounts the local Blender
build into the container at `/blender` and runs it through
`/usr/local/bin/blender-wrapper`.

## Blender Build Provenance

The Blender runtime used here comes from the prebuilt AArch64 release published
at:

- `CoconutMacaroon/blender-arm64`
- release tag: `v4-5.0.1`
- release page: [CoconutMacaroon/blender-arm64 v4-5.0.1](https://github.com/CoconutMacaroon/blender-arm64/releases/tag/v4-5.0.1)

The local archive in this workspace is:

- `backend/.blender-dist/blender-v4.tar.xz`

That archive matches the GitHub release asset exactly:

- asset name: `blender-v4.tar.xz`
- size: `307,698,672` bytes
- SHA-256: `18d3fb97da839c9a90d91d5a870b620d20e6c0a1fbea3ff28c9040dbea07d7c2`

The extracted runtime mounted by Docker lives under:

- `backend/.blender-build_release_headless`

Local build metadata inside that extracted tree also matches the same release
family:

- Blender version: `5.0.1`
- architecture: `aarch64`
- package name: `blender-5.0.1-git20251228.a3db93c5b259-aarch64`

Related local artifacts:

- source tree snapshot: `backend/.blender-src`
- extracted runtime: `backend/.blender-build_release_headless`
- source archive / downloaded release asset: `backend/.blender-dist/blender-v4.tar.xz`

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
- `ADMIN_BOOTSTRAP_USERNAME`
- `ADMIN_BOOTSTRAP_PASSWORD`
- `ADMIN_PANEL_PATH`
- `AUTH_COOKIE_SECURE`
- `TRUSTED_PROXIES`

The example port settings live in [.env.example](/home/maxaitel/render-farm/.env.example).

This stack does not support upgrading legacy pre-account databases or importing
old `jobs/*/job.json` payloads. Start from the current schema in
`./data/renderfarm.sqlite3`.

## Frontend Notes

- The frontend uses relative asset URLs so it can still load correctly through
  forwarded ports and path-prefixed proxies.
- The web image copies `.next/static` into the standalone output so the Next.js
  server can serve its built CSS and JS correctly.
- The admin UI is served from a single-segment hidden route and only renders
  when that route matches `ADMIN_PANEL_PATH`.

## Storage Layout

Uploaded projects and renders are stored under:

- `./data/files/<file-id>/source/` for reusable user-owned source trees
- `./data/jobs/<job-id>/outputs/` for render outputs and archives
- `./data/renderfarm.sqlite3` for users, sessions, activity, files, and runs
