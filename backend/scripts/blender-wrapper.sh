#!/usr/bin/env bash
set -euo pipefail

BLENDER_ROOT="${BLENDER_ROOT:-/blender/build_linux}"
export LD_LIBRARY_PATH="${BLENDER_ROOT}/libExt${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

exec "${BLENDER_ROOT}/bin/blender" "$@"
