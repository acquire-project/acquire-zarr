#!/usr/bin/env bash
set -euo pipefail

if sccache --zero-stats; then
    exit 0
fi

echo "::warning::Compiler cache startup failed; use the local cache."
sccache --stop-server || true
export SCCACHE_GHA_ENABLED=false
printf '%s\n' 'SCCACHE_GHA_ENABLED=false' >> "${GITHUB_ENV:?}"
sccache --zero-stats
