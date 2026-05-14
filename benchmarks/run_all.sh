#!/usr/bin/env bash
# Runs benchmark.py three times (baseline PyPI wheel, shim-cpu wheel,
# shim-gpu wheel), then emits a comparison plot. Intended to run inside
# benchmarks/Dockerfile's runtime stage. Writes results/plots to /out.
# CWD must be on a bind-mounted fast disk so the zarr scratch files are
# measured against real storage, not overlayfs/tmpfs.

set -euo pipefail

PY=/opt/bench/bin/python
OUT=${OUT:-/out}
SHA=${BENCH_GIT_SHA:-local}
T_CHUNK=${T_CHUNK:-64}
XY_CHUNK=${XY_CHUNK:-64}
XY_SHARD=${XY_SHARD:-16}
FRAMES=${FRAMES:-1024}
BASELINE_SPEC=${BASELINE_SPEC:-acquire-zarr>=0.5.2}

mkdir -p "$OUT"

resolve_wheel() {
    local pattern=$1
    local hit
    hit=$(find /opt/wheels -maxdepth 1 -name "$pattern" -print -quit)
    if [[ -z "$hit" ]]; then
        echo "No wheel matching /opt/wheels/$pattern" >&2
        exit 1
    fi
    echo "$hit"
}

CPU_WHEEL=$(resolve_wheel 'acquire_zarr_cpu-*.whl')
GPU_WHEEL=$(resolve_wheel 'acquire_zarr_gpu-*.whl')

# Detect GPU once; shim-gpu import will fail without a visible device.
HAS_GPU=0
if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi >/dev/null 2>&1; then
    HAS_GPU=1
fi

run_scenario() {
    local name=$1
    local spec=$2
    local dist=$3
    local out="$OUT/benchmark-${name}-${SHA}.json"

    echo
    echo "============================================================"
    echo "  Scenario: ${name}"
    echo "============================================================"
    uv pip install --python "$PY" --quiet "$spec"
    BENCH_GIT_SHA="$SHA" "$PY" /src/benchmarks/benchmark.py \
        --nocompare \
        --t-chunk-size "$T_CHUNK" \
        --xy-chunk-size "$XY_CHUNK" \
        --xy-shard-size "$XY_SHARD" \
        --frame-count "$FRAMES" \
        --output "$out"
    uv pip uninstall --python "$PY" --quiet "$dist"
    rm -rf acquire_zarr_test.zarr tensorstore_test.zarr
}

run_scenario "baseline" "$BASELINE_SPEC" "acquire-zarr"
run_scenario "shim-cpu" "$CPU_WHEEL" "acquire-zarr-cpu"

if [[ $HAS_GPU -eq 1 ]]; then
    run_scenario "shim-gpu" "$GPU_WHEEL" "acquire-zarr-gpu"
else
    echo
    echo "No GPU visible (nvidia-smi not runnable) — skipping shim-gpu."
    echo "Pass --gpus all (or equivalent) to the container to include it."
fi

echo
echo "============================================================"
echo "  Plot"
echo "============================================================"
"$PY" /src/benchmarks/plot_benchmarks.py \
    --mode backends \
    --input-dir "$OUT" \
    --output-prefix "$OUT/compare"

echo
echo "Done. Results: $OUT"
ls -la "$OUT"
