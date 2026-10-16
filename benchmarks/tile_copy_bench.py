#!/usr/bin/env python3
"""Serial vs fixed fan-out tile-scatter: per-frame append() latency + throughput.

acquire-zarr scatters each incoming frame into its chunk buffers on the frame-
processing thread (Array::write_frame_to_chunks_). That scatter used to be a
`#pragma omp parallel for`; it is now `zarr::parallel_for_reduce`, a fixed small
worker team (default 4) with a serial shortcut for small frames and NO OpenMP
dependency. This script measures what the fan-out buys over a plain serial copy,
by timing append() across frame sizes.

Because the choice is compile-time (no runtime/env knob is shipped), the A/B is
TWO builds of the extension, compared by running this script against each:

    # fan-out build (default)
    uv pip install -e . --no-build-isolation
    python benchmarks/tile_copy_bench.py --label fanout  --json fanout.json

    # serial build (benchmark-only CMake flag)
    CMAKE_ARGS="-DACQUIRE_ZARR_SERIAL_TILE_COPY=ON" \
        uv pip install -e . --no-build-isolation
    python benchmarks/tile_copy_bench.py --label serial  --json serial.json

Point --store at your FASTEST disk (or a tmpfs / RAM disk) so the scatter, not
I/O, dominates the per-frame latency -- that is the quantity the fan-out moves.
Throughput is reported too, but on real storage it is write-bound and the two
builds should match; the per-frame p50/p99 is where any difference shows up.

Needs only acquire_zarr + numpy. Works on Linux/macOS/Windows.
"""
import argparse, json, os, shutil, sys, tempfile, time
import numpy as np
import acquire_zarr as aqz


def run_one(xy, frames, store):
    """Append `frames` planes of xy*xy uint16 at the 64/64/16 baseline geometry;
    return latency percentiles (ms) and throughput (GiB/s)."""
    shutil.rmtree(store, ignore_errors=True)
    nsrc = max(8, min(64, (2 << 30) // (xy * xy * 2)))
    src = np.random.randint(0, 2**16 - 1, (nsrc, xy, xy), dtype=np.uint16)

    settings = aqz.StreamSettings(
        store_path=store,
        arrays=[aqz.ArraySettings(
            dimensions=[
                aqz.Dimension(name="t", kind=aqz.DimensionType.TIME,
                              array_size_px=0, chunk_size_px=64, shard_size_chunks=1),
                aqz.Dimension(name="y", kind=aqz.DimensionType.SPACE,
                              array_size_px=xy, chunk_size_px=64, shard_size_chunks=16),
                aqz.Dimension(name="x", kind=aqz.DimensionType.SPACE,
                              array_size_px=xy, chunk_size_px=64, shard_size_chunks=16),
            ],
            data_type=aqz.DataType.UINT16)],
    )
    stream = aqz.ZarrStream(settings)
    lat = np.empty(frames, dtype=np.float64)
    t0 = time.perf_counter()
    for i in range(frames):
        s = time.perf_counter()
        stream.append(src[i % nsrc])
        lat[i] = (time.perf_counter() - s) * 1e3  # ms
    del stream
    total_s = time.perf_counter() - t0
    gib = frames * xy * xy * 2 / (1 << 30)
    warm = lat[min(8, frames // 8):]              # drop warm-up from percentiles
    shutil.rmtree(store, ignore_errors=True)
    return {
        "xy": xy, "frames": frames,
        "throughput_gib_s": gib / total_s,
        "p50": float(np.percentile(warm, 50)),
        "p90": float(np.percentile(warm, 90)),
        "p99": float(np.percentile(warm, 99)),
        "max": float(lat.max()),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--xy", type=int, nargs="+", default=[2048, 4096])
    ap.add_argument("--frames", type=int, default=512)
    ap.add_argument("--repeats", type=int, default=3,
                    help="repeat each size; report the median percentile row")
    ap.add_argument("--label", default="build",
                    help="tag for this build (e.g. fanout / serial)")
    ap.add_argument("--store", default=os.path.join(tempfile.gettempdir(), "aqz_tilecopy_bench"))
    ap.add_argument("--json", default=None, help="write per-size results here")
    a = ap.parse_args()

    ncpu = os.cpu_count() or 1
    print(f"# acquire-zarr tile-scatter bench | label={a.label} | {sys.platform} | "
          f"{ncpu} CPUs | frames={a.frames} x {a.repeats} | store={a.store}")
    print(f"{'label':>8} {'xy':>6} {'frames':>7} {'p50 ms':>9} {'p90 ms':>9} "
          f"{'p99 ms':>9} {'max ms':>10} {'GiB/s':>7}")

    rows = []
    for xy in a.xy:
        reps = [run_one(xy, a.frames, a.store) for _ in range(a.repeats)]
        # report the run with the median p50 (robust to a single noisy run)
        reps.sort(key=lambda r: r["p50"])
        r = reps[len(reps) // 2]
        r["label"] = a.label
        rows.append(r)
        print(f"{a.label:>8} {xy:>6} {a.frames:>7} {r['p50']:>9.3f} {r['p90']:>9.3f} "
              f"{r['p99']:>9.3f} {r['max']:>10.3f} {r['throughput_gib_s']:>7.2f}")

    if a.json:
        with open(a.json, "w") as f:
            json.dump({"label": a.label, "platform": sys.platform, "ncpu": ncpu,
                       "frames": a.frames, "repeats": a.repeats, "rows": rows}, f, indent=2)
        print(f"# wrote {a.json}")


if __name__ == "__main__":
    main()
