#!/usr/bin/env python3
"""Fresh pages faulted in per byte streamed, at a steady frame rate.

Each chunk used to get a freshly allocated, zero-filled buffer, and each
compression a fresh output buffer, both freed after the shard write. A stream
therefore faulted in about twice its data rate in new pages, and under load the
writer threads queued on the kernel's page allocator. Chunk buffers and
compression outputs now come from bounded per-array pools, so after the first
layers the steady state should fault in almost nothing.

This streams camera-sized uint16 frames (blosc zstd level 2, one array) at a
fixed rate and counts minor page faults after a warmup, which excludes filling
the pools. Compare two builds by running it against each:

    python benchmarks/chunk_buffer_faults.py /tmp/faults --chunk 16
    python benchmarks/chunk_buffer_faults.py /tmp/faults --chunk 64

The store is written under OUTDIR and deleted afterwards. Use a local disk, so
the sink is not the bottleneck.
"""

import argparse
import resource
import shutil
import time
from pathlib import Path

import numpy as np

import acquire_zarr as az

HEIGHT, WIDTH = 1260, 2036
PAGE_BYTES = resource.getpagesize()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path, help="scratch directory")
    parser.add_argument("--frames", type=int, default=3072)
    parser.add_argument(
        "--warmup",
        type=int,
        default=256,
        help="frames appended before counting faults",
    )
    parser.add_argument("--fps", type=float, default=150.0)
    parser.add_argument(
        "--chunk", type=int, default=64, help="frames per chunk"
    )
    args = parser.parse_args()
    if not 0 <= args.warmup < args.frames:
        parser.error("--warmup must be in [0, --frames)")

    rng = np.random.default_rng(0)
    frames = [
        rng.poisson(210, (HEIGHT, WIDTH)).astype(np.uint16) for _ in range(32)
    ]

    shutil.rmtree(args.out, ignore_errors=True)
    args.out.mkdir(parents=True)
    dimensions = [
        az.Dimension(
            name="t",
            kind=az.DimensionType.TIME,
            array_size_px=0,
            chunk_size_px=args.chunk,
            shard_size_chunks=16,
        ),
        az.Dimension(
            name="y",
            kind=az.DimensionType.SPACE,
            array_size_px=HEIGHT,
            chunk_size_px=HEIGHT,
            shard_size_chunks=1,
        ),
        az.Dimension(
            name="x",
            kind=az.DimensionType.SPACE,
            array_size_px=WIDTH,
            chunk_size_px=WIDTH,
            shard_size_chunks=1,
        ),
    ]
    compression = az.CompressionSettings(
        compressor=az.Compressor.BLOSC1,
        codec=az.CompressionCodec.BLOSC_ZSTD,
        level=2,
        shuffle=1,
    )
    stream = az.ZarrStream(
        az.StreamSettings(
            store_path=str(args.out / "faults.zarr"),
            arrays=[
                az.ArraySettings(
                    dimensions=dimensions,
                    data_type=np.uint16,
                    compression=compression,
                )
            ],
            overwrite=True,
        )
    )

    period = 1.0 / args.fps
    latency = np.empty(args.frames)
    faults0 = t0 = None
    t_next = time.perf_counter()
    for k in range(args.frames):
        if k == args.warmup:
            faults0 = resource.getrusage(resource.RUSAGE_SELF).ru_minflt
            t0 = time.perf_counter()
        while time.perf_counter() < t_next:
            pass
        t_next += period
        start = time.perf_counter()
        stream.append(frames[k % len(frames)])
        latency[k] = time.perf_counter() - start

    wall = time.perf_counter() - t0
    faults = resource.getrusage(resource.RUSAGE_SELF).ru_minflt - faults0
    stream.close()
    shutil.rmtree(args.out, ignore_errors=True)

    counted = args.frames - args.warmup
    data_bytes = counted * HEIGHT * WIDTH * 2
    ms = latency[args.warmup :] * 1e3
    peak_rss_gb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6
    print(
        f"chunk {args.chunk} frames: {counted / wall:.1f} fps, "
        f"{data_bytes / wall / 1e6:.0f} MB/s | "
        f"faults {faults / wall / 1e3:.1f}k/s, "
        f"{faults * PAGE_BYTES / data_bytes:.3f}x the data | "
        f"append mean {ms.mean():.2f} p99 {np.percentile(ms, 99):.2f} "
        f"max {ms.max():.2f} ms | peak RSS {peak_rss_gb:.2f} GB"
    )


if __name__ == "__main__":
    main()
