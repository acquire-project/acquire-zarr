#!/usr/bin/env python3
"""Fresh pages faulted in per byte streamed, at a steady frame rate.

Each chunk used to get a freshly allocated, zero-filled buffer, and each
compression a fresh output buffer, both freed after the shard write. With chunks
above glibc's mmap threshold, as here, a stream therefore faulted in about twice
its data rate in new pages, each one handled and zeroed by the kernel. Chunk
buffers and compression outputs now come from bounded per-array pools, so after
the first layers the steady state should fault in almost nothing.

This streams camera-sized uint16 frames (blosc zstd level 2, one array) at a
fixed rate and, after a warmup that excludes filling the pools, reports minor
page faults and the process's user and system CPU time. The CPU time covers
every thread of the process, so it includes compression and writing.

To compare two builds, install one, run the script, install the other and run
it again with the same arguments; for example, at each of two chunk depths:

    python benchmarks/chunk_buffer_faults.py /tmp/faults --chunk 16
    python benchmarks/chunk_buffer_faults.py /tmp/faults --chunk 64

The store is written in a new temporary directory under OUTDIR, and only that
directory is deleted afterwards. Put OUTDIR on the storage you want to measure;
results depend on it. The first line of output records the host, OUTDIR's
filesystem, the acquire-zarr version (and this checkout's commit) and any
ZARR_* environment variables, so runs can be compared.
"""

import argparse
import importlib.metadata
import os
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import numpy as np

import acquire_zarr as az

HEIGHT, WIDTH = 1260, 2036
PAGE_BYTES = resource.getpagesize()


def filesystem_of(path: Path) -> str:
    """The filesystem type holding path, from /proc/self/mounts on Linux."""
    try:
        mounts = Path("/proc/self/mounts").read_text().splitlines()
    except OSError:
        return "unknown"
    resolved = str(path.resolve())
    best, fstype = "", "unknown"
    for line in mounts:
        fields = line.split()
        if len(fields) < 3:
            continue
        mount_point = fields[1].replace("\\040", " ")
        inside = resolved == mount_point or resolved.startswith(
            mount_point.rstrip("/") + "/"
        )
        if inside and len(mount_point) > len(best):
            best, fstype = mount_point, fields[2]
    return fstype


def checkout_commit() -> str:
    """This script's checkout commit, if it runs from a git checkout."""
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def context(out: Path) -> str:
    zarr_env = " ".join(
        f"{k}={v}"
        for k, v in sorted(os.environ.items())
        if k.startswith("ZARR_")
    )
    return (
        f"host {platform.node()} ({platform.system()}) | "
        f"OUTDIR {out} ({filesystem_of(out)}) | "
        f"acquire-zarr {importlib.metadata.version('acquire-zarr')}, "
        f"checkout {checkout_commit()} | "
        f"env {zarr_env or '(no ZARR_* set)'}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "out", type=Path, help="directory to create the scratch store under"
    )
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

    args.out.mkdir(parents=True, exist_ok=True)
    print(context(args.out))
    scratch = Path(
        tempfile.mkdtemp(prefix="chunk_buffer_faults_", dir=args.out)
    )
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
            store_path=str(scratch / "faults.zarr"),
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
    usage0 = t0 = None
    t_next = time.perf_counter()
    for k in range(args.frames):
        if k == args.warmup:
            usage0 = resource.getrusage(resource.RUSAGE_SELF)
            t0 = time.perf_counter()
        # sleep rather than spin, so the pacing adds no CPU time of its own
        delay = t_next - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        t_next += period
        start = time.perf_counter()
        stream.append(frames[k % len(frames)])
        latency[k] = time.perf_counter() - start

    wall = time.perf_counter() - t0
    usage1 = resource.getrusage(resource.RUSAGE_SELF)
    faults = usage1.ru_minflt - usage0.ru_minflt
    user_s = usage1.ru_utime - usage0.ru_utime
    sys_s = usage1.ru_stime - usage0.ru_stime
    stream.close()
    shutil.rmtree(scratch, ignore_errors=True)

    counted = args.frames - args.warmup
    data_bytes = counted * HEIGHT * WIDTH * 2
    ms = latency[args.warmup :] * 1e3
    # ru_maxrss is in bytes on macOS and in KiB elsewhere
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_rss_gib = maxrss / (2**30 if sys.platform == "darwin" else 2**20)
    print(
        f"chunk {args.chunk} frames: {counted / wall:.1f} fps, "
        f"{data_bytes / wall / 1e6:.0f} MB/s | "
        f"faults {faults / wall / 1e3:.1f}k/s, "
        f"{faults * PAGE_BYTES / data_bytes:.3f}x the data | "
        f"CPU user {user_s:.2f} s, sys {sys_s:.2f} s in {wall:.1f} s | "
        f"append mean {ms.mean():.2f} p99 {np.percentile(ms, 99):.2f} "
        f"max {ms.max():.2f} ms | peak RSS {peak_rss_gib:.2f} GiB"
    )


if __name__ == "__main__":
    main()
