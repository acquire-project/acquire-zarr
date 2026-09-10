# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "acquire-zarr",
#     "numpy",
#     "matplotlib",
#     "pandas",
# ]
# ///
# !/usr/bin/env python3
import itertools
from dataclasses import dataclass, field
from typing import Optional

import time
import shutil
import tempfile
import acquire_zarr as aqz
from pathlib import Path

import csv
import os
import traceback
from dataclasses import asdict

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np


@dataclass
class BenchmarkParams:
    frame_width: int
    frame_height: int
    frame_count: int
    dtype: str  # "uint16", "uint8", etc.
    chunk_width: int
    chunk_height: int
    chunk_frames: int  # chunk size along time/frame axis
    shard_width_chunks: int  # shard size in chunks, x
    shard_height_chunks: int
    shard_frames_chunks: int
    compressor: str  # "none", "blosc_lz4", "blosc_zstd"
    store_path: str
    max_threads: int = 0  # 0 = hardware concurrency


@dataclass
class BenchmarkResult:
    params: BenchmarkParams
    elapsed_s: float
    bytes_written: int
    error: Optional[str] = None

    @property
    def throughput_gbps(self) -> float:
        if self.elapsed_s <= 0:
            return 0.0
        return (self.bytes_written / 1e9) / self.elapsed_s


def make_param_grid(base_path: str) -> list[BenchmarkParams]:
    """
    Sweep chunk and shard configs at fixed frame count,
    then sweep frame count at a fixed "good" config.
    """
    W, H = 2048, 2048
    DTYPE = "uint16"

    chunk_configs = list(itertools.product(
        [128, 256, 512, 1024],  # chunk_width = chunk_height
        [1, 2, 4],  # chunk_frames
    ))
    shard_configs = [
        (2, 1), (4, 1), (8, 1), (4, 2),
    ]  # (shard_xy_chunks, shard_frame_chunks)

    compressors = ["none", "blosc_lz4", "blosc_zstd"]
    frame_count = 512  # fixed for chunk/shard sweep

    grid = []
    for (cxy, cf), (sxy, sf), comp in itertools.product(
            chunk_configs, shard_configs, compressors
    ):
        grid.append(BenchmarkParams(
            frame_width=W, frame_height=H,
            frame_count=frame_count,
            dtype=DTYPE,
            chunk_width=cxy, chunk_height=cxy,
            chunk_frames=cf,
            shard_width_chunks=sxy, shard_height_chunks=sxy,
            shard_frames_chunks=sf,
            compressor=comp,
            store_path=f"{base_path}/chunk_{cxy}_cf{cf}_s{sxy}x{sf}_{comp}.zarr",
        ))

    # Frame count scaling — fix a reasonable middle config
    for n in [8, 16, 32, 64, 128, 256, 512, 1024]:
        for compressor in compressors:
            grid.append(BenchmarkParams(
                frame_width=W, frame_height=H,
                frame_count=n,
                dtype=DTYPE,
                chunk_width=256, chunk_height=256, chunk_frames=1,
                shard_width_chunks=4, shard_height_chunks=4, shard_frames_chunks=1,
                compressor=compressor,
                store_path=f"{base_path}/scaling_n{n}.zarr",
            ))

    return grid


DTYPE_MAP = {
    "uint8": (np.uint8, aqz.DataType.UINT8),
    "uint16": (np.uint16, aqz.DataType.UINT16),
    "float32": (np.float32, aqz.DataType.FLOAT32),
}

COMPRESSOR_MAP = {
    "none": None,
    "blosc_lz4": aqz.CompressionSettings(
        compressor=aqz.Compressor.BLOSC1,
        codec=aqz.CompressionCodec.BLOSC_LZ4,
        level=1, shuffle=1),
    "blosc_zstd": aqz.CompressionSettings(
        compressor=aqz.Compressor.BLOSC1,
        codec=aqz.CompressionCodec.BLOSC_ZSTD,
        level=1, shuffle=1),
}


def run_benchmark(params: BenchmarkParams) -> BenchmarkResult:
    np_dtype, az_dtype = DTYPE_MAP[params.dtype]
    bytes_per_frame = params.frame_width * params.frame_height * np.dtype(np_dtype).itemsize
    total_bytes = bytes_per_frame * params.frame_count

    # Pre-allocate one frame of random data — reuse it for every append.
    # This avoids measuring allocation overhead, and random data is
    # worst-case for compression (realistic for shot-noise-limited images).
    frame = np.random.randint(0, np.iinfo(np.uint16).max,
                              size=(params.frame_height, params.frame_width),
                              dtype=np_dtype)

    store = Path(params.store_path)
    if store.exists():
        shutil.rmtree(store)

    settings = aqz.StreamSettings(
        store_path=str(store),
        overwrite=True,
        max_threads=params.max_threads,
        arrays=[aqz.ArraySettings(
            dimensions=[
                aqz.Dimension(
                    name="t",
                    kind=aqz.DimensionType.TIME,
                    array_size_px=0,  # 0 = unlimited / streaming
                    chunk_size_px=params.chunk_frames,
                    shard_size_chunks=params.shard_frames_chunks,
                ),
                aqz.Dimension(
                    name="y",
                    kind=aqz.DimensionType.SPACE,
                    array_size_px=params.frame_height,
                    chunk_size_px=params.chunk_height,
                    shard_size_chunks=params.shard_height_chunks,
                ),
                aqz.Dimension(
                    name="x",
                    kind=aqz.DimensionType.SPACE,
                    array_size_px=params.frame_width,
                    chunk_size_px=params.chunk_width,
                    shard_size_chunks=params.shard_width_chunks,
                ),
            ],
            data_type=az_dtype,
            compression=COMPRESSOR_MAP[params.compressor],
        )],
    )

    try:
        stream = aqz.ZarrStream(settings)
        t0 = time.perf_counter()
        for _ in range(params.frame_count):
            stream.append(frame)
        stream.close()
        elapsed = time.perf_counter() - t0

        return BenchmarkResult(params=params, elapsed_s=elapsed, bytes_written=total_bytes)

    except Exception as e:
        return BenchmarkResult(params=params, elapsed_s=0, bytes_written=0, error=str(e))


def run_all(grid: list[BenchmarkParams],
            output_csv: str = "benchmark_results.csv",
            base_path: str = "/var/tmp/zarr_bench") -> list[BenchmarkResult]:
    os.makedirs(base_path, exist_ok=True)
    results = []

    # flatten nested params dict into top-level columns
    fieldnames = (
            [f"params.{k}" for k in asdict(grid[0]).keys()]
            + ["elapsed_s", "bytes_written", "throughput_gbps", "error"]
    )

    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i, params in enumerate(grid):
            print(f"[{i + 1}/{len(grid)}] "
                  f"chunk={params.chunk_width}px cf={params.chunk_frames} "
                  f"shard={params.shard_width_chunks}x{params.shard_frames_chunks} "
                  f"comp={params.compressor} n={params.frame_count} ... ",
                  end="", flush=True)

            try:
                result = run_benchmark(params)
            except Exception as e:
                traceback.print_exc()
                result = BenchmarkResult(params=params, elapsed_s=0,
                                         bytes_written=0, error=str(e))

            results.append(result)

            if result.error:
                print(f"ERROR: {result.error}")
            else:
                print(f"{result.throughput_gbps:.3f} GB/s")

            # write row immediately so a crash mid-run doesn't lose data
            row = {f"params.{k}": v for k, v in asdict(params).items()}
            row["elapsed_s"] = result.elapsed_s
            row["bytes_written"] = result.bytes_written
            row["throughput_gbps"] = result.throughput_gbps
            row["error"] = result.error or ""
            writer.writerow(row)
            f.flush()

            # clean up store so disk doesn't fill up
            store = Path(params.store_path)
            if store.exists():
                shutil.rmtree(store)

    print(f"\nDone. Results written to {output_csv}")
    return results


def load_results(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df = df[df["error"].isna() | (df["error"] == "")]  # drop failed runs
    return df


def plot_chunk_shard_heatmap(df: pd.DataFrame):
    """
    One heatmap per compressor: chunk_width (y) vs shard_width_chunks (x),
    cell value = mean throughput in GB/s across all matching runs.
    """
    compressors = df["params.compressor"].unique()
    fig, axes = plt.subplots(1, len(compressors),
                             figsize=(5 * len(compressors), 4),
                             sharey=True)
    if len(compressors) == 1:
        axes = [axes]

    # shared color scale across all subplots
    vmin = df["throughput_gbps"].min()
    vmax = df["throughput_gbps"].max()

    for ax, comp in zip(axes, compressors):
        sub = df[df["params.compressor"] == comp]
        pivot = sub.pivot_table(
            index="params.chunk_width",
            columns="params.shard_width_chunks",
            values="throughput_gbps",
            aggfunc="mean",
        )
        im = ax.imshow(pivot.values, aspect="auto", origin="lower",
                       vmin=vmin, vmax=vmax, cmap="viridis")
        ax.set_title(comp)
        ax.set_xticks(range(len(pivot.columns)))
        ax.set_xticklabels(pivot.columns)
        ax.set_yticks(range(len(pivot.index)))
        ax.set_yticklabels(pivot.index)
        ax.set_xlabel("shard width (chunks)")
        if ax == axes[0]:
            ax.set_ylabel("chunk width (px)")

        # annotate cells
        for r in range(pivot.values.shape[0]):
            for c in range(pivot.values.shape[1]):
                val = pivot.values[r, c]
                if not np.isnan(val):
                    ax.text(c, r, f"{val:.2f}", ha="center", va="center",
                            fontsize=7, color="white" if val < (vmin + vmax) / 2 else "black")

    fig.colorbar(im, ax=axes[-1], label="throughput (GB/s)")
    fig.suptitle("Chunk/Shard Sweep — Mean Throughput by Compressor")
    plt.tight_layout()
    plt.savefig("heatmap_chunk_shard.png", dpi=150)
    plt.show()


def plot_scaling(df: pd.DataFrame):
    """
    Throughput vs frame count, one line per compressor.
    Uses only the scaling runs (fixed chunk/shard config).
    """
    # scaling runs are identified by fixed chunk config from make_param_grid
    scaling = df[
        (df["params.chunk_width"] == 256) &
        (df["params.shard_width_chunks"] == 4)
        ].copy()

    fig, ax = plt.subplots(figsize=(7, 4))
    for comp, group in scaling.groupby("params.compressor"):
        group = group.sort_values("params.frame_count")
        ax.plot(group["params.frame_count"], group["throughput_gbps"],
                marker="o", label=comp)

    ax.set_xlabel("frame count")
    ax.set_ylabel("throughput (GB/s)")
    ax.set_title("Throughput vs Frame Count (2048×2048 uint16)")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig("scaling_frame_count.png", dpi=150)
    plt.show()


def plot_all(csv_path: str):
    df = load_results(csv_path)
    plot_chunk_shard_heatmap(df)
    plot_scaling(df)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="acquire-zarr streaming benchmark")
    parser.add_argument("--base-path", default="/var/tmp/zarr_bench",
                        help="Directory for temporary Zarr stores")
    parser.add_argument("--output-csv", default="benchmark_results.csv",
                        help="Path to write results CSV")
    parser.add_argument("--no-viz", action="store_true",
                        help="Skip visualization after benchmarking")
    args = parser.parse_args()

    aqz.set_log_level(aqz.LogLevel.NONE)

    grid = make_param_grid(base_path=args.base_path)

    print(f"Running {len(grid)} benchmarks...\n")
    run_all(grid, output_csv=args.output_csv, base_path=args.base_path)

    if not args.no_viz:
        plot_all(args.output_csv)
