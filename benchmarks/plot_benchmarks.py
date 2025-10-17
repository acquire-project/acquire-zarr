import json
import click
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import re


@click.command()
@click.option("--input-dir", "-i", default=".", type=click.Path(exists=True),
              help="Directory to search for benchmark files")
@click.option("--output", "-o", default="benchmark_comparison.png",
              help="Output plot filename")
def plot_benchmarks(input_dir, output):
    """Plot throughput comparison across platforms from benchmark JSON files."""

    # Find all files matching pattern: benchmark-{platform}-{sha}.json
    pattern = re.compile(r"benchmark-(.+)-([a-f0-9]+)\.json")

    data = {}
    input_path = Path(input_dir)

    for filepath in input_path.glob("benchmark-*.json"):
        match = pattern.match(filepath.name)
        if not match:
            continue

        platform = match.group(1)

        with open(filepath) as f:
            result = json.load(f)

        data[platform] = {
            "acquire_zarr": result["acquire_zarr"]["throughput_gib_per_s"],
            "tensorstore": result["tensorstore"]["throughput_gib_per_s"],
            "git_commit_hash": result["git_commit_hash"],
            "test_parameters": result["test_parameters"]
        }

    if not data:
        print(f"No benchmark files found in {input_dir}")
        return

    platforms = sorted(data.keys())

    hashes = {data[p]["git_commit_hash"] for p in platforms}
    assert len(hashes) == 1, "All benchmarks must be from the same git commit hash"
    commit_hash = hashes.pop()

    t_chunk_sizes = {data[p]["test_parameters"]["t_chunk_size"] for p in platforms}
    assert len(t_chunk_sizes) == 1, "All benchmarks must use the same t_chunk_size"
    t_chunk_size = t_chunk_sizes.pop()

    xy_chunk_sizes = {data[p]["test_parameters"]["xy_chunk_size"] for p in platforms}
    assert len(xy_chunk_sizes) == 1, "All benchmarks must use the same xy_chunk_size"
    xy_chunk_size = xy_chunk_sizes.pop()

    xy_shard_sizes = {data[p]["test_parameters"]["xy_shard_size"] for p in platforms}
    assert len(xy_shard_sizes) == 1, "All benchmarks must use the same xy_shard_size"
    xy_shard_size = xy_shard_sizes.pop()

    frame_counts = {data[p]["test_parameters"]["frame_count"] for p in platforms}
    assert len(frame_counts) == 1, "All benchmarks must use the same frame_count"
    frame_count = frame_counts.pop()

    str_params = (f"t_chunk_size={t_chunk_size}, "
                  f"xy_chunk_size={xy_chunk_size}, "
                  f"xy_shard_size={xy_shard_size}, "
                  f"frame_count={frame_count}")
    print(f"Benchmark parameters: {str_params}")

    az_throughput = [data[p]["acquire_zarr"] for p in platforms]
    ts_throughput = [data[p]["tensorstore"] for p in platforms]

    x = np.arange(len(platforms))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x - width / 2, az_throughput, width, label="acquire-zarr")
    ax.bar(x + width / 2, ts_throughput, width, label="tensorstore")

    ax.set_ylabel("Throughput (GiB/s)")
    ax.set_title(f"Benchmark Throughput by Platform (Commit: {commit_hash[:7]})\n{str_params}")
    ax.set_xticks(x)
    ax.set_xticklabels(platforms, rotation=45, ha="right")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output, dpi=150)
    print(f"Plot saved to {output}")


if __name__ == "__main__":
    plot_benchmarks()
