# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "acquire-zarr>=0.5.2",
#     "zarr",
#     "rich",
#     "tensorstore",
#     "click",
#     "psutil",
# ]
# ///
# !/usr/bin/env python3
"""Compare write performance of TensorStore vs. acquire-zarr for a Zarr v3 store. Thanks to Talley Lambert @tlambert03
for the original version of this script: https://gist.github.com/tlambert03/f8c1b069c2947b411ce24ea05aa370b1
"""

import json
import os
from pathlib import Path
import platform
import psutil
import shutil
import subprocess
import time
from typing import Tuple

import acquire_zarr as aqz
import click
import numpy as np
import tensorstore
import zarr
from rich import print


class CyclicArray:
    def __init__(self, data: np.ndarray, n_frames: int):
        self.data = data
        self.t = self.data.shape[0]  # Size of first dimension
        self.shape = (n_frames,) + self.data.shape[1:]

    def __getitem__(self, idx):
        if isinstance(idx, tuple) and len(idx) > 0:
            if isinstance(idx[0], int):
                return self.data[(idx[0] % self.t,) + idx[1:]]
        elif isinstance(idx, int):
            return self.data[idx % self.t]

        return self.data[idx]

    def compare_array(self, arr: np.ndarray) -> None:
        """Compare an array with a CyclicArray."""
        assert self.shape == arr.shape

        for i in range(0, arr.shape[0], self.t):
            start = i
            stop = min(i + self.t, arr.shape[0])
            # print(f"Comparing 0:{stop - start} to {start}:{stop}")
            np.testing.assert_array_equal(
                self.data[0 : (stop - start)], arr[start:stop]
            )


def run_tensorstore_test(
    data: CyclicArray, path: str, metadata: dict
) -> Tuple[float, np.ndarray]:
    """Write data using TensorStore and print per-plane and total write times."""
    # Define a TensorStore spec for a Zarr v3 store.
    spec = {
        "driver": "zarr3",
        "kvstore": {"driver": "file", "path": path},
        "metadata": metadata,
        "delete_existing": True,
        "create": True,
    }
    # Open (or create) the store.
    ts = tensorstore.open(spec).result()
    print(ts)
    total_start = time.perf_counter_ns()
    futures = []
    elapsed_times = []

    # cache data until we've reached a write-chunk-aligned block
    chunk_length = ts.schema.chunk_layout.write_chunk.shape[0]
    write_chunk_shape = (chunk_length, *ts.domain.shape[1:])
    chunk = np.empty(write_chunk_shape, dtype=np.uint16)
    for i in range(data.shape[0]):
        start_plane = time.perf_counter_ns()
        chunk_idx = i % chunk_length
        chunk[chunk_idx] = data[i]
        if chunk_idx == chunk_length - 1:
            slc = slice(i - chunk_length + 1, i + 1)
            futures.append(ts[slc].write(chunk))
            chunk = np.empty(write_chunk_shape, dtype=np.uint16)
        elapsed = time.perf_counter_ns() - start_plane
        elapsed_times.append(elapsed)
        print(f"TensorStore: Plane {i} written in {elapsed / 1e6:.3f} ms")

    start_futures = time.perf_counter_ns()
    # Wait for all writes to finish.
    for future in futures:
        future.result()
    elapsed = time.perf_counter_ns() - start_futures
    elapsed_times.append(elapsed)
    print(f"TensorStore: Final futures took {elapsed / 1e6:.3f} ms")

    total_elapsed = time.perf_counter_ns() - total_start
    tot_ms = total_elapsed / 1e6
    print(f"TensorStore: Total write time: {tot_ms:.3f} ms")

    return tot_ms, np.array(elapsed_times) / 1e6


def run_acquire_zarr_test(
    data: CyclicArray,
    path: str,
    tchunk_size: int = 1,
    xy_chunk_size: int = 2048,
    xy_shard_size: int = 1,
) -> Tuple[float, np.ndarray]:
    """Write data using acquire-zarr and print per-plane and total write times."""
    settings = aqz.StreamSettings(
        store_path=path,
        arrays=[
            aqz.ArraySettings(
                dimensions=[
                    aqz.Dimension(
                        name="t",
                        kind=aqz.DimensionType.TIME,
                        array_size_px=0,
                        chunk_size_px=tchunk_size,
                        shard_size_chunks=1,
                    ),
                    aqz.Dimension(
                        name="y",
                        kind=aqz.DimensionType.SPACE,
                        array_size_px=2048,
                        chunk_size_px=xy_chunk_size,
                        shard_size_chunks=xy_shard_size,
                    ),
                    aqz.Dimension(
                        name="x",
                        kind=aqz.DimensionType.SPACE,
                        array_size_px=2048,
                        chunk_size_px=xy_chunk_size,
                        shard_size_chunks=xy_shard_size,
                    ),
                ],
                data_type=aqz.DataType.UINT16,
            )
        ],
    )

    # Create a ZarrStream for appending frames.
    stream = aqz.ZarrStream(settings)

    elapsed_times = []

    total_start = time.perf_counter_ns()
    for i in range(data.shape[0]):
        start_plane = time.perf_counter_ns()
        stream.append(data[i])
        elapsed = time.perf_counter_ns() - start_plane
        elapsed_times.append(elapsed)
        print(f"Acquire-zarr: Plane {i} written in {elapsed / 1e6:.3f} ms")

    # Close (or flush) the stream to finalize writes.
    del stream
    total_elapsed = time.perf_counter_ns() - total_start
    tot_ms = total_elapsed / 1e6
    print(f"Acquire-zarr: Total write time: {tot_ms:.3f} ms")

    return tot_ms, np.array(elapsed_times) / 1e6


def get_git_commit_hash():
    """Get the current git commit hash, or None if not in a git repo."""
    # cache the current working directory
    cwd = os.getcwd()

    # cd to the script directory
    os.chdir(Path(__file__).parent)

    hash_out = None

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        hash_out = result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass
    finally:
        # return to the original working directory
        os.chdir(cwd)

    return hash_out


def get_system_info() -> dict:
    """Collect system information for benchmark context."""
    info = {
        "platform": platform.system(),
        "platform_release": platform.release(),
        "platform_version": platform.version(),
        "architecture": platform.machine(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
        "cpu_count_physical": psutil.cpu_count(logical=False),
        "cpu_count_logical": psutil.cpu_count(logical=True),
        "memory_total_gb": round(psutil.virtual_memory().total / (1024**3), 2),
    }

    # try to get CPU brand on different platforms
    try:
        if platform.system() == "Darwin":  # macOS
            result = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                capture_output=True,
                text=True,
                check=True,
            )
            info["cpu_brand"] = result.stdout.strip()
        elif platform.system() == "Linux":
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if "model name" in line:
                        info["cpu_brand"] = line.split(":")[1].strip()
                        break
        elif platform.system() == "Windows":
            result = subprocess.run(
                ["wmic", "cpu", "get", "name"],
                capture_output=True,
                text=True,
                check=True,
            )
            info["cpu_brand"] = result.stdout.split("\n")[1].strip()
    except Exception:
        info["cpu_brand"] = "Unknown"

    return info


def compare(
    t_chunk_size: int,
    xy_chunk_size: int,
    xy_shard_size: int,
    frame_count: int,
    do_compare: bool = True,
) -> dict:
    print("tchunk_size:", t_chunk_size)
    print("xy_chunk_size:", xy_chunk_size)
    print("xy_shard_size:", xy_shard_size)
    print("frame_count:", frame_count)
    print("compare_data:", do_compare)

    print("\nRunning acquire-zarr test:")
    az_path = "acquire_zarr_test.zarr"
    print("I'm saving to ", Path(az_path).absolute())

    # Pre-generate the data (timing excluded)
    data = CyclicArray(
        np.random.randint(0, 2**16 - 1, (128, 2048, 2048), dtype=np.uint16),
        frame_count,
    )

    time_az_ms, frame_write_times_az = run_acquire_zarr_test(
        data, az_path, t_chunk_size, xy_chunk_size, xy_shard_size
    )

    # use the exact same metadata that was used for the acquire-zarr test
    # to ensure we're using the same chunks and codecs, etc...
    az = zarr.open(az_path)["0"]

    print("\nRunning TensorStore test:")
    ts_path = "tensorstore_test.zarr"
    time_ts_ms, frame_write_times_ts = run_tensorstore_test(
        data,
        ts_path,
        {**az.metadata.to_dict(), "data_type": "uint16"},
    )

    # Data comparison (optional)
    comparison_result = None
    if do_compare:
        print("\nComparing the written data:", end=" ")
        try:
            ts = zarr.open(ts_path)
            data.compare_array(
                az
            )  # ensure acquire-zarr wrote the correct data
            data.compare_array(ts)  # ensure tensorstore wrote the correct data
            print("[OK]\n")

            metadata_match = ts.metadata == az.metadata
            print(f"Metadata matches: {metadata_match}")
            if metadata_match:
                print(ts.metadata)

            comparison_result = {
                "data_match": True,
                "metadata_match": metadata_match,
            }
        except Exception as e:
            print(f"[ERROR] Comparison failed: {e}")
            comparison_result = {
                "data_match": False,
                "metadata_match": False,
                "error": str(e),
            }
    else:
        print("\nSkipping data comparison")

    # clean up test data
    del az

    try:
        print("\nCleaning up test data...", end="")
        shutil.rmtree(az_path)
        shutil.rmtree(ts_path)
        print("[OK]")
    except Exception:
        print("[ERROR] Failed to remove test data")

    data_size_gib = (2048 * 2048 * 2 * frame_count) / (1 << 30)

    # Calculate statistics
    az_stats = {
        "total_time_ms": time_az_ms,
        "throughput_gib_per_s": 1000 * data_size_gib / time_az_ms,
        "frame_write_time_50th_percentile_ms": float(
            np.percentile(frame_write_times_az, 50)
        ),
        "frame_write_time_99th_percentile_ms": float(
            np.percentile(frame_write_times_az, 99)
        ),
    }

    ts_stats = {
        "total_time_ms": time_ts_ms,
        "throughput_gib_per_s": 1000 * data_size_gib / time_ts_ms,
        "frame_write_time_50th_percentile_ms": float(
            np.percentile(frame_write_times_ts, 50)
        ),
        "frame_write_time_99th_percentile_ms": float(
            np.percentile(frame_write_times_ts, 99)
        ),
    }

    print("\nPerformance comparison:")
    print(
        f"  acquire-zarr: {az_stats['total_time_ms']:.3f} ms, {az_stats['throughput_gib_per_s']:.3f} GiB/s, "
        f"50th percentile frame write time: {az_stats['frame_write_time_50th_percentile_ms']:.3f} ms, "
        f"99th percentile: {az_stats['frame_write_time_99th_percentile_ms']:.3f} ms"
    )
    print(
        f"  TensorStore: {ts_stats['total_time_ms']:.3f} ms, {ts_stats['throughput_gib_per_s']:.3f} GiB/s, "
        f"50th percentile frame write time: {ts_stats['frame_write_time_50th_percentile_ms']:.3f} ms, "
        f"99th percentile: {ts_stats['frame_write_time_99th_percentile_ms']:.3f} ms"
    )
    print(f"  TS/AZ Ratio: {time_ts_ms / time_az_ms:.3f}")

    # Structure results for JSON output
    results = {
        "test_parameters": {
            "t_chunk_size": t_chunk_size,
            "xy_chunk_size": xy_chunk_size,
            "xy_shard_size": xy_shard_size,
            "frame_count": frame_count,
            "data_size_gib": data_size_gib,
        },
        "acquire_zarr": az_stats,
        "tensorstore": ts_stats,
        "ratio_ts_to_az": time_ts_ms / time_az_ms,
        "timestamp": time.time(),
        "git_commit_hash": get_git_commit_hash(),
        "system_info": get_system_info(),
    }

    if comparison_result is not None:
        results["comparison"] = comparison_result

    return results


@click.command()
@click.option("--t-chunk-size", default=64, help="Time dimension chunk size")
@click.option(
    "--xy-chunk-size", default=64, help="Spatial dimension chunk size"
)
@click.option(
    "--xy-shard-size", default=16, help="Spatial dimension shard size"
)
@click.option("--frame-count", default=1024, help="Number of frames to write")
@click.option(
    "--output", default="results.json", help="Output file for results"
)
@click.option(
    "--nocompare/--compare",
    default=False,
    help="Disable data comparison between implementations",
)
def main(
    t_chunk_size, xy_chunk_size, xy_shard_size, frame_count, output, nocompare
):
    """Compare write performance of TensorStore vs. acquire-zarr for a Zarr v3 store."""
    results = compare(
        t_chunk_size, xy_chunk_size, xy_shard_size, frame_count, not nocompare
    )

    with open(output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults written to {output}")


if __name__ == "__main__":
    main()
