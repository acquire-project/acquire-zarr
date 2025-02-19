#!/usr/bin/env python3

import subprocess
import sys
import os
import math
from datetime import datetime
from pathlib import Path
import signal
import time

# Configuration
CHUNK_CONFIGS = [
    "1,1,64,64,64",
    "1,1,128,128,128",
    "1,1,256,256,256"
]

COMPRESSION_TYPES = ["none", "lz4", "zstd"]
ARRAY_WIDTH = 1920
ARRAY_HEIGHT = 1080

outfile = None


def max_cps(chunk_size, array_size):
    """Calculate maximum chunks per shard."""
    return math.ceil(array_size / chunk_size)


def run_benchmark(cmd, timeout=300):
    """
    Run a single benchmark command with timeout.
    Returns (success, output) tuple.
    """
    try:
        # Run benchmark and capture output
        # stderr is redirected to devnull to keep only the CSV output clean
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=timeout,
            text=True,
            check=True
        )
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        print(f"Benchmark timed out after {timeout}s: {' '.join(cmd)}", file=sys.stderr)
        return False, None
    except subprocess.CalledProcessError as e:
        print(f"Benchmark failed with exit code {e.returncode}: {' '.join(cmd)}", file=sys.stderr)
        return False, None
    except Exception as e:
        print(f"Error running benchmark: {e}", file=sys.stderr)
        return False, None


def get_executable():
    """Get the benchmark executable name based on platform."""
    if len(sys.argv) > 1:
        return sys.argv[1]

    if sys.platform == "win32":
        return "./benchmark.exe"
    return "./benchmark"


def write_out(msg):
    """Write message to output file and terminal (if not redirected)."""
    if sys.stdout.isatty():
        print(msg)
    print(msg, file=outfile)


def main():
    # Setup benchmark executable
    executable = get_executable()
    if not Path(executable).is_file():
        print(f"Error: '{executable}' not found", file=sys.stderr)
        sys.exit(1)

    # Get S3 configuration if available
    s3_args = []
    if os.getenv("ZARR_S3_ENDPOINT"):
        s3_args = [
            "--s3-endpoint", os.getenv("ZARR_S3_ENDPOINT"),
            "--s3-bucket", os.getenv("ZARR_S3_BUCKET_NAME"),
            "--s3-access-key", os.getenv("ZARR_S3_ACCESS_KEY_ID"),
            "--s3-secret-key", os.getenv("ZARR_S3_SECRET_ACCESS_KEY")
        ]

    outfile_path = f"zarr_benchmarks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    print(f"Writing output to {outfile_path}", file=sys.stderr)

    global outfile
    outfile = open(f"zarr_benchmarks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "w")

    # print header
    write_out("chunk_size,zarr_version,compression,storage,chunks_per_shard_y,chunks_per_shard_x,time_seconds,run")

    # Run V2 benchmarks
    for chunk in CHUNK_CONFIGS:
        for compression in COMPRESSION_TYPES:
            # Filesystem storage
            cmd = [
                executable,
                "--chunk", chunk,
                "--version", "2",
                "--compression", compression,
                "--storage", "filesystem"
            ]
            for run in range(5):
                success, output = run_benchmark(cmd)
                if success and output:
                    write_out(output + f",{run + 1}")

            # S3 storage if configured
            if s3_args:
                cmd = [
                          executable,
                          "--chunk", chunk,
                          "--version", "2",
                          "--compression", compression,
                          "--storage", "s3"
                      ] + s3_args
                for run in range(5):
                    success, output = run_benchmark(cmd)
                    if success and output:
                        write_out(output + f",{run + 1}")

    # Run V3 benchmarks with sharding
    for chunk in CHUNK_CONFIGS:
        # Parse chunk dimensions
        chunk_dims = [int(x) for x in chunk.split(",")]
        chunk_y, chunk_x = chunk_dims[3], chunk_dims[4]

        # Calculate maximum chunks per shard
        max_cps_y = max_cps(chunk_y, ARRAY_HEIGHT)
        max_cps_x = max_cps(chunk_x, ARRAY_WIDTH)

        # Test different shard configurations
        cps_y = 1
        while cps_y <= max_cps_y:
            cps_x = 1
            while cps_x <= max_cps_x:
                for compression in COMPRESSION_TYPES:
                    # Filesystem storage
                    cmd = [
                        executable,
                        "--chunk", chunk,
                        "--version", "3",
                        "--compression", compression,
                        "--storage", "filesystem",
                        "--shard-y", str(cps_y),
                        "--shard-x", str(cps_x)
                    ]
                    for run in range(5):
                        success, output = run_benchmark(cmd)
                        if success and output:
                            write_out(output + f",{run + 1}")

                    # S3 storage if configured
                    if s3_args:
                        cmd = [
                                  executable,
                                  "--chunk", chunk,
                                  "--version", "3",
                                  "--compression", compression,
                                  "--storage", "s3",
                                  "--shard-y", str(cps_y),
                                  "--shard-x", str(cps_x)
                              ] + s3_args
                        for run in range(5):
                            success, output = run_benchmark(cmd)
                            if success and output:
                                write_out(output + f",{run + 1}")

                cps_x *= 2
            cps_y *= 2

    if outfile is not None:
        outfile.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBenchmark interrupted by user", file=sys.stderr)
        if outfile is not None:
            outfile.close()
        sys.exit(1)
