import json
import click
from pathlib import Path
import matplotlib.pyplot as plt
import numpy as np
import re


def create_platform_label(platform_name: str, system_info: dict) -> str:
    """Create a readable platform label from system info."""
    os_name = system_info.get("platform", platform_name)
    arch = system_info.get("architecture", "")

    # Simplify OS names
    os_map = {"Darwin": "macOS", "Windows": "Windows", "Linux": "Linux"}
    os_display = os_map.get(os_name, os_name)

    # Simplify architecture
    arch_map = {
        "x86_64": "x64",
        "AMD64": "x64",
        "arm64": "ARM64",
        "aarch64": "ARM64",
    }
    arch_display = arch_map.get(arch, arch)

    return f"{os_display} {arch_display}"


def get_cpu_config(system_info: dict) -> tuple:
    """Extract CPU configuration for grouping."""
    return (
        system_info.get("cpu_count_physical"),
        system_info.get("cpu_count_logical"),
    )


@click.command()
@click.option(
    "--input-dir",
    "-i",
    default=".",
    type=click.Path(exists=True),
    help="Directory to search for benchmark files",
)
@click.option(
    "--output-prefix",
    "-o",
    default="benchmark_comparison",
    help="Output plot filename prefix",
)
def plot_benchmarks(input_dir, output_prefix):
    """Plot throughput comparison across platforms from benchmark JSON files."""

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
            "test_parameters": result["test_parameters"],
            "system_info": result.get("system_info", {}),
        }

    if not data:
        print(f"No benchmark files found in {input_dir}")
        return

    # Verify all benchmarks use same commit and parameters
    hashes = {data[p]["git_commit_hash"] for p in data}
    assert (
        len(hashes) == 1
    ), "All benchmarks must be from the same git commit hash"
    commit_hash = hashes.pop()

    test_params = {
        tuple(sorted(data[p]["test_parameters"].items())) for p in data
    }
    assert (
        len(test_params) == 1
    ), "All benchmarks must use the same test parameters"
    params = dict(test_params.pop())

    str_params = (
        f"t_chunk={params['t_chunk_size']}, "
        f"xy_chunk={params['xy_chunk_size']}, "
        f"xy_shard={params['xy_shard_size']}, "
        f"frames={params['frame_count']}"
    )
    print(f"Benchmark parameters: {str_params}")

    # Group by CPU configuration
    cpu_groups = {}
    for platform_name, platform_data in data.items():
        cpu_config = get_cpu_config(platform_data["system_info"])
        if cpu_config not in cpu_groups:
            cpu_groups[cpu_config] = {}
        cpu_groups[cpu_config][platform_name] = platform_data

    # Create a plot for each CPU configuration group
    for cpu_config, platforms_data in cpu_groups.items():
        physical, logical = cpu_config
        cpu_label = (
            f"{physical}p{logical}l" if physical and logical else "unknown"
        )

        # Create platform labels
        platform_labels = {}
        for platform_name, platform_data in platforms_data.items():
            label = create_platform_label(
                platform_name, platform_data["system_info"]
            )

            # Add CPU brand to subtitle if available
            cpu_brand = platform_data["system_info"].get("cpu_brand", "")
            platform_labels[platform_name] = (label, cpu_brand)

        # Sort by platform label
        sorted_platforms = sorted(
            platforms_data.keys(), key=lambda p: platform_labels[p][0]
        )

        az_throughput = [
            platforms_data[p]["acquire_zarr"] for p in sorted_platforms
        ]
        ts_throughput = [
            platforms_data[p]["tensorstore"] for p in sorted_platforms
        ]
        display_labels = [platform_labels[p][0] for p in sorted_platforms]

        # Collect unique CPU brands for subtitle
        cpu_brands = set(
            platform_labels[p][1]
            for p in sorted_platforms
            if platform_labels[p][1]
        )
        cpu_subtitle = ", ".join(sorted(cpu_brands)) if cpu_brands else ""

        x = np.arange(len(sorted_platforms))
        width = 0.35

        fig, ax = plt.subplots(figsize=(12, 6))
        ax.bar(x - width / 2, az_throughput, width, label="acquire-zarr")
        ax.bar(x + width / 2, ts_throughput, width, label="tensorstore")

        ax.set_ylabel("Throughput (GiB/s)")

        title_parts = [
            f"Benchmark Throughput by Platform (Commit: {commit_hash[:7]})"
        ]
        if physical and logical:
            title_parts.append(
                f"{physical} physical cores, {logical} logical cores"
            )
        if cpu_subtitle:
            title_parts.append(cpu_subtitle)
        title_parts.append(str_params)

        ax.set_title("\n".join(title_parts), fontsize=10)
        ax.set_xticks(x)
        ax.set_xticklabels(display_labels, rotation=45, ha="right")
        ax.legend()
        ax.grid(axis="y", alpha=0.3)

        plt.tight_layout()

        output_file = f"{output_prefix}_{cpu_label}.png"
        plt.savefig(output_file, dpi=150)
        print(f"Plot saved to {output_file}")
        plt.close()


if __name__ == "__main__":
    plot_benchmarks()
