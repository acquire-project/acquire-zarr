# Cross-platform development setup for acquire-zarr
# run `just` without arguments to see available commands

set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
set shell := ["bash", "-cu"]

# Global paths - exported to all child processes
ROOT := justfile_directory()
VCPKG_DIR := ROOT / "vcpkg"
export VCPKG_ROOT := VCPKG_DIR

# Default recipe
default:
    @just --list

# Full development setup: submodules + vcpkg + uv sync
# (args are passed to uv sync, e.g.: `just install -p 3.12`)
install *args: _setup-submodules setup-vcpkg (uv-sync args)

# Run uv sync (includes testing dependencies)
uv-sync *args: _ensure-uv
    uv sync --extra testing {{args}}

# Run tests
# (args are passed to pytest, e.g.: `just test -k test_function`)
test *args: _ensure-uv
    uv run pytest {{args}}

# Setup vcpkg (clone and bootstrap if needed)
[unix]
setup-vcpkg:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -d "{{VCPKG_DIR}}/.git" ]; then
        echo "Cloning vcpkg..."
        git clone https://github.com/microsoft/vcpkg.git "{{VCPKG_DIR}}"
    fi
    if [ ! -f "{{VCPKG_DIR}}/vcpkg" ]; then
        echo "Bootstrapping vcpkg..."
        "{{VCPKG_DIR}}/bootstrap-vcpkg.sh" -disableMetrics
    fi
    # macOS: install libomp if needed
    if [ "$(uname)" = "Darwin" ] && ! brew list libomp &>/dev/null; then
        echo "Installing libomp via Homebrew..."
        brew install libomp
    fi

[windows]
setup-vcpkg:
    if (-not (Test-Path "{{VCPKG_DIR}}\.git")) {
        Write-Host "Cloning vcpkg..."
        git clone https://github.com/microsoft/vcpkg.git "{{VCPKG_DIR}}"
    }
    if (-not (Test-Path "{{VCPKG_DIR}}\vcpkg.exe")) {
        Write-Host "Bootstrapping vcpkg..."
        & "{{VCPKG_DIR}}\bootstrap-vcpkg.bat" -disableMetrics
    }

# Build with cmake directly (useful for C development)
[unix]
cmake-build:
    cmake --preset=default -B build . && cmake --build build

[windows]
cmake-build:
    cmake --preset=default -B build -DVCPKG_TARGET_TRIPLET=x64-windows-static .
    cmake --build build

# Update vcpkg to latest
[unix]
update-vcpkg:
    cd "{{VCPKG_DIR}}" && git pull && ./bootstrap-vcpkg.sh -disableMetrics

[windows]
update-vcpkg:
    Push-Location "{{VCPKG_DIR}}"; git pull; .\bootstrap-vcpkg.bat -disableMetrics; Pop-Location

# Clean build artifacts (keeps vcpkg)
[unix]
clean:
    rm -rf build/ .venv/ dist/ *.egg-info/ .pytest_cache/ .mypy_cache/ .ruff_cache/
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

[windows]
clean:
    @("build", ".venv", "dist", ".pytest_cache", ".mypy_cache", ".ruff_cache") | ForEach-Object { if (Test-Path $_) { Remove-Item -Recurse -Force $_ } }
    Get-ChildItem -Filter "*.egg-info" -Directory -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force
    Get-ChildItem -Recurse -Directory -Filter "__pycache__" -ErrorAction SilentlyContinue | Remove-Item -Recurse -Force

# Clean everything including vcpkg
[unix]
clean-all: clean
    rm -rf "{{VCPKG_DIR}}"

[windows]
clean-all: clean
    if (Test-Path "{{VCPKG_DIR}}") { Remove-Item -Recurse -Force "{{VCPKG_DIR}}" }

_setup-submodules:
    git -C "{{ROOT}}" submodule update --init --recursive

[unix]
_ensure-uv:
    @command -v uv >/dev/null || { echo "This command requires uv: https://docs.astral.sh/uv/getting-started/installation/"; exit 1; }

[windows]
_ensure-uv:
    @if (-not (Get-Command uv -ErrorAction SilentlyContinue)) { Write-Host "This command requires uv: https://docs.astral.sh/uv/getting-started/installation/"; exit 1 }
