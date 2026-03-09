#!/usr/bin/env python3
# scripts/generate-launch.py
"""Generate .vscode/launch.json to enable debugging test targets."""
import json
import os
import platform
from pathlib import Path

repo_root = Path(__file__).parent.parent

test_dirs = [
    repo_root / "cmake-build-debug" / "tests" / "unit-tests",
    repo_root / "cmake-build-debug" / "tests" / "integration",
]

system = platform.system()

if system == "Windows":
    test_dirs = [d / "Debug" for d in test_dirs]
    executables = []
    for d in test_dirs:
        if d.is_dir():
            executables.extend(str(f) for f in d.iterdir() if f.suffix == ".exe")
    configuration = {
        "name": "Debug Test",
        "type": "cppvsdbg",
        "request": "launch",
        "program": "${input:testExecutable}",
        "args": [],
        "cwd": "${workspaceFolder}",
    }
else:
    executables = []
    for d in test_dirs:
        if d.is_dir():
            executables.extend(
                str(f) for f in d.iterdir()
                if f.is_file() and os.access(f, os.X_OK)
            )
    configuration = {
        "name": "Debug Test",
        "type": "cppdbg",
        "request": "launch",
        "program": "${input:testExecutable}",
        "args": [],
        "cwd": "${workspaceFolder}",
        "MIMode": "lldb" if system == "Darwin" else "gdb",
    }

launch = {
    "version": "0.2.0",
    "configurations": [configuration],
    "inputs": [
        {
            "id": "testExecutable",
            "type": "pickString",
            "description": "Select test to debug",
            "options": executables,
        }
    ],
}

vscode_dir = repo_root / ".vscode"
vscode_dir.mkdir(exist_ok=True)

with open(vscode_dir / "launch.json", "w") as f:
    json.dump(launch, f, indent=4)

print(f"Generated .vscode/launch.json with {len(executables)} test(s).")
