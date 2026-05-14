import os
from pathlib import Path
import subprocess
from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=""):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def build_extension(self, ext):
        extdir = os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name)))
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep

        # The shim CMakeLists.txt is two directories up from this setup.py
        # (shim/python-gpu/setup.py -> shim/).
        shim_dir = Path(__file__).resolve().parent.parent

        build_dir = shim_dir / "build-wheel-gpu"

        cfg = "Debug" if self.debug else "Release"

        cmake_args = [
            f"-DCMAKE_BUILD_TYPE={cfg}",
            "-DBUILD_PYTHON=ON",
            "-DCHUCKY_ENABLE_GPU=ON",
            "-DCMAKE_CUDA_ARCHITECTURES=80;86;89;90;100",
            "-DBUILD_TESTING=OFF",
        ]

        extra_args = os.environ.get("CMAKE_ARGS", "").split()
        cmake_args += [arg for arg in extra_args if arg]

        build_args = ["--config", cfg]

        if "CMAKE_BUILD_PARALLEL_LEVEL" not in os.environ:
            if hasattr(self, "parallel") and self.parallel:
                build_args += [f"-j{self.parallel}"]

        os.makedirs(build_dir, exist_ok=True)
        subprocess.check_call(
            ["cmake", str(shim_dir)] + cmake_args, cwd=build_dir
        )
        subprocess.check_call(
            ["cmake", "--build", "."] + build_args, cwd=build_dir
        )

        patterns = ("__init__*.so", "__init__*.pyd")
        matching_files = []
        for pattern in patterns:
            matching_files.extend(build_dir.glob(f"**/{pattern}"))
            if matching_files:
                break

        dst = self.get_ext_fullpath(ext.name)
        self._copy_file(matching_files, dst)

    def _copy_file(self, src_files, dst):
        import shutil

        os.makedirs(os.path.dirname(dst), exist_ok=True)
        for filename in src_files:
            shutil.copy2(filename, dst)


setup(
    ext_modules=[CMakeExtension("acquire_zarr.__init__")],
    cmdclass=dict(build_ext=CMakeBuild),
)
