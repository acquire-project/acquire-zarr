import glob
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

        build_dir = os.path.abspath(os.path.join(ext.sourcedir, "build"))

        cfg = "Debug" if self.debug else "Release"

        vcpkg_root = os.environ.get("VCPKG_ROOT")
        if not vcpkg_root:
            raise RuntimeError("VCPKG_ROOT environment variable is not set")

        cmake_args = [
            f"-DCMAKE_TOOLCHAIN_FILE={vcpkg_root}/scripts/buildsystems/vcpkg.cmake",
            f"-DCMAKE_BUILD_TYPE={cfg}",
            "-DBUILD_PYTHON=ON",
            "-DBUILD_TESTING=OFF",
        ]

        extra_args = os.environ.get("CMAKE_ARGS", "").split()
        cmake_args += [arg for arg in extra_args if arg]  # Filter out empty strings

        if self.compiler.compiler_type == "msvc":
            cmake_args.append("-DVCPKG_TARGET_TRIPLET=x64-windows-static")

        build_args = ["--config", cfg]

        if "CMAKE_BUILD_PARALLEL_LEVEL" not in os.environ:
            if hasattr(self, "parallel") and self.parallel:
                build_args += [f"-j{self.parallel}"]

        os.makedirs(build_dir, exist_ok=True)
        subprocess.check_call(["cmake", ext.sourcedir] + cmake_args, cwd=build_dir)
        subprocess.check_call(["cmake", "--build", "."] + build_args, cwd=build_dir)

        py_build_dir = Path(build_dir) / "python"
        patterns = ("__init__*.pyd", "__init__*.so")

        matching_files = []
        for pattern in patterns:
            matching_files.extend(py_build_dir.glob(f"**/{pattern}"))
            if matching_files:
                break

        # Move the built extension to the correct location
        dst = self.get_ext_fullpath(ext.name)
        self.move_file(matching_files, dst)

    def move_file(self, src_files, dst, level=1):
        import shutil

        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            for filename in src_files:
                shutil.copy2(filename, dst)
        except Exception as e:
            print(f"Error moving files to {dst}: {e}")
            raise


setup(
    ext_modules=[CMakeExtension("acquire_zarr.__init__")],
    cmdclass=dict(build_ext=CMakeBuild),
)
