import glob
import os
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

        # Required for auto-detection of auxiliary "native" libs
        if not extdir.endswith(os.path.sep):
            extdir += os.path.sep

        build_dir = os.path.abspath(os.path.join(ext.sourcedir, 'build'))

        cfg = "Debug" if self.debug else "Release"

        # Set CMAKE_BUILD_TYPE
        cmake_args = [
            "--preset=default",
            f"-DCMAKE_BUILD_TYPE={cfg}",
            "-DBUILD_PYTHON=ON",
            "-DBUILD_TESTS=OFF"
        ]
        # cmake_args += [f"-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{cfg.upper()}={extdir}"]

        if self.compiler.compiler_type == "msvc":
            cmake_args.append("-DVCPKG_TARGET_TRIPLET=x64-windows-static")

        build_args = ['--config', cfg]

        # Set CMAKE_BUILD_PARALLEL_LEVEL for parallel builds
        if "CMAKE_BUILD_PARALLEL_LEVEL" not in os.environ:
            if hasattr(self, "parallel") and self.parallel:
                build_args += [f"-j{self.parallel}"]

        # Build the entire project
        subprocess.check_call(['cmake', ext.sourcedir] + cmake_args, cwd=build_dir)
        subprocess.check_call(['cmake', '--build', '.'] + build_args, cwd=build_dir)

        # Move the built extension to the correct location
        built_ext = os.path.join(build_dir, 'python', cfg, 'acquire_zarr*.pyd')
        self.move_file(built_ext, self.get_ext_fullpath(ext.name))

    def move_file(self, src, dst):
        import shutil
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            for filename in glob.glob(src):
                shutil.move(filename, dst)
        except Exception as e:
            print(f"Error moving file from {src} to {dst}: {e}")
            raise


setup(
    name='acquire-zarr',
    version='0.0.1',
    author='Your Name',
    author_email='your.email@example.com',
    description='Python bindings for acquire-zarr',
    long_description='',
    ext_modules=[CMakeExtension('acquire_zarr')],
    cmdclass=dict(build_ext=CMakeBuild),
    zip_safe=False,
    python_requires='>=3.6',
)
