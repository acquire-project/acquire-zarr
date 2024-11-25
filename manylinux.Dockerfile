FROM dockcross/manylinux-x64

COPY scripts/build-wheel-linux.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
RUN mkdir /acquire-zarr

COPY cmake /acquire-zarr/cmake
COPY include /acquire-zarr/include
COPY python /acquire-zarr/python
COPY src /acquire-zarr/src
COPY CMakeLists.txt /acquire-zarr/CMakeLists.txt
COPY CMakePresets.json /acquire-zarr/CMakePresets.json
COPY vcpkg-configuration.json /acquire-zarr/vcpkg-configuration.json
COPY vcpkg.json /acquire-zarr/vcpkg.json

COPY LICENSE /acquire-zarr/LICENSE
COPY MANIFEST.in /acquire-zarr/MANIFEST.in
COPY pyproject.toml /acquire-zarr/pyproject.toml
COPY README.md /acquire-zarr/README.md
COPY setup.py /acquire-zarr/setup.py

ENTRYPOINT ["/entrypoint.sh"]