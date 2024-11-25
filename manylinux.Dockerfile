FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

RUN apt-get update && apt-get install -y \
    wget \
    software-properties-common \
    lsb-release \
    gnupg

RUN wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null
RUN apt-add-repository "deb https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main"

RUN apt-get install -y \
    build-essential \
    curl \
    git \
    python3-dev \
    python3-pip \
    python3-venv \
    yasm \
    cmake \
    git \
    zip \
    unzip \
    tar \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --upgrade pip \
    && python3 -m pip install \
    wheel \
    setuptools \
    twine \
    auditwheel \
    build

RUN cd / && git clone https://github.com/microsoft/vcpkg.git && cd vcpkg && ./bootstrap-vcpkg.sh

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