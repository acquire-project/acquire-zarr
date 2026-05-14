# syntax=docker/dockerfile:1

# CPU-only shim build & test — no CUDA, no vcpkg.
FROM ubuntu:24.04 AS deps

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        ninja-build \
        libzstd-dev \
        liblz4-dev \
        libblosc-dev \
        zlib1g-dev \
        libsnappy-dev \
        libomp-dev \
        libssl-dev \
        nlohmann-json3-dev \
        git \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Build AWS C libraries from source (not packaged in Ubuntu 24.04).
RUN git clone --depth 1 --branch v0.12.6 https://github.com/awslabs/aws-c-common.git      /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.9.13  https://github.com/awslabs/aws-c-cal.git          /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.2.10  https://github.com/awslabs/aws-checksums.git      /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v1.7.0   https://github.com/aws/s2n-tls.git               /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.26.3  https://github.com/awslabs/aws-c-io.git           /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.3.2   https://github.com/awslabs/aws-c-compression.git  /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.10.12 https://github.com/awslabs/aws-c-http.git         /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.2.4   https://github.com/awslabs/aws-c-sdkutils.git     /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.10.1  https://github.com/awslabs/aws-c-auth.git         /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.11.5  https://github.com/awslabs/aws-c-s3.git           /tmp/b && cmake -S /tmp/b -B /tmp/b/build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/aws -DCMAKE_PREFIX_PATH=/opt/aws -DBUILD_TESTING=OFF && cmake --build /tmp/b/build --target install && rm -rf /tmp/b

ENV CMAKE_PREFIX_PATH="/opt/aws"

# AWS CLI for S3 integration tests and bucket creation
RUN apt-get update && apt-get install -y --no-install-recommends \
        wget unzip \
    && rm -rf /var/lib/apt/lists/* \
    && wget -qO /tmp/awscliv2.zip \
        "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/awscliv2.zip /tmp/aws

# uv for chucky's test_ome_validate (ome-zarr validator invocation).
RUN wget -qO- https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

FROM deps AS build
WORKDIR /src
COPY . .

ARG CMAKE_BUILD_TYPE=Release
RUN cmake -S shim -B shim/build -G Ninja \
        -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE} \
        -DCHUCKY_ENABLE_GPU=OFF \
    && cmake --build shim/build

FROM build AS test
WORKDIR /src/shim/build
RUN ctest --output-on-failure

# --- Python wheel --------------------------------------------------------
#
# Build compression libs from source as static+PIC so the wheel is
# self-contained (no shared-lib runtime dependencies).

FROM ubuntu:24.04 AS wheel-deps

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates \
        libomp-dev libssl-dev nlohmann-json3-dev \
        python3-dev python3-pip python3-venv \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# PIC flags used for all from-source deps
ENV PIC_FLAGS="-DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=/opt/pic -DCMAKE_PREFIX_PATH=/opt/pic -DBUILD_TESTING=OFF -G Ninja"

RUN git clone --depth 1 --branch v1.10.0 https://github.com/lz4/lz4.git /tmp/b \
    && cmake -S /tmp/b/build/cmake -B /tmp/b/out $PIC_FLAGS \
       -DBUILD_SHARED_LIBS=OFF -DLZ4_BUILD_CLI=OFF \
    && cmake --build /tmp/b/out --target install && rm -rf /tmp/b

RUN git clone --depth 1 --branch v1.5.7 https://github.com/facebook/zstd.git /tmp/b \
    && cmake -S /tmp/b/build/cmake -B /tmp/b/out $PIC_FLAGS \
       -DZSTD_BUILD_PROGRAMS=OFF -DZSTD_BUILD_SHARED=OFF \
    && cmake --build /tmp/b/out --target install && rm -rf /tmp/b

RUN git clone --depth 1 --branch v1.21.6 https://github.com/Blosc/c-blosc.git /tmp/b \
    && cmake -S /tmp/b -B /tmp/b/out $PIC_FLAGS \
       -DBUILD_SHARED=OFF -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF -DBUILD_FUZZERS=OFF \
       -DPREFER_EXTERNAL_LZ4=ON -DPREFER_EXTERNAL_ZSTD=ON \
    && cmake --build /tmp/b/out --target install && rm -rf /tmp/b

# AWS C libraries (same as deps stage, but with PIC)
RUN git clone --depth 1 --branch v0.12.6 https://github.com/awslabs/aws-c-common.git      /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.9.13  https://github.com/awslabs/aws-c-cal.git          /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.2.10  https://github.com/awslabs/aws-checksums.git      /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v1.7.0   https://github.com/aws/s2n-tls.git               /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.26.3  https://github.com/awslabs/aws-c-io.git           /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.3.2   https://github.com/awslabs/aws-c-compression.git  /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.10.12 https://github.com/awslabs/aws-c-http.git         /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.2.4   https://github.com/awslabs/aws-c-sdkutils.git     /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.10.1  https://github.com/awslabs/aws-c-auth.git         /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b \
 && git clone --depth 1 --branch v0.11.5  https://github.com/awslabs/aws-c-s3.git           /tmp/b && cmake -S /tmp/b -B /tmp/b/build $PIC_FLAGS && cmake --build /tmp/b/build --target install && rm -rf /tmp/b

ENV CMAKE_PREFIX_PATH="/opt/pic"

FROM wheel-deps AS wheel-build
WORKDIR /src
COPY . .

RUN python3 -m venv /venv \
    && /venv/bin/pip install --upgrade pip \
    && /venv/bin/pip install build pybind11[global]

ARG CMAKE_BUILD_TYPE=Release
RUN /venv/bin/python -m build --wheel --outdir /wheels /src/shim/python

FROM scratch AS wheel
COPY --from=wheel-build /wheels/*.whl /
