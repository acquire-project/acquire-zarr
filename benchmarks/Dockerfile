# syntax=docker/dockerfile:1
#
# Benchmark image: runs tensorstore + acquire-zarr under three configs
# (baseline from PyPI, shim-cpu from this tree, shim-gpu from this tree)
# and emits a single comparison plot.
#
# Build context: repo root. `.dockerignore` excludes stray build/venv dirs
# so the wheel builds always start from a clean tree.

# --- wheel build deps -------------------------------------------------
# Shared stage for both the CPU and GPU wheel builds. The GPU image is a
# superset of what the CPU wheel needs; colocating them avoids duplicating
# the ~10 from-source library builds below.

FROM nvidia/cuda:12.8.0-devel-ubuntu24.04 AS wheel-deps

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates \
        libomp-dev libssl-dev nlohmann-json3-dev \
        python3-dev python3-pip python3-venv \
        zlib1g-dev curl xz-utils \
    && rm -rf /var/lib/apt/lists/*

ARG NVCOMP_VERSION=5.1.0.21
RUN curl -fsSL -o /tmp/nvcomp.tgz \
        "https://developer.download.nvidia.com/compute/nvcomp/redist/nvcomp/linux-x86_64/nvcomp-linux-x86_64-${NVCOMP_VERSION}_cuda12-archive.tar.xz" \
    && mkdir -p /opt/nvcomp \
    && tar -xJf /tmp/nvcomp.tgz -C /opt/nvcomp --strip-components=1 \
    && rm /tmp/nvcomp.tgz

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

ENV CMAKE_PREFIX_PATH="/opt/pic:/opt/nvcomp"

RUN python3 -m venv /venv-build \
    && /venv-build/bin/pip install --upgrade pip \
    && /venv-build/bin/pip install build pybind11[global]

# --- CPU wheel build --------------------------------------------------
FROM wheel-deps AS cpu-wheel-build
WORKDIR /src
COPY . .
RUN /venv-build/bin/python -m build --wheel --outdir /wheels-cpu /src/shim/python

# --- GPU wheel build --------------------------------------------------
FROM wheel-deps AS gpu-wheel-build
WORKDIR /src
COPY . .
RUN /venv-build/bin/python -m build --wheel --outdir /wheels-gpu /src/shim/python-gpu

# --- runtime ----------------------------------------------------------
FROM nvidia/cuda:12.8.0-runtime-ubuntu24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 python3-venv ca-certificates curl libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

RUN uv venv /opt/bench --python python3 \
    && uv pip install --python /opt/bench/bin/python \
        tensorstore zarr rich click numpy psutil matplotlib

COPY --from=cpu-wheel-build /wheels-cpu/ /opt/wheels/
COPY --from=gpu-wheel-build /wheels-gpu/ /opt/wheels/

COPY benchmarks/ /src/benchmarks/

WORKDIR /work
ENTRYPOINT ["/src/benchmarks/run_all.sh"]
