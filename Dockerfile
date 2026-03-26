FROM ubuntu:24.04 AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        git \
        ninja-build \
        pkg-config \
        python3 \
        zip unzip tar \
        libomp-dev \
    && rm -rf /var/lib/apt/lists/*

# ── vcpkg ────────────────────────────────────────────────────────────────────
ENV VCPKG_ROOT=/opt/vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git "$VCPKG_ROOT" \
    && "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics

# ── build ────────────────────────────────────────────────────────────────────
FROM base AS build

WORKDIR /src
COPY . .

# git is needed by cmake versioning; init a throwaway repo so it works
RUN git init \
    && git config user.email "docker@build" \
    && git config user.name "docker" \
    && git add -A \
    && git commit -m "docker build"

RUN cmake --preset=default -B build \
    && cmake --build build --parallel

# ── test (default target) ────────────────────────────────────────────────────
FROM build AS test

WORKDIR /src/build
CMD ["ctest", "--output-on-failure"]
