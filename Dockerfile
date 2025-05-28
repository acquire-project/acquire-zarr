# Dockerfile
FROM ubuntu:24.04

# Install build essentials and OpenMP
RUN apt-get update && apt-get install -y \
    ca-certificates \
    gpg \
    wget

RUN test -f /usr/share/doc/kitware-archive-keyring/copyright || \
    wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null

RUN echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ noble main' | tee /etc/apt/sources.list.d/kitware.list >/dev/null
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake=3.30.8-0kitware1ubuntu24.04.1 \
    cmake-data=3.30.8-0kitware1ubuntu24.04.1 \
    libomp-dev \
    curl \
    gdb \
    git \
    htop \
    pkg-config \
    python3 \
    python3-dev \
    python3-venv \
    python3-pip \
    stress-ng \
    valgrind \
    zip

# Set up OpenMP environment for debugging
#ENV OMP_NUM_THREADS=2
ENV OMP_DISPLAY_ENV=TRUE
ENV OMP_DISPLAY_AFFINITY=TRUE
ENV OMP_PROC_BIND=close
ENV OMP_STACKSIZE=100M
ENV OMP_SCHEDULE=STATIC

# Create a script to set up core dumps at runtime
RUN echo '#!/bin/bash\n\
ulimit -c unlimited\n\
echo "Core dumps enabled in /tmp/"\n\
exec "$@"' > /entrypoint.sh && \
    chmod +x /entrypoint.sh

WORKDIR /app

# Copy your source
COPY cmake ./cmake
COPY include ./include
COPY python ./python
COPY src ./src
COPY tests ./tests
COPY CMakeLists.txt .
COPY CMakePresets.json .
COPY pyproject.toml .
COPY setup.py .
COPY README.md .
COPY vcpkg.json .
COPY vcpkg-configuration.json .

# clone minio-cpp
RUN git clone https://github.com/minio/minio-cpp.git

# Install vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git && \
    cd vcpkg && bash ./bootstrap-vcpkg.sh && \
    echo "export VCPKG_ROOT=${PWD}" >> ~/.bashrc && \
    echo "export PATH=\$VCPKG_ROOT:\$PATH:" >> ~/.bashrc && \
    . ~/.bashrc && vcpkg install --triplet x64-linux && \
    cp -r /app/vcpkg_installed/x64-linux/include/* /usr/include && \
    cp -r /app/vcpkg_installed/x64-linux/lib/* /usr/lib && \
    cp -r /app/vcpkg_installed/x64-linux/share/* /usr/share && \
    cp -r /app/vcpkg_installed/x64-linux/debug /usr/debug && \
    cp -r /app/vcpkg_installed/x64-linux/etc /usr/etc && \
    cp -r /app/vcpkg_installed/x64-linux/tools /usr/tools && \
    cp -r /app/vcpkg/scripts /

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]