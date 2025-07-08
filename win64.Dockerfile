FROM mcr.microsoft.com/windows/servercore:ltsc2022

# Install Chocolatey
RUN powershell -Command \
    Set-ExecutionPolicy Bypass -Scope Process -Force; \
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; \
    iex ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))

# Install Visual Studio Build Tools
RUN powershell -Command \
    Invoke-WebRequest -Uri 'https://aka.ms/vs/17/release/vs_buildtools.exe' -OutFile 'vs_buildtools.exe'; \
    Start-Process -FilePath 'vs_buildtools.exe' -ArgumentList '--quiet', '--wait', '--add', 'Microsoft.VisualStudio.Workload.VCTools', '--add', 'Microsoft.VisualStudio.Component.VC.Tools.x86.x64', '--add', 'Microsoft.VisualStudio.Component.Windows11SDK.22621' -Wait; \
    Remove-Item 'vs_buildtools.exe'

# Install Git and CMake 3.31
RUN choco install -y git
RUN powershell -Command \
    Invoke-WebRequest -Uri 'https://github.com/Kitware/CMake/releases/download/v3.31.0/cmake-3.31.0-windows-x86_64.msi' -OutFile 'cmake.msi'; \
    Start-Process msiexec.exe -Wait -ArgumentList '/I cmake.msi /quiet ADD_CMAKE_TO_PATH=System'; \
    Remove-Item cmake.msi

# Set up vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git -b 2025.03.19 C:\vcpkg
RUN C:\vcpkg\bootstrap-vcpkg.bat
RUN C:\vcpkg\vcpkg.exe integrate install

# Set environment variables
ENV VCPKG_ROOT=C:\\vcpkg
RUN setx PATH "%PATH%;C:\vcpkg" /M

# Enable crash dump collection
RUN reg add "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps" /v DumpFolder /t REG_EXPAND_SZ /d "%%LOCALAPPDATA%%\CrashDumps" /f
RUN reg add "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows\Windows Error Reporting\LocalDumps" /v DumpType /t REG_DWORD /d 2 /f
RUN mkdir "C:\Users\ContainerAdministrator\AppData\Local\CrashDumps"

# Install vcpkg dependencies
COPY vcpkg.json C:/workspace/acquire-zarr/
COPY vcpkg-configuration.json C:/workspace/acquire-zarr/

WORKDIR C:/workspace/acquire-zarr

RUN C:/vcpkg/vcpkg.exe install --triplet=x64-windows-static

COPY cmake C:/workspace/acquire-zarr/cmake
COPY include C:/workspace/acquire-zarr/include
COPY minio-cpp C:/workspace/acquire-zarr/minio-cpp
COPY src C:/workspace/acquire-zarr/src
COPY tests C:/workspace/acquire-zarr/tests
COPY CMakeLists.txt C:/workspace/acquire-zarr/
COPY CMakePresets.json C:/workspace/acquire-zarr/
COPY pyproject.toml C:/workspace/acquire-zarr/
COPY setup.py C:/workspace/acquire-zarr/

# Configure with debug symbols
RUN cmake --preset=default -DVCPKG_TARGET_TRIPLET=x64-windows-static -DCMAKE_BUILD_TYPE=Debug

# Build the project
RUN cmake --build build --config Debug

# Install Python with chocolatey
RUN choco install -y python3

# Install Python dependencies
RUN python -m pip install -U pip "pybind11[global]" "cmake<4.0.0" build numpy pytest zarr

COPY python C:/workspace/acquire-zarr/python
COPY README.md C:/workspace/acquire-zarr/

# Build the Python package
RUN python -m pip install ".[testing]"

