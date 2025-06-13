cmake_minimum_required(VERSION 3.23)
project(acquire-zarr)
cmake_policy(SET CMP0057 NEW) # allows IN_LIST operator (for pybind11)
cmake_policy(SET CMP0079 NEW) # allows use with targets in other directories
enable_testing()

include(cmake/aq_require.cmake)
include(cmake/git-versioning.cmake)
include(cmake/ide.cmake)
include(cmake/install-prefix.cmake)
include(cmake/wsl.cmake)
include(cmake/simd.cmake)
include(cmake/openmp.cmake)

find_package(nlohmann_json CONFIG REQUIRED)
find_package(blosc CONFIG REQUIRED)
#find_package(miniocpp CONFIG REQUIRED) # TODO (aliddell): uncomment when minio-cpp pushes a new release to vcpkg
find_package(Crc32c CONFIG REQUIRED)
find_package(OpenMP REQUIRED)
find_package(fmt CONFIG REQUIRED)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(BUILD_PYTHON "Build Python bindings" OFF)
option(BUILD_EXAMPLES "Build examples" OFF)
option(BUILD_BENCHMARK "Build benchmarks" OFF)

if(CMAKE_PROJECT_NAME STREQUAL PROJECT_NAME)
    include(CTest)
endif()

if(MSVC)
    # Save the original value to restore later if needed
    set(ORIG_MSVC_RUNTIME_LIBRARY ${CMAKE_MSVC_RUNTIME_LIBRARY})

    # Set to static runtime library
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()
add_subdirectory(minio-cpp)
if(MSVC AND DEFINED ORIG_MSVC_RUNTIME_LIBRARY)
    set(CMAKE_MSVC_RUNTIME_LIBRARY ${ORIG_MSVC_RUNTIME_LIBRARY})
endif()

add_subdirectory(src)
if (BUILD_TESTING)
    add_subdirectory(tests)
else ()
    message(STATUS "Skipping test targets")
endif ()

if (${BUILD_EXAMPLES})
    add_subdirectory(examples)
else ()
    message(STATUS "Skipping examples")
endif ()

if (BUILD_BENCHMARK)
    add_subdirectory(benchmarks)
else ()
    message(STATUS "Skipping benchmarks")
endif ()

if (BUILD_PYTHON)
    add_subdirectory(python)
else ()
    message(STATUS "Skipping Python bindings")
endif ()

include(CPack)
