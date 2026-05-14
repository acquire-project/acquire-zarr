cmake_minimum_required(VERSION 3.18)
project(acquire-zarr-shim LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)

option(BUILD_PYTHON "Build Python bindings" OFF)

enable_testing()
include(CTest)

# Chucky's CMakeLists.txt uses CMAKE_SOURCE_DIR for its cmake module path,
# which breaks when it's a subdirectory. Prepend the correct path here.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/chucky/cmake")

# Apple's clang doesn't find Homebrew's keg-only libomp automatically —
# pre-seed the FindOpenMP hints when it's installed there. Skip when a
# conda/micromamba env is active (llvm-openmp is picked up via
# CMAKE_PREFIX_PATH=$CONDA_PREFIX).
if(APPLE)
    if(CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL "arm64")
        set(_brew_libomp "/opt/homebrew/opt/libomp")
    else()
        set(_brew_libomp "/usr/local/opt/libomp")
    endif()
    if(EXISTS "${_brew_libomp}/include/omp.h"
       AND NOT DEFINED ENV{CONDA_PREFIX})
        set(OpenMP_C_FLAGS "-Xclang;-fopenmp;-I${_brew_libomp}/include")
        set(OpenMP_CXX_FLAGS "-Xclang;-fopenmp;-I${_brew_libomp}/include")
        set(OpenMP_C_LIB_NAMES "omp")
        set(OpenMP_CXX_LIB_NAMES "omp")
        set(OpenMP_omp_LIBRARY "${_brew_libomp}/lib/libomp.a")
    endif()
endif()

add_subdirectory(chucky)

# --- shim library --------------------------------------------------------

set(shim_sources
    shim.c shim_array.c shim_hcs.c shim_hcs_json.c shim_log.c
    shim_settings.c shim_util.c shim_convert.c shim_sink.c)
set(shim_non_backend_libs
    store_api store_fs store_s3 zarr_array zarr_group ngff_multiscale
    hcs hcs_metadata
    dimension writer stream_config
    platform chucky_log
)

add_library(acquire-zarr-chucky-cpu STATIC ${shim_sources})

target_include_directories(acquire-zarr-chucky-cpu
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/../include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
)

target_link_libraries(acquire-zarr-chucky-cpu PRIVATE
    stream_cpu multiarray_cpu
    ${shim_non_backend_libs}
)

target_compile_definitions(acquire-zarr-chucky-cpu PRIVATE
    ACQUIRE_ZARR_API_VERSION="0.6.0"
)

# MSVC gates <stdatomic.h> behind an experimental flag; the shim uses C11
# atomics for the log-level guard.
if(MSVC)
    target_compile_options(acquire-zarr-chucky-cpu PRIVATE
        /experimental:c11atomics)
endif()

set_target_properties(acquire-zarr-chucky-cpu PROPERTIES
    POSITION_INDEPENDENT_CODE ON
)

if(CHUCKY_ENABLE_GPU)
    add_library(acquire-zarr-chucky-gpu STATIC ${shim_sources})

    target_include_directories(acquire-zarr-chucky-gpu
        PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/../include
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
    )

    target_link_libraries(acquire-zarr-chucky-gpu PRIVATE
        stream multiarray_gpu
        ${shim_non_backend_libs}
    )

    target_compile_definitions(acquire-zarr-chucky-gpu PRIVATE
        ACQUIRE_ZARR_API_VERSION="0.6.0"
        SHIM_BACKEND_GPU=1
    )

    if(MSVC)
        target_compile_options(acquire-zarr-chucky-gpu PRIVATE
            /experimental:c11atomics)
    endif()

    set_target_properties(acquire-zarr-chucky-gpu PROPERTIES
        POSITION_INDEPENDENT_CODE ON
    )
endif()

# --- Python bindings -----------------------------------------------------

if(BUILD_PYTHON)
    add_subdirectory(pybind)
endif()

# --- logger compat lib for integration test macros -----------------------

if(BUILD_TESTING)
add_library(shim-test-logger STATIC
    compat/logger.cpp
)

target_include_directories(shim-test-logger PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/compat
)

set_target_properties(shim-test-logger PROPERTIES
    CXX_STANDARD 17
    CXX_STANDARD_REQUIRED ON
)

# --- integration tests ---------------------------------------------------

find_package(nlohmann_json CONFIG QUIET)

if(nlohmann_json_FOUND)
    set(integration_tests
        stream-raw-to-filesystem
        stream-named-array-to-filesystem
        stream-compressed-to-filesystem
        stream-zstd-compressed-to-filesystem
        stream-2d-multiscale-to-filesystem
        stream-3d-multiscale-to-filesystem
        stream-multi-frame-append
        stream-multiscale-trivial-3rd-dim
        stream-multiple-arrays-to-filesystem
        estimate-memory-usage
        stream-pure-hcs-acquisition
        stream-mixed-flat-and-hcs-acquisition
        stream-with-ragged-final-shard
        stream-raw-to-s3
        stream-named-array-to-s3
        stream-compressed-to-s3
        stream-append-nullptr
        stream-throws-on-overflow
    )

    # These tests have chucky-specific LOD expectations (see shim/plan.md #1).
    # We carry shim-flavoured copies at shim/tests/integration/ and source
    # them instead of the baseline versions at tests/integration/.
    set(shim_override_tests
        stream-2d-multiscale-to-filesystem
        stream-3d-multiscale-to-filesystem
        stream-multiple-arrays-to-filesystem
    )

    foreach(name ${integration_tests})
        set(tgt "shim-test-${name}")
        if(name IN_LIST shim_override_tests)
            set(src "${CMAKE_CURRENT_SOURCE_DIR}/tests/integration/${name}.cpp")
        else()
            set(src "${CMAKE_CURRENT_SOURCE_DIR}/../tests/integration/${name}.cpp")
        endif()
        add_executable(${tgt} ${src})
        target_compile_definitions(${tgt} PRIVATE
            "TEST=\"${tgt}\""
            S3_TEST_HELPERS_USE_AWS_CLI
        )
        target_include_directories(${tgt} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/../include
            ${CMAKE_CURRENT_SOURCE_DIR}/../tests/integration
        )
        target_link_libraries(${tgt} PRIVATE
            acquire-zarr-chucky-cpu
            shim-test-logger
            nlohmann_json::nlohmann_json
        )
        set_target_properties(${tgt} PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED ON
        )
        add_test(NAME ${tgt} COMMAND ${tgt})

        set(labels "shim;integration")
        if(name MATCHES ".*s3.*")
            list(APPEND labels "s3")
        endif()
        set_tests_properties(${tgt} PROPERTIES LABELS "${labels}")
    endforeach()
else()
    message(STATUS "nlohmann_json not found -- skipping integration tests")
endif()
endif() # BUILD_TESTING
