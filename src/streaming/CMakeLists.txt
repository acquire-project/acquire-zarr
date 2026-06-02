set(tgt acquire-zarr)

if (WIN32)
    set(PLATFORM_CPP win32/platform.cpp)
else ()
    set(PLATFORM_CPP posix/platform.cpp)
endif ()

add_library(${tgt}
        macros.hh
        acquire.zarr.cpp
        array.dimensions.hh
        array.dimensions.cpp
        frame.queue.hh
        frame.queue.cpp
        downsampler.hh
        downsampler.cpp
        zarr.stream.hh
        zarr.stream.cpp
        zarr.common.hh
        zarr.common.cpp
        blosc.compression.params.hh
        blosc.compression.params.cpp
        zstd.compression.params.hh
        compression.params.hh
        thread.pool.hh
        thread.pool.cpp
        s3.connection.hh
        s3.connection.cpp
        file.handle.hh
        file.handle.cpp
        sink.hh
        sink.cpp
        file.sink.hh
        file.sink.cpp
        ${PLATFORM_CPP}
        s3.sink.hh
        s3.sink.cpp
        array.base.hh
        array.base.cpp
        array.hh
        array.cpp
        multiscale.array.hh
        multiscale.array.cpp
        plate.hh
        plate.cpp
        chunk.hh
        chunk.cpp
        shard.hh
        shard.cpp
        $<TARGET_OBJECTS:acquire-logger-obj>
)

target_include_directories(${tgt}
        PUBLIC
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
        PRIVATE
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src/logger>
)

target_link_libraries(${tgt} PRIVATE
        blosc_static
        miniocpp::miniocpp
        Crc32c::crc32c
        $<IF:$<TARGET_EXISTS:zstd::libzstd_static>,zstd::libzstd_static,zstd::libzstd_shared>
        OpenMP::OpenMP_CXX
)

target_compile_definitions(${tgt} PRIVATE
        "ACQUIRE_ZARR_API_VERSION=\"0.8.0\""
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
        POSITION_INDEPENDENT_CODE ON
)

# Performance: AVX2 baseline for the streaming hot path (tiling/downsample
# loops). NB: requires an AVX2-capable CPU at runtime; gate for distribution
# on pre-AVX2 targets. Blosc has its own runtime SIMD dispatch.
target_enable_simd(${tgt})

# Link-time optimization for Release builds of the hot-path library.
include(CheckIPOSupported)
check_ipo_supported(RESULT ipo_supported OUTPUT ipo_error LANGUAGES CXX)
if (ipo_supported)
    set_target_properties(${tgt} PROPERTIES INTERPROCEDURAL_OPTIMIZATION_RELEASE ON)
else ()
    message(STATUS "IPO/LTO not supported, skipping: ${ipo_error}")
endif ()

install(TARGETS ${tgt}
        EXPORT acquire-zarrTargets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

# Install public header files
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
        DESTINATION include
)