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
        locked.buffer.hh
        locked.buffer.cpp
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
        thread.pool.hh
        thread.pool.cpp
        s3.connection.hh
        s3.connection.cpp
        file.handle.hh
        file.handle.cpp
        ${PLATFORM_CPP}
        s3.object.hh
        s3.object.cpp
        array.base.hh
        array.base.cpp
        array.hh
        array.cpp
        multiscale.array.hh
        multiscale.array.cpp
        fs.storage.hh
        fs.storage.cpp
        fs.array.hh
        fs.array.cpp
        fs.multiscale.array.hh
        fs.multiscale.array.cpp
        s3.storage.hh
        s3.storage.cpp
        s3.array.hh
        s3.array.cpp
        s3.multiscale.array.hh
        s3.multiscale.array.cpp
        plate.hh
        plate.cpp
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
        OpenMP::OpenMP_CXX
)

target_compile_definitions(${tgt} PRIVATE
        "ACQUIRE_ZARR_API_VERSION=\"0.6.0\""
)

set_target_properties(${tgt} PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
        POSITION_INDEPENDENT_CODE ON
)

install(TARGETS ${tgt}
        EXPORT acquire-zarrTargets
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib
)

# Install public header files
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/
        DESTINATION include
)