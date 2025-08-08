cmake_minimum_required(VERSION 3.23)
project(acquire-zarr-examples)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)

find_package(acquire-zarr REQUIRED)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

add_executable(stream-raw-to-filesystem stream-raw-to-filesystem.c)
target_link_libraries(stream-raw-to-filesystem acquire-zarr::acquire-zarr)

add_executable(stream-raw-multiscale-to-filesystem stream-raw-multiscale-to-filesystem.c)
target_link_libraries(stream-raw-multiscale-to-filesystem acquire-zarr::acquire-zarr)

add_executable(stream-compressed-to-filesystem stream-compressed-to-filesystem.c)
target_link_libraries(stream-compressed-to-filesystem acquire-zarr::acquire-zarr)

add_executable(stream-raw-to-s3 stream-raw-to-s3.c)
target_link_libraries(stream-raw-to-s3 acquire-zarr::acquire-zarr)

add_executable(stream-compressed-to-s3 stream-compressed-to-s3.c)
target_link_libraries(stream-compressed-to-s3 acquire-zarr::acquire-zarr)

add_executable(stream-compressed-multiscale-to-s3 stream-compressed-multiscale-to-s3.c)
target_link_libraries(stream-compressed-multiscale-to-s3 acquire-zarr::acquire-zarr)

add_executable(stream-multiarray-to-filesystem stream-multiarray-to-filesystem.cpp)
target_link_libraries(stream-multiarray-to-filesystem acquire-zarr::acquire-zarr)
