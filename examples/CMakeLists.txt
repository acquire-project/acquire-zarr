cmake_minimum_required(VERSION 3.23)
project(acquire-zarr-examples)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 20)

find_package(acquire-zarr REQUIRED)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

add_executable(zarrv3-compressed-filesystem zarrv3-compressed-filesystem.c)
target_link_libraries(zarrv3-compressed-filesystem acquire-zarr::acquire-zarr)

add_executable(zarrv3-compressed-multiscale-s3 zarrv3-compressed-multiscale-s3.c)
target_link_libraries(zarrv3-compressed-multiscale-s3 acquire-zarr::acquire-zarr)

add_executable(zarrv3-compressed-s3 zarrv3-compressed-s3.c)
target_link_libraries(zarrv3-compressed-s3 acquire-zarr::acquire-zarr)

add_executable(zarrv3-raw-filesystem zarrv3-raw-filesystem.c)
target_link_libraries(zarrv3-raw-filesystem acquire-zarr::acquire-zarr)

add_executable(zarrv3-raw-multiscale-filesystem zarrv3-raw-multiscale-filesystem.c)
target_link_libraries(zarrv3-raw-multiscale-filesystem acquire-zarr::acquire-zarr)

add_executable(zarrv3-raw-s3 zarrv3-raw-s3.c)
target_link_libraries(zarrv3-raw-s3 acquire-zarr::acquire-zarr)
