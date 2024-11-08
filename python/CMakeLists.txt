project(acquire-zarr-py)

execute_process(COMMAND python3 -m pybind11 --cmakedir
        RESULT_VARIABLE pybind11_NOT_FOUND
        OUTPUT_VARIABLE pybind11_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
)

if (pybind11_NOT_FOUND)
    message(FATAL_ERROR "pybind11 not found in the current environment. Please install pybind11 via pip.")
else ()
    LIST(APPEND CMAKE_MODULE_PATH ${pybind11_DIR})
    cmake_path(CONVERT CMAKE_MODULE_PATH TO_CMAKE_PATH_LIST CMAKE_MODULE_PATH)
endif ()

find_package(Python3 REQUIRED COMPONENTS Interpreter Development)
find_package(pybind11 REQUIRED)

pybind11_add_module(acquire_zarr acquire-zarr-py.cpp)

target_include_directories(acquire_zarr PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(acquire_zarr PRIVATE acquire-zarr)

set_target_properties(acquire_zarr PROPERTIES
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)
