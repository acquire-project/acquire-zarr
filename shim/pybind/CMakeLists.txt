project(acquire-zarr-shim-py)

find_package(Python3 REQUIRED COMPONENTS Interpreter Development.Module)

execute_process(COMMAND ${Python3_EXECUTABLE} -m pybind11 --cmakedir
    RESULT_VARIABLE pybind11_NOT_FOUND
    OUTPUT_VARIABLE pybind11_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

if(pybind11_NOT_FOUND)
    message(FATAL_ERROR "pybind11 not found. Install via: pip install pybind11")
else()
    list(APPEND CMAKE_MODULE_PATH ${pybind11_DIR})
    cmake_path(CONVERT CMAKE_MODULE_PATH TO_CMAKE_PATH_LIST CMAKE_MODULE_PATH)
endif()

find_package(pybind11 REQUIRED)

pybind11_add_module(acquire_zarr
    ${CMAKE_CURRENT_SOURCE_DIR}/../../python/acquire-zarr-py.cpp
)

target_include_directories(acquire_zarr PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
)

if(CHUCKY_ENABLE_GPU)
    target_link_libraries(acquire_zarr PRIVATE acquire-zarr-chucky-gpu chucky_log)
else()
    target_link_libraries(acquire_zarr PRIVATE acquire-zarr-chucky-cpu chucky_log)
endif()

target_compile_definitions(acquire_zarr PRIVATE ACQUIRE_ZARR_WITH_CHUCKY_LOG=1)

target_compile_features(acquire_zarr PRIVATE cxx_std_20)

set_target_properties(acquire_zarr PROPERTIES
    OUTPUT_NAME "__init__"
    MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)
