project(acquire-zarr-py)

# A wheel build (pip/uv) configures this same build tree with an explicit
# -DPython3_EXECUTABLE pointing into an ephemeral PEP 517 build environment,
# which is deleted once the build finishes. FindPython3 additionally memoizes
# its own result in the internal _Python3_EXECUTABLE and reuses it on every
# later configure. Drop both when they point at something that no longer
# exists, so a plain `cmake` configure re-discovers a real interpreter instead
# of failing with a misleading "Could NOT find Python3".
foreach (_py_cached Python3_EXECUTABLE _Python3_EXECUTABLE)
    if (DEFINED CACHE{${_py_cached}} AND NOT EXISTS "${${_py_cached}}")
        message(STATUS "Discarding stale ${_py_cached}: ${${_py_cached}}")
        unset(${_py_cached} CACHE)
        unset(_Python3_INTERPRETER_SIGNATURE CACHE)
        unset(_Python3_DEVELOPMENT_MODULE_SIGNATURE CACHE)
    endif ()
endforeach ()

# Pin down interpreter selection. On macOS Python3_FIND_FRAMEWORK defaults to
# FIRST, so a Homebrew framework python outranks the active virtualenv and the
# module gets built against the wrong ABI. LOCATION strategy makes the search
# stop at the first location holding an acceptable version rather than hunting
# for the highest version anywhere, which is what makes VIRTUALENV FIRST
# meaningful. The version floor matches requires-python and keeps the
# /usr/bin/python3 stub from being selected as a fallback.
set(Python3_FIND_VIRTUALENV FIRST)
set(Python3_FIND_FRAMEWORK LAST)
set(Python3_FIND_STRATEGY LOCATION)

# With no virtualenv active, fall back to the uv-managed .venv this project
# creates at the repo root, so a bare `cmake -DBUILD_PYTHON=ON` builds against
# the same interpreter `just test` will import from. Without this the search
# lands on whatever system python comes first, which generally has no pybind11.
if (NOT DEFINED Python3_EXECUTABLE
        AND NOT DEFINED ENV{VIRTUAL_ENV}
        AND EXISTS "${CMAKE_SOURCE_DIR}/.venv/bin/python")
    # Set the hint directly rather than faking VIRTUAL_ENV: an explicit
    # Python3_EXECUTABLE also invalidates FindPython3's memoized result, which
    # a mere environment variable does not.
    set(Python3_EXECUTABLE "${CMAKE_SOURCE_DIR}/.venv/bin/python")
    message(STATUS "No active virtualenv; using ${Python3_EXECUTABLE}")
endif ()

# Development.Module doesn't require a .so which is missing in manylinux containers
find_package(Python3 3.10 REQUIRED COMPONENTS Interpreter Development.Module)

message(STATUS "Python bindings: ${Python3_EXECUTABLE} (${Python3_VERSION})")

# pybind11 may be in a non-standard location, so we try to find it using the pybind11 module
execute_process(COMMAND ${Python3_EXECUTABLE} -m pybind11 --cmakedir
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

find_package(pybind11 REQUIRED)

pybind11_add_module(acquire_zarr acquire-zarr-py.cpp)

target_include_directories(acquire_zarr PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(acquire_zarr PRIVATE acquire-zarr)

set_target_properties(acquire_zarr PROPERTIES
        OUTPUT_NAME "__init__"
        MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
)
