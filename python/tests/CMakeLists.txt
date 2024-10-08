enable_testing()

add_test(
        NAME python_api_test
        COMMAND pytest ${CMAKE_CURRENT_LIST_DIR}/test_zarr.py
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)

if (WIN32)
    set(PYTHONPATH "${acquire-zarr-py_BINARY_DIR}/${CMAKE_BUILD_TYPE}")
    message(STATUS "WINDOWS PYTHONPATH: ${PYTHONPATH}")
else ()
    set(PYTHONPATH "${acquire-zarr-py_BINARY_DIR}")
endif ()

set_tests_properties(python_api_test
        PROPERTIES
        LABELS "acquire-zarr-python"
        ENVIRONMENT "PYTHONPATH=${PYTHONPATH};"
        DEPENDS acquire_zarr
)