if (APPLE)
    if (${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "arm64")
        # Apple Silicon
        set(LIBOMP_PATH "/opt/homebrew/opt/libomp")
    else ()
        # Intel Mac
        set(LIBOMP_PATH "/usr/local/opt/libomp")
    endif ()

    # OpenMP support
    set(OpenMP_C_FLAGS "-Xclang -fopenmp -I${LIBOMP_PATH}/include")
    set(OpenMP_CXX_FLAGS "-Xclang -fopenmp -I${LIBOMP_PATH}/include")
    set(OpenMP_C_LIB_NAMES "omp")
    set(OpenMP_CXX_LIB_NAMES "omp")
    set(OpenMP_omp_LIBRARY "${LIBOMP_PATH}/lib/libomp.dylib")
endif ()
