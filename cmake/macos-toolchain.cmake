if (APPLE)
    # Determine architecture
    if (${CMAKE_HOST_SYSTEM_PROCESSOR} STREQUAL "arm64")
        # Apple Silicon
        set(LIBOMP_PATH "/opt/homebrew/opt/libomp")
        set(CMAKE_OSX_ARCHITECTURES "arm64")
    else ()
        # Intel Mac
        set(LIBOMP_PATH "/usr/local/opt/libomp")
        set(CMAKE_OSX_ARCHITECTURES "x86_64")
    endif ()

    # OpenMP support
    set(OpenMP_C_FLAGS "-Xclang -fopenmp -I${LIBOMP_PATH}/include")
    set(OpenMP_CXX_FLAGS "-Xclang -fopenmp -I${LIBOMP_PATH}/include")
    set(OpenMP_C_LIB_NAMES "omp")
    set(OpenMP_CXX_LIB_NAMES "omp")

    # Use static library
    set(OpenMP_omp_LIBRARY "${LIBOMP_PATH}/lib/libomp.a")

    # Ensure consistent architecture for all libraries
    add_compile_options(-march=native)

    # Add linking flags
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -L${LIBOMP_PATH}/lib")
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -L${LIBOMP_PATH}/lib -lomp")

    # Disable universal binaries as they can cause architecture issues
    set(CMAKE_XCODE_ATTRIBUTE_ONLY_ACTIVE_ARCH "YES")
endif ()