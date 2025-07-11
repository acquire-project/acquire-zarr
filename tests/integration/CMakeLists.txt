set(project acquire-zarr)

set(tests
        stream-zarr-v2-raw-to-filesystem
        stream-zarr-v2-named-array-to-filesystem
        stream-zarr-v2-compressed-to-filesystem
        stream-zarr-v2-raw-to-s3
        stream-zarr-v2-named-array-to-s3
        stream-zarr-v2-compressed-to-s3
        stream-zarr-v3-raw-to-filesystem
        stream-zarr-v3-named-array-to-filesystem
        stream-zarr-v3-compressed-to-filesystem
        stream-zarr-v3-2d-multiscale-to-filesystem
        stream-zarr-v3-3d-multiscale-to-filesystem
        stream-zarr-v3-raw-to-s3
        stream-zarr-v3-named-array-to-s3
        stream-zarr-v3-compressed-to-s3
        stream-zarr-v3-multi-frame-append
        stream-zarr-v3-multiscale-trivial-3rd-dim
        stream-zarr-v3-multiple-arrays-to-filesystem
)

foreach (name ${tests})
    set(tgt "${project}-${name}")
    add_executable(${tgt} ${name}.cpp test.macros.hh)
    target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
    set_target_properties(${tgt} PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
    )
    target_include_directories(${tgt} PRIVATE
            ${PROJECT_SOURCE_DIR}/include
            ${PROJECT_SOURCE_DIR}/src/logger
    )
    target_link_libraries(${tgt} PRIVATE
            acquire-zarr
            nlohmann_json::nlohmann_json
            miniocpp::miniocpp
            Crc32c::crc32c
    )

    add_test(NAME test-${tgt} COMMAND ${tgt})

    set(test_labels "anyplatform;acquire-zarr;integration")

    if (name MATCHES ".*v2.*")
        list(APPEND test_labels "deprecated")
    endif ()

    if (name MATCHES ".*s3.*")
        list(APPEND test_labels "s3")
    endif ()

    set_tests_properties(test-${tgt} PROPERTIES LABELS "${test_labels}")
endforeach ()