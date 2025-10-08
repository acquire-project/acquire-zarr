set(project acquire-zarr)

set(tests
        stream-raw-to-filesystem
        stream-named-array-to-filesystem
        stream-compressed-to-filesystem
        stream-2d-multiscale-to-filesystem
        stream-3d-multiscale-to-filesystem
        stream-raw-to-s3
        stream-named-array-to-s3
        stream-compressed-to-s3
        stream-multi-frame-append
        stream-multiscale-trivial-3rd-dim
        stream-multiple-arrays-to-filesystem
        estimate-memory-usage
        stream-pure-hcs-acquisition
        stream-mixed-flat-and-hcs-acquisition
        stream-with-ragged-final-shard
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