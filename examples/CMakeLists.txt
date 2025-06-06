set(project acquire-zarr)

set(examples
        zarrv3-raw-filesystem
        zarrv3-compressed-filesystem
        zarrv3-raw-multiscale-filesystem
        zarrv3-raw-s3
        zarrv3-compressed-s3
        zarrv3-compressed-multiscale-s3
)

foreach (name ${examples})
    set(tgt "${project}-${name}")
    add_executable(${tgt} ${name}.c)
    set_target_properties(${tgt} PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
    )
    target_include_directories(${tgt} PRIVATE
            ${PROJECT_SOURCE_DIR}/include
    )
    target_link_libraries(${tgt} PRIVATE
            acquire-zarr
            nlohmann_json::nlohmann_json
            miniocpp::miniocpp
            Crc32c::crc32c
    )

    add_test(NAME example-${tgt} COMMAND ${tgt})

    set(test_labels "examples")
    if (name MATCHES ".*s3.*")
        list(APPEND test_labels "s3")
    endif ()

    set_tests_properties(example-${tgt} PROPERTIES LABELS "${test_labels}")
endforeach ()