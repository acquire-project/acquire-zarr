set(project acquire-zarr)

set(tests
        create-stream
        create-stream-with-metadata
        array-dimensions-chunk-lattice-index
        array-dimensions-tile-group-offset
        array-dimensions-chunk-internal-offset
        array-dimensions-shard-index-for-chunk
        array-dimensions-shard-internal-index
        thread-pool-push-to-job-queue
        make-dirs
        construct-data-paths
        s3-connection-bucket-exists
        s3-connection-object-exists-check-false-positives
        s3-connection-put-object
        s3-connection-upload-multipart-object
        file-sink-write
        s3-sink-write
        s3-sink-write-multipart
        make-data-sinks
        array-write-even
        array-write-ragged-append-dim
        array-write-ragged-internal-dim
        zarr-stream-partial-append
        frame-queue
        downsampler
        downsampler-odd-z
        plate
)

foreach (name ${tests})
    set(tgt "${project}-unit-test-${name}")
    add_executable(${tgt} ${name}.cpp unit.test.macros.hh)
    target_compile_definitions(${tgt} PUBLIC "TEST=\"${tgt}\"")
    set_target_properties(${tgt} PROPERTIES
            MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"
    )
    target_include_directories(${tgt} PRIVATE
            ${PROJECT_SOURCE_DIR}/include
            ${PROJECT_SOURCE_DIR}/src/logger
            ${PROJECT_SOURCE_DIR}/src/streaming
    )
    target_link_libraries(${tgt} PRIVATE
            acquire-zarr
            miniocpp::miniocpp
            Crc32c::crc32c
    )

    add_test(NAME test-${tgt} COMMAND ${tgt})

    set(test_labels "anyplatform;acquire-zarr;unit-tests")

    if (name MATCHES ".*v2.*")
        list(APPEND test_labels "deprecated")
    endif ()

    if (name MATCHES ".*s3.*")
        list(APPEND test_labels "s3")
    endif ()

    set_tests_properties(test-${tgt} PROPERTIES LABELS "${test_labels}")
endforeach ()