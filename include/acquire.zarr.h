#pragma once

#include "zarr.types.h"

#ifdef __cplusplus
extern "C"
{
#endif

    /**
     * @brief The settings for a Zarr stream.
     * @details This struct contains the settings for a Zarr stream, including
     * the store path, custom metadata, S3 settings, chunk compression settings,
     * dimension properties, whether to stream to multiple levels of detail, the
     * pixel data type, and the Zarr format version.
     * @note The store path can be a filesystem path or an S3 key prefix. For
     * example, supplying an endpoint "http://localhost:9000" and a bucket
     * "my-bucket" with a store_path of "my-dataset.zarr" writes objects under
     * the key prefix "my-dataset.zarr/" in "my-bucket", which that endpoint
     * serves at "http://localhost:9000/my-bucket/my-dataset.zarr/...". The
     * request URL depends on the addressing style; see ZarrS3Settings.
     */
    typedef struct ZarrStreamSettings_s
    {
        const char* store_path; /**< Path to the store. Filesystem path or S3
                                   key prefix. */
        ZarrS3Settings* s3_settings; /**< Optional S3 settings for the store. */
        ZarrVersion version;         /**< The version of the Zarr format to use.
                                        Reserved for Zarr v4. */
        unsigned int max_threads; /**< The maximum number of threads to use in
                                     the stream. Set to 0 to use the supported
                                     number of concurrent threads. */
        bool overwrite; /**< Remove everything in store_path if true. */
        ZarrArraySettings*
          arrays; /**< The settings for the Zarr arrays being streamed. */
        size_t array_count; /**< The number of arrays in the Zarr stream. */
        ZarrIntermediateGroups
          intermediate_groups; /**< Whether to write the generic groups
                                  above the arrays. */
    } ZarrStreamSettings;

    typedef struct ZarrStream_s ZarrStream;

    /**
     * @brief Get the version of the Zarr API.
     * @return The library release version, e.g. "0.10.0".
     */
    const char* Zarr_get_api_version();

    /**
     * @brief Set the log level for the Zarr API.
     * @param level The log level.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode Zarr_set_log_level(ZarrLogLevel level);

    /**
     * @brief Get the log level for the Zarr API.
     * @return The log level for the Zarr API.
     */
    ZarrLogLevel Zarr_get_log_level();

    /**
     * @brief Get the message for the given status code.
     * @param code The status code.
     * @return A human-readable status message.
     */
    const char* Zarr_get_status_message(ZarrStatusCode code);

    /**
     * @brief Allocate memory for the ZarrArraySettings array in the Zarr stream
     * settings struct.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param array_count The number of Zarr arrays in the dataset to allocate
     * memory for.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_create_arrays(
      ZarrStreamSettings* settings,
      size_t array_count);

    /**
     * @brief Free memory for the ZarrArraySettings array in the Zarr stream
     * @param[in, out] settings The Zarr stream settings struct containing the
     * ZarrArraySettings array to free.
     */
    void ZarrStreamSettings_destroy_arrays(ZarrStreamSettings* settings);

    /**
     * @brief Estimate the maximum memory usage of the Zarr stream.
     * @param[in] settings The Zarr stream settings struct.
     * @param[out] usage The estimated maximum memory usage in bytes.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_estimate_max_memory_usage(
      const ZarrStreamSettings* settings,
      size_t* usage);

    /**
     * @brief Get the number of arrays configured in the Zarr stream settings.
     * @param settings The Zarr stream settings struct.
     * @return The number of array paths configured in the settings.
     */
    size_t ZarrStreamSettings_get_array_count(
      const ZarrStreamSettings* settings);

    /**
     * @brief Get the @p i th array key configured in the Zarr stream settings.
     * The caller is responsible for freeing the memory allocated for the path
     * in the array of paths. C++ callers should use `free` to free the memory.
     * @param settings  The Zarr stream settings struct.
     * @param index The index .
     * @param[out] key Pointer to a string to be allocated and filled with the
     * @i th array key. The caller is responsible for freeing the memory
     * allocated for @p key.
     * @return ZarrStatusCode_Success on success, ZarrStatusCode_InvalidIndex
     * if the index is out of range, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_get_array_key(
      const ZarrStreamSettings* settings,
      size_t index,
      char** key);

    /**
     * @brief Populate stream settings from a YAML or JSON config file.
     * @details JSON is a subset of YAML, so both formats are accepted. All
     * arrays, dimensions, and strings referenced by @p settings are allocated
     * and owned by the loader; free them with
     * ZarrStreamSettings_destroy_loaded (NOT the piecemeal destroy_* functions).
     * Secrets (e.g. AWS credentials) are never read from the file; provide them
     * via the environment.
     * @param[out] settings The stream settings struct to populate.
     * @param path Path to the config file.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_load_from_file(
      ZarrStreamSettings* settings,
      const char* path);

    /**
     * @brief Populate stream settings from an in-memory YAML or JSON string.
     * @details Ownership matches ZarrStreamSettings_load_from_file; free with
     * ZarrStreamSettings_destroy_loaded.
     * @param[out] settings The stream settings struct to populate.
     * @param text The config document.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_load_from_string(
      ZarrStreamSettings* settings,
      const char* text);

    /**
     * @brief Free settings populated by ZarrStreamSettings_load_from_file or
     * ZarrStreamSettings_load_from_string, including all loader-owned strings,
     * arrays and dimensions.
     * @param[in, out] settings The loader-populated stream settings struct.
     */
    void ZarrStreamSettings_destroy_loaded(ZarrStreamSettings* settings);

    /**
     * @brief Serialize stream settings to a config file.
     * @details The format is chosen by the file extension: `.json` writes JSON,
     * `.yaml`/`.yml` (or anything else) writes YAML.
     * @param settings The stream settings struct to serialize.
     * @param path Destination file path.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_dump_to_file(
      const ZarrStreamSettings* settings,
      const char* path);

    /**
     * @brief Serialize stream settings to a newly allocated config string.
     * @param settings The stream settings struct to serialize.
     * @param[out] text Set to a newly allocated string; the caller frees it
     * (C++ callers use `free`).
     * @param format Output format (YAML or JSON).
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_dump_to_string(
      const ZarrStreamSettings* settings,
      char** text,
      ZarrConfigFormat format);

    /**
     * @brief Allocate memory for the dimension array in the Zarr array settings
     * struct.
     * @param[in, out] settings The Zarr array settings struct.
     * @param dimension_count The number of dimensions in the array to
     * allocate memory for.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrArraySettings_create_dimension_array(
      ZarrArraySettings* settings,
      size_t dimension_count);

    /**
     * @brief Free memory for the dimension array in the Zarr array settings
     * struct.
     * @param[in, out] settings The Zarr array settings struct containing the
     * dimension array to free.
     */
    void ZarrArraySettings_destroy_dimension_array(ZarrArraySettings* settings);

    /**
     * @brief Create a Zarr stream.
     * @param[in, out] settings The settings for the Zarr stream.
     * @return A pointer to the Zarr stream struct, or NULL on failure.
     */
    ZarrStream* ZarrStream_create(ZarrStreamSettings* settings);

    /**
     * @brief Finalize and close a Zarr stream, reporting failures.
     * @details This function waits for all pending writes to complete, flushes
     * all data durably to storage, and frees the memory allocated for the Zarr
     * stream. Unlike ZarrStream_destroy, it returns a status code so callers can
     * detect a failed flush (e.g. an I/O error on a network filesystem) instead
     * of silently producing a corrupt store. The stream is destroyed and the
     * pointer is invalid after this call returns, regardless of the status.
     * @param stream The Zarr stream struct to close.
     * @return ZarrStatusCode_Success on success. Currently any failure, whether
     * an earlier asynchronous write error or an error during finalization, is
     * reported as ZarrStatusCode_IOError.
     */
    ZarrStatusCode ZarrStream_close(ZarrStream* stream);

    /**
     * @brief Destroy a Zarr stream.
     * @details This function waits for all pending writes to complete and frees
     * the memory allocated for the Zarr stream. Any failure during finalization
     * is logged but not reported; prefer ZarrStream_close if you need to detect
     * a failed flush.
     * @param stream The Zarr stream struct to destroy.
     */
    void ZarrStream_destroy(ZarrStream* stream);

    /**
     * @brief Append data to the Zarr stream.
     * @details Copies @p data into the stream's bounded internal frame queue;
     * chunking, compression and I/O happen on the stream's worker threads,
     * never on the calling thread. This function therefore returns once the
     * data has been *accepted*, not once it has been written to the store. It
     * blocks only when the frame queue is full, until the consumer frees a
     * slot. Multiple frames can be appended in a single call.
     *
     * Not thread-safe: a stream expects a single producer. Concurrent calls for
     * the same stream, including calls for different array keys, are undefined.
     * @param[in, out] stream The Zarr stream struct.
     * @param[in] data The data to append. If @p data is NULL, append
     * @p bytes_in zeros instead.
     * @param[in] bytes_in The number of bytes to append, pointed to by @p data,
     * or the number of zeros to fill if @p data is NULL. This can be any
     * nonnegative integer. On a value of 0, this function will immediately
     * return.
     * @param[out] bytes_out The number of bytes accepted by the stream. If
     * @p stream and @p bytes_out are not NULL, this is set on every return,
     * to 0 if the call fails before it accepts any data. Bytes that complete a
     * partial frame are counted before that frame enters the queue, so if the
     * stream stops during the call, @p bytes_out can include up to one frame
     * that was not queued. A trailing part of @p data shorter than one frame is
     * counted and held in a per-array staging buffer; it enters the frame queue
     * only when a later call completes the frame.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     * ZarrStatusCode_PartialWrite, with @p bytes_out less than @p bytes_in,
     * means the stream stopped accepting data during this call. Only an error
     * that the frame-queue thread detects stops the stream this way; a full
     * frame queue blocks instead. If the stop drops only the partial frame
     * that this call completed, the call can still return
     * ZarrStatusCode_Success, and the next call returns
     * ZarrStatusCode_InternalError. A chunk compression or write failure on a
     * pool worker does not stop an append that is in progress, and that append
     * can return ZarrStatusCode_Success. Such a failure is reported as
     * ZarrStatusCode_InternalError by a later call, or as
     * ZarrStatusCode_IOError by ZarrStream_close.
     */
    ZarrStatusCode ZarrStream_append(ZarrStream* stream,
                                     const void* data,
                                     size_t bytes_in,
                                     size_t* bytes_out,
                                     const char* key);

    /**
     * @brief Write custom metadata to a node of the stream.
     * @details The node can be an array, a group that the stream writes (the
     * root, or a group between the root and an array, such as an HCS plate or
     * well), or a level of detail of a multiscale array
     * (`<array_key>/<level>`, e.g. `"plate/A/1/0/1"`). For an array, a level of
     * detail, or a multiscale image group (for example an HCS field of view),
     * the metadata is held in memory and written with the array's metadata, at
     * the latest when the stream is closed. Any other group is written at once,
     * so a reader sees the metadata during the acquisition, and again at close:
     * its whole zarr.json is replaced by the group as the stream wrote or kept
     * it at create, with all of its custom attributes added. A change that
     * anything else made to that group after create is lost. On a filesystem
     * the file is rewritten in place, so a reader that opens it during the
     * write can see an incomplete document, as for array metadata. If the write
     * fails, the call returns ZarrStatusCode_IOError and the stream keeps the
     * group's earlier custom metadata; the file is undefined until a later
     * call or the close writes it again. The stream continues. No group is
     * addressable when `intermediate_groups` is ZarrIntermediateGroups_Never,
     * because the stream does not write the groups.
     * @param stream The Zarr stream struct.
     * @param node_key Key of the node to write the metadata to, relative to
     * the store root. An empty key is the root. If NULL, the stream's only
     * array; NULL is an error if the stream has more than one array.
     * @param metadata_key Key under 'attributes' to write the metadata to. If
     * metadata_key is empty or NULL, metadata will be written directly under
     * 'attributes'. OME-NGFF metadata goes under the 'ome' key, as a JSON
     * object, given either with metadata_key "ome" or as a top-level key of
     * @p metadata. Its keys merge with the 'ome' keys that the stream writes
     * on the node. A later custom 'ome' replaces an earlier one, as for any
     * other attribute. 'ome.multiscales' is refused on every node, and so is
     * any other key that the stream writes under 'ome' on the node ('version'
     * and 'name' on a multiscale group), except 'version' with the same value.
     * A metadata_key that begins with "ome/" is refused: a metadata key is not
     * a path.
     * @param metadata JSON-formatted metadata to be written to the dataset.
     * @return ZarrStatusCode_Success on success, ZarrStatusCode_KeyNotFound if
     * no node has the key, ZarrStatusCode_InvalidArgument if the node is not
     * addressable or the metadata is not valid, ZarrStatusCode_IOError if a
     * group cannot be written, or another error code on failure.
     */
    ZarrStatusCode ZarrStream_write_custom_metadata(ZarrStream* stream,
                                                    const char* node_key,
                                                    const char* metadata_key,
                                                    const char* metadata);

    /**
     * @brief Get the current memory usage of the Zarr stream.
     * @param[in] stream The Zarr stream struct.
     * @param[out] usage The current memory usage in bytes.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStream_get_current_memory_usage(const ZarrStream* stream,
                                                       size_t* usage);

#ifdef __cplusplus
}
#endif
