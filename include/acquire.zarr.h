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
     * example, supplying an endpoint "s3://my-endpoint.com" and a bucket
     * "my-bucket" with a store_path of "my-dataset.zarr" will result in the
     * store being written to "s3://my-endpoint.com/my-bucket/my-dataset.zarr".
     */
    typedef struct ZarrStreamSettings_s
    {
        const char* store_path; /**< Path to the store. Filesystem path or S3
                                   key prefix. */
        ZarrS3Settings* s3_settings; /**< Optional S3 settings for the store. */
        ZarrVersion
          version; /**< The version of the Zarr format to use. 2 or 3. */
        unsigned int max_threads; /**< The maximum number of threads to use in
                                     the stream. Set to 0 to use the supported
                                     number of concurrent threads. */
        bool overwrite; /**< Remove everything in store_path if true. */
        ZarrArraySettings* arrays; /**< The settings for the Zarr arrays being streamed. */
        size_t array_count; /**< The number of arrays in the Zarr stream. */
    } ZarrStreamSettings;

    typedef struct ZarrStream_s ZarrStream;

    /**
     * @brief Get the version of the Zarr API.
     * @return Semver formatted version of the Zarr API.
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
     * @brief Destroy a Zarr stream.
     * @details This function waits for all pending writes to complete and frees
     * the memory allocated for the Zarr stream.
     * @param stream The Zarr stream struct to destroy.
     */
    void ZarrStream_destroy(ZarrStream* stream);

    /**
     * @brief Append data to the Zarr stream.
     * @details This function will block while chunks are compressed and written
     * to the store. It will return when all data has been written. Multiple
     * frames can be appended in a single call.
     * @param[in, out] stream The Zarr stream struct.
     * @param[in] data The data to append.
     * @param[in] bytes_in The number of bytes in @p data. This can be any
     * nonnegative integer. On a value of 0, this function will immediately
     * return.
     * @param[out] bytes_out The number of bytes written to the stream.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStream_append(ZarrStream* stream,
                                     const void* data,
                                     size_t bytes_in,
                                     size_t* bytes_out,
                                     const char* key);

    /**
     * @brief Write custom metadata to the Zarr stream.
     * @param stream The Zarr stream struct.
     * @param custom_metadata JSON-formatted custom metadata to be written to
     * the dataset.
     * @param overwrite If true, overwrite any existing custom metadata.
     * Otherwise, if custom_metadata is not empty and the stream has already
     * written custom metadata, this function will return an error.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStream_write_custom_metadata(ZarrStream* stream,
                                                    const char* custom_metadata,
                                                    bool overwrite);

#ifdef __cplusplus
}
#endif
