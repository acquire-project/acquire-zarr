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
     * @note The store path can be a filesystem path or an S3 key prefix. For example,
     * supplying an endpoint "s3://my-endpoint.com" and a bucket "my-bucket" with a
     * store_path of "my-dataset.zarr" will result in the store being written to
     * "s3://my-endpoint.com/my-bucket/my-dataset.zarr".
     * @note The dimensions array may be allocated with ZarrStreamSettings_create_dimension_array
     * and freed with ZarrStreamSettings_destroy_dimension_array. The order in which you
     * set the dimension properties in the array should match the order of the dimensions
     * from slowest to fastest changing, for example, [Z, Y, X] for a 3D dataset.
     */
    typedef struct ZarrStreamSettings_s
    {
        const char* store_path; /**< Path to the store. Filesystem path or S3 key prefix. */
        const char* custom_metadata; /**< JSON-formatted custom metadata to be stored with the dataset. */
        ZarrS3Settings* s3_settings; /**< Optional S3 settings for the store. */
        ZarrCompressionSettings* compression_settings; /**< Optional chunk compression settings for the store. */
        ZarrDimensionProperties* dimensions; /**< The properties of each dimension in the dataset. */
        size_t dimension_count; /**< The number of dimensions in the dataset. */
        bool multiscale; /**< Whether to stream to multiple levels of detail. */
        ZarrDataType data_type; /**< The pixel data type of the dataset. */
        ZarrVersion version; /**< The version of the Zarr format to use. 2 or 3. */
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
     * @brief Allocate memory for the dimension array in the Zarr stream settings struct.
     * @param[in, out] settings The Zarr stream settings struct.
     * @param dimension_count The number of dimensions in the dataset to allocate memory for.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStreamSettings_create_dimension_array(ZarrStreamSettings* settings, size_t dimension_count);

    /**
     * @brief Free memory for the dimension array in the Zarr stream settings struct.
     * @param[in, out] settings The Zarr stream settings struct containing the dimension array to free.
     */
    void ZarrStreamSettings_destroy_dimension_array(ZarrStreamSettings* settings);

    /**
     * @brief Create a Zarr stream.
     * @param[in, out] settings The settings for the Zarr stream.
     * @return A pointer to the Zarr stream struct, or NULL on failure.
     */
    ZarrStream* ZarrStream_create(ZarrStreamSettings* settings);

    /**
     * @brief Destroy a Zarr stream.
     * @details This function frees the memory allocated for the Zarr stream.
     * @param stream The Zarr stream struct to destroy.
     */
    void ZarrStream_destroy(ZarrStream* stream);

    /**
     * @brief Append data to the Zarr stream.
     * @details This function will block while chunks are compressed and written
     * to the store. It will return when all data has been written.
     * @param[in, out] stream The Zarr stream struct.
     * @param[in] data The data to append.
     * @param[in] bytes_in The number of bytes in @p data. It should be at least
     * the size of a single frame.
     * @param[out] bytes_out The number of bytes written to the stream.
     * @return ZarrStatusCode_Success on success, or an error code on failure.
     */
    ZarrStatusCode ZarrStream_append(ZarrStream* stream,
                                     const void* data,
                                     size_t bytes_in,
                                     size_t* bytes_out);

#ifdef __cplusplus
}
#endif
