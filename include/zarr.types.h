#ifndef H_ACQUIRE_ZARR_TYPES_V0
#define H_ACQUIRE_ZARR_TYPES_V0

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif

    typedef enum
    {
        ZarrStatusCode_Success = 0,       // Success
        ZarrStatusCode_InvalidArgument,   // Invalid argument
        ZarrStatusCode_Overflow,          // Buffer overflow
        ZarrStatusCode_InvalidIndex,      // Invalid index
        ZarrStatusCode_NotYetImplemented, // Not yet implemented
        ZarrStatusCode_InternalError,     // Internal error
        ZarrStatusCode_OutOfMemory,       // Out of memory
        ZarrStatusCode_IOError,           // I/O error
        ZarrStatusCode_CompressionError,  // Error compressing
        ZarrStatusCode_InvalidSettings,   // Invalid settings
        ZarrStatusCode_WillNotOverwrite,  // Refusing to overwrite existing data
        ZarrStatusCode_PartialWrite,      // Data partially written
        ZarrStatusCode_WriteOutOfBounds,  // Attempted write beyond array
                                          // boundary
        ZarrStatusCode_KeyNotFound,       // Array key not found
        ZarrStatusCodeCount,              // Status unknown
    } ZarrStatusCode;

    typedef enum
    {
        ZarrVersion_3 = 3,
        ZarrVersionCount
    } ZarrVersion;

    typedef enum
    {
        ZarrLogLevel_Debug = 0,
        ZarrLogLevel_Info,
        ZarrLogLevel_Warning,
        ZarrLogLevel_Error,
        ZarrLogLevel_None,
        ZarrLogLevelCount
    } ZarrLogLevel;

    typedef enum
    {
        ZarrDataType_uint8 = 0,
        ZarrDataType_uint16,
        ZarrDataType_uint32,
        ZarrDataType_uint64,
        ZarrDataType_int8,
        ZarrDataType_int16,
        ZarrDataType_int32,
        ZarrDataType_int64,
        ZarrDataType_float32,
        ZarrDataType_float64,
        ZarrDataTypeCount
    } ZarrDataType;

    typedef enum
    {
        ZarrCompressor_None = 0,
        ZarrCompressor_Blosc1,
        ZarrCompressor_Zstd,
        ZarrCompressorCount
    } ZarrCompressor;

    typedef enum
    {
        ZarrCompressionCodec_None = 0,
        ZarrCompressionCodec_BloscLZ4,
        ZarrCompressionCodec_BloscZstd,
        ZarrCompressionCodec_Zstd,
        ZarrCompressionCodecCount
    } ZarrCompressionCodec;

    typedef enum
    {
        ZarrDimensionType_Space = 0,
        ZarrDimensionType_Channel,
        ZarrDimensionType_Time,
        ZarrDimensionType_Other,
        ZarrDimensionTypeCount
    } ZarrDimensionType;

    /**
     * @brief Method used to downsample frames when building an image
     *        pyramid.
     * @note `ZarrDownsamplingMethod_None` is the zero value, so a zeroed
     *       `ZarrArraySettings` requests no image pyramid.
     */
    typedef enum
    {
        ZarrDownsamplingMethod_None = 0, /**< No downsampling. When used with
                                          * is_ngff=true, produces a single-level
                                          * OME-NGFF multiscales group. */
        ZarrDownsamplingMethod_Decimate, /**< Decimate by taking the top-left
                                          * pixel of each 2x2 (2D) or 2x2x2 (3D)
                                          * region. */
        ZarrDownsamplingMethod_Mean, /**< Average all pixels in each 2x2 (2D)
                                      *   or 2x2x2 (3D) region. Integer types
                                      *   truncate the mean toward zero. In
                                      *   3D, the 2x2 mean of each plane is
                                      *   truncated before the two planes are
                                      *   averaged. */
        ZarrDownsamplingMethod_Min,  /**< Take the minimum pixel value in each
                                      *   2x2 (2D) or 2x2x2 (3D) region. */
        ZarrDownsamplingMethod_Max,  /**< Take the maximum pixel value in each
                                      *   2x2 (2D) or 2x2x2 (3D) region. */
        ZarrDownsamplingMethodCount, /**< Sentinel value, do not use. */
    } ZarrDownsamplingMethod;

    /**
     * @brief Whether the stream writes `zarr.json` for the groups above its
     *        arrays.
     * @details This applies to every group between the store root and an
     *          array, HCS plate and well groups included. The stream writes
     *          these groups when it is created. It writes a group again only
     *          if the caller gives it attributes.
     * @note `ZarrIntermediateGroups_IfMissing` is the zero value, so a zeroed
     *       `ZarrStreamSettings` keeps metadata that the caller wrote.
     */
    typedef enum
    {
        ZarrIntermediateGroups_IfMissing = 0, /**< Write a group's `zarr.json`
                                               * unless a valid Zarr v3 group
                                               * node is already there. Clear
                                               * the consolidated metadata of a
                                               * kept group. When overwrite is
                                               * set, write it always. */
        ZarrIntermediateGroups_Always, /**< Always write a group's `zarr.json`,
                                        * and replace what is there. */
        ZarrIntermediateGroups_Never,  /**< Never write a group's `zarr.json`.
                                        * The caller writes the hierarchy. */
        ZarrIntermediateGroupsCount,   /**< Sentinel value, do not use. */
    } ZarrIntermediateGroups;

    /**
     * @brief Serialization format for stream settings config files.
     */
    typedef enum
    {
        ZarrConfigFormat_Yaml = 0,
        ZarrConfigFormat_Json,
        ZarrConfigFormatCount,
    } ZarrConfigFormat;

    /**
     * @brief S3 settings for streaming to Zarr.
     * @details Credentials are never taken from here; they come from the
     * environment or an AWS credentials file.
     */
    typedef struct
    {
        /** Endpoint URL, which must begin with http:// or https://. There is
         * no default scheme. The scheme and host are matched
         * case-insensitively, and any path, query or fragment is ignored. */
        const char* endpoint;
        /** Destination bucket, which must already exist. Requests use
         * virtual-host addressing for an `*.amazonaws.com` endpoint whose
         * bucket name is a legal DNS label, and path-style addressing
         * otherwise; a name containing a dot, an underscore or an upper-case
         * letter therefore falls back to path style even on AWS. */
        const char* bucket_name;
        /** Region to sign requests for, or NULL. NULL is signed as
         * "us-east-1"; endpoints that validate the signing region require this
         * to be set explicitly to match the server. */
        const char* region;
    } ZarrS3Settings;

    /**
     * @brief Compression settings for a Zarr array.
     * @detail The compressor is not the same as the codec. A codec is
     * a specific implementation of a compression algorithm, while a compressor
     * is a library that implements one or more codecs.
     */
    typedef struct
    {
        ZarrCompressor compressor;  /**< Compressor to use */
        ZarrCompressionCodec codec; /**< Codec to use */
        uint8_t level;              /**< Compression level */
        uint8_t shuffle; /**< Whether to shuffle the data before compressing */
    } ZarrCompressionSettings;

    /**
     * @brief Properties of a dimension of a Zarr array.
     */
    typedef struct
    {
        const char* name;           /**< Name of the dimension */
        ZarrDimensionType type;     /**< Type of the dimension */
        uint32_t array_size_px;     /**< Size of the array along this dimension
                                         in pixels */
        uint32_t chunk_size_px;     /**< Size of the chunks along this dimension
                                         in pixels */
        uint32_t shard_size_chunks; /**< Number of chunks in a shard along this
                                         dimension */
        const char* unit;           /** Unit of the dimension */
        double scale;               /**< Scale of the dimension */
    } ZarrDimensionProperties;

    /**
     * @brief Properties of a Zarr array.
     * @note The dimensions array may be allocated with
     * ZarrArraySettings_create_dimension_array and freed with
     * ZarrArraySettings_destroy_dimension_array. The order in which you set the
     * dimension properties in the array should match the order of the
     * dimensions during acquisition from slowest to fastest changing, for
     * example, [Z, Y, X] for a 3D dataset.
     * @note To write a transposed target storage order (e.g., for OME-NGFF
     * compliance, which currently requires TCZYX order), set
     * storage_dimension_order to a permutation array specifying the output
     * order. Each element is the index of the acquisition dimension that should
     * appear at that storage position. For example, if acquisition dimensions
     * are [t, z, c, y, x] and you want storage order [t, c, z, y, x], use [0,
     * 2, 1, 3, 4]. If storage_dimension_order is NULL, dimensions will be
     * stored in the order provided.
     */
    typedef struct
    {
        const char* output_key;
        ZarrCompressionSettings* compression_settings;
        ZarrDimensionProperties* dimensions;
        size_t dimension_count;
        ZarrDataType data_type;
        ZarrDownsamplingMethod downsampling_method;
        uint32_t max_levels; /**< Maximum number of downsampled levels in the
                                  pyramid. 0 means no limit. */
        const size_t* storage_dimension_order;
        bool is_ngff;
    } ZarrArraySettings;

    /**
     * @brief Attributes that ZarrStream_create writes to a node, as if by
     * ZarrStream_write_attributes, before it returns.
     * @details A config file can give these entries, so that a stream that
     * needs attributes (for example the plate and well metadata of an HCS
     * dataset) can be described in one file.
     */
    typedef struct
    {
        const char* node_key; /**< Key of the node, relative to the store
                                   root. NULL is the stream's only array. */
        const char* key;      /**< Key under 'attributes', or NULL to write
                                   each key of @p value there. */
        const char* value;    /**< The value, as JSON. */
    } ZarrAttributes;

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_TYPES_V0