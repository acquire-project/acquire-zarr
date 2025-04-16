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
        ZarrStatusCode_Success = 0,
        ZarrStatusCode_InvalidArgument,
        ZarrStatusCode_Overflow,
        ZarrStatusCode_InvalidIndex,
        ZarrStatusCode_NotYetImplemented,
        ZarrStatusCode_InternalError,
        ZarrStatusCode_OutOfMemory,
        ZarrStatusCode_IOError,
        ZarrStatusCode_CompressionError,
        ZarrStatusCode_InvalidSettings,
        ZarrStatusCode_WillNotOverwrite,
        ZarrStatusCodeCount,
    } ZarrStatusCode;

    typedef enum
    {
        ZarrVersion_2 = 2,
        ZarrVersion_3,
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
        ZarrCompressorCount
    } ZarrCompressor;

    typedef enum
    {
        ZarrCompressionCodec_None = 0,
        ZarrCompressionCodec_BloscLZ4,
        ZarrCompressionCodec_BloscZstd,
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
     * @brief S3 settings for streaming to Zarr.
     */
    typedef struct
    {
        const char* endpoint;
        const char* bucket_name;
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
     * @brief Properties of a dimension of the Zarr array.
     */
    typedef struct
    {
        const char* name;       /**< Name of the dimension */
        ZarrDimensionType type; /**< Type of the dimension */
        uint32_t array_size_px; /**< Size of the array along this dimension in
                                       pixels */
        uint32_t chunk_size_px; /**< Size of the chunks along this dimension in
                                       pixels */
        uint32_t shard_size_chunks; /**< Number of chunks in a shard along this
                                       dimension */
    } ZarrDimensionProperties;

    /**
     * @brief Settings for a well field of view.
     */
    typedef struct ZarrWellFieldSettings_s
    {
        const char* path;        /**< Path (name) of the field of view */
        uint32_t acquisition_id; /**< ID of the acquisition for this field */
    } ZarrWellFieldSettings;

    /**
     * @brief Settings for a plate well.
     */
    typedef struct ZarrWellSettings_s
    {
        const char* row_name;          /**< Name of the row, e.g., "A" */
        const char* column_name;       /**< Name of the column, e.g., "1" */
        uint32_t row_index;            /**< Index of the row */
        uint32_t column_index;         /**< Index of the column */
        ZarrWellFieldSettings* fields; /**< Field of view settings */
        size_t field_count;            /**< Number of fields */
    } ZarrWellSettings;

    /**
     * @brief Settings for a plate acquisition.
     */
    typedef struct ZarrAcquisitionSettings_s
    {
        uint32_t id;                  /**< Unique ID for this acquisition */
        const char* name;             /**< Name of the acquisition */
        uint32_t maximum_field_count; /**< Maximum number of fields per well */
        uint64_t start_time;          /**< Start timestamp (epoch) */
        uint64_t end_time;            /**< End timestamp (epoch) */
        const char* description;      /**< Optional description */
    } ZarrAcquisitionSettings;

    /**
     * @brief Settings for a plate.
     */
    typedef struct ZarrPlateSettings_s
    {
        const char* name;     /**< Name of the plate */
        const char* version;  /**< Version of the plate specification */
        uint32_t field_count; /**< Maximum fields across all wells */

        /* Row and column definitions */
        const char** row_names;    /**< Array of row names */
        size_t row_count;          /**< Number of rows */
        const char** column_names; /**< Array of column names */
        size_t column_count;       /**< Number of columns */

        /* Acquisitions */
        ZarrAcquisitionSettings* acquisitions; /**< Array of acquisitions */
        size_t acquisition_count;              /**< Number of acquisitions */

        /* Wells */
        ZarrWellSettings* wells; /**< Array of wells */
        size_t well_count;       /**< Number of wells */
    } ZarrPlateSettings;

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_TYPES_V0