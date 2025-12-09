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

    typedef enum
    {
        ZarrDownsamplingMethod_Decimate = 0,
        ZarrDownsamplingMethod_Mean,
        ZarrDownsamplingMethod_Min,
        ZarrDownsamplingMethod_Max,
        ZarrDownsamplingMethodCount,
    } ZarrDownsamplingMethod;

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
     * @note The dimensions array may be allocated with ZarrArraySettings_create_dimension_array
     * and freed with ZarrArraySettings_destroy_dimension_array. The order in which you
     * set the dimension properties in the array should match the order of the dimensions
     * during acquisition from slowest to fastest changing, for example, [Z, Y, X] for a 3D dataset.
     * @note To write a transposed target storage order (e.g., for OME-NGFF compliance, which
     * currently requires TCZYX order), set storage_dimension_order to a permutation array
     * specifying the output order. Each element is the index of the acquisition dimension
     * that should appear at that storage position. For example, if acquisition dimensions are
     * [t, z, c, y, x] and you want storage order [t, c, z, y, x], use [0, 2, 1, 3, 4].
     * If storage_dimension_order is NULL, dimensions will be stored in the order provided.
     */
    typedef struct
    {
        const char* output_key;
        ZarrCompressionSettings* compression_settings;
        ZarrDimensionProperties* dimensions;
        size_t dimension_count;
        ZarrDataType data_type;
        bool multiscale;
        ZarrDownsamplingMethod downsampling_method;
        const size_t* storage_dimension_order;
    } ZarrArraySettings;

    /**
     * @brief Settings for a field of view in a high-content screening (HCS)
     * well.
     * @note @p array_settings->output_key must be @p NULL, because the path to
     * the array is fully specified by @p path. Validation will fail if it is
     * non-@p NULL
     */
    typedef struct
    {
        const char* path; /**< Path to the FOV, relative to its parent Well */
        uint32_t acquisition_id; /**< Acquisition ID, if applicable */
        bool has_acquisition_id; /**< Whether acquisition_id is valid */
        ZarrArraySettings* array_settings; /**< Settings for the array */
    } ZarrHCSFieldOfView;

    /**
     * @brief Settings for a well in a high-content screening (HCS) plate.
     */
    typedef struct
    {
        const char* row_name;    /**< Name of the row containing this Well */
        const char* column_name; /**< Name of the column containing this Well */
        ZarrHCSFieldOfView* images; /**< Array of FieldOfView structs */
        size_t image_count;         /**< Number of FieldOfView structs */
    } ZarrHCSWell;

    /**
     * @brief Settings for an acquisition in a high-content screening (HCS)
     * plate.
     */
    typedef struct
    {
        uint32_t id;      /**< Unique identifier for the acquisition */
        const char* name; /**< Name of the acquisition. Can be NULL */
        const char*
          description;       /**< Description of the acquisition. Can be NULL */
        uint64_t start_time; /**< Start time as an epoch timestamp */
        bool has_start_time; /**< Whether start_time is valid */
        uint64_t end_time;   /**< End time as an epoch timestamp */
        bool has_end_time;   /**< Whether end_time is valid */
    } ZarrHCSAcquisition;

    /**
     * @brief Settings for a high-content screening (HCS) plate.
     */
    typedef struct
    {
        const char* path;       /**< Path to the plate, relative to the root */
        const char* name;       /**< Name of the plate */
        const char** row_names; /**< Array of row names */
        size_t row_count;       /**< Number of row names */
        const char** column_names;        /**< Array of column names */
        size_t column_count;              /**< Number of column names */
        ZarrHCSWell* wells;               /**< Array of Well structs */
        size_t well_count;                /**< Number of Well structs */
        ZarrHCSAcquisition* acquisitions; /**< Array of Acquisition structs */
        size_t acquisition_count;         /**< Number of Acquisition structs */
    } ZarrHCSPlate;

    /**
     * @brief Settings for high-content screening (HCS) datasets.
     * @note The plates array may be allocated with ZarrHCSSettings_create_plate_array
     * and freed with ZarrHCSSettings_destroy_plate_array.
     */
    typedef struct
    {
        ZarrHCSPlate* plates; /**< Array of Plate structs */
        size_t plate_count;   /**< Number of Plate structs */
    } ZarrHCSSettings;

#ifdef __cplusplus
}
#endif

#endif // H_ACQUIRE_ZARR_TYPES_V0