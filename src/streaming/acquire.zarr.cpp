#include "acquire.zarr.h"
#include "macros.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"

#include <cstdint> // uint32_t

extern "C"
{
    const char* Zarr_get_api_version()
    {
        return ACQUIRE_ZARR_API_VERSION;
    }

    ZarrStatusCode Zarr_set_log_level(ZarrLogLevel level_)
    {
        LogLevel level;
        switch (level_) {
            case ZarrLogLevel_Debug:
                level = LogLevel_Debug;
                break;
            case ZarrLogLevel_Info:
                level = LogLevel_Info;
                break;
            case ZarrLogLevel_Warning:
                level = LogLevel_Warning;
                break;
            case ZarrLogLevel_Error:
                level = LogLevel_Error;
                break;
            case ZarrLogLevel_None:
                level = LogLevel_None;
                break;
            default:
                return ZarrStatusCode_InvalidArgument;
        }

        try {
            Logger::set_log_level(level);
        } catch (const std::exception& e) {
            LOG_ERROR("Error setting log level: ", e.what());
            return ZarrStatusCode_InternalError;
        }
        return ZarrStatusCode_Success;
    }

    ZarrLogLevel Zarr_get_log_level()
    {
        ZarrLogLevel level;
        switch (Logger::get_log_level()) {
            case LogLevel_Debug:
                level = ZarrLogLevel_Debug;
                break;
            case LogLevel_Info:
                level = ZarrLogLevel_Info;
                break;
            case LogLevel_Warning:
                level = ZarrLogLevel_Warning;
                break;
            case LogLevel_Error:
                level = ZarrLogLevel_Error;
                break;
            case LogLevel_None:
                level = ZarrLogLevel_None;
                break;
        }
        return level;
    }

    const char* Zarr_get_status_message(ZarrStatusCode code)
    {
        switch (code) {
            case ZarrStatusCode_Success:
                return "Success";
            case ZarrStatusCode_InvalidArgument:
                return "Invalid argument";
            case ZarrStatusCode_Overflow:
                return "Buffer overflow";
            case ZarrStatusCode_InvalidIndex:
                return "Invalid index";
            case ZarrStatusCode_NotYetImplemented:
                return "Not yet implemented";
            case ZarrStatusCode_InternalError:
                return "Internal error";
            case ZarrStatusCode_OutOfMemory:
                return "Out of memory";
            case ZarrStatusCode_IOError:
                return "I/O error";
            case ZarrStatusCode_CompressionError:
                return "Compression error";
            case ZarrStatusCode_InvalidSettings:
                return "Invalid settings";
            case ZarrStatusCode_WillNotOverwrite:
                return "Will not overwrite existing data";
            default:
                return "Unknown error";
        }
    }

    ZarrStatusCode ZarrStreamSettings_create_arrays(
      ZarrStreamSettings* settings,
      size_t array_count)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");

        ZarrArraySettings* arrays = nullptr;

        try {
            arrays = new ZarrArraySettings[array_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for arrays");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrStreamSettings_destroy_arrays(settings);
        memset(arrays, 0, sizeof(ZarrArraySettings) * array_count);
        settings->arrays = arrays;
        settings->array_count = array_count;

        return ZarrStatusCode_Success;
    }

    void ZarrStreamSettings_destroy_arrays(ZarrStreamSettings* settings)
    {
        if (settings == nullptr) {
            return;
        }

        if (settings->arrays == nullptr) {
            settings->array_count = 0;
            return;
        }

        // destroy dimension arrays for each ZarrArraySettings
        for (auto i = 0; i < settings->array_count; ++i) {
            ZarrArraySettings_destroy_dimension_array(&settings->arrays[i]);
        }
        delete[] settings->arrays;
        settings->arrays = nullptr;
        settings->array_count = 0;
    }

    ZarrStatusCode ZarrStreamSettings_estimate_max_memory_usage(
      const ZarrStreamSettings* settings,
      size_t* usage)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(settings->arrays,
                              "Null pointer: settings->arrays");

        for (auto i = 1; i < settings->array_count; ++i) {
            EXPECT_VALID_ARGUMENT(
              settings->arrays + i, "Null pointer: settings for array ", i);
            for (auto j = 0; j < settings->arrays[i].dimension_count; ++j) {
                EXPECT_VALID_ARGUMENT(settings->arrays[i].dimensions + j,
                                      "Null pointer: dimension ",
                                      j,
                                      " for array ",
                                      i);
            }
        }

        EXPECT_VALID_ARGUMENT(usage, "Null pointer: usage");

        *usage = (1 << 30); // start with 1 GiB for the frame queue

        for (size_t i = 0; i < settings->array_count; ++i) {
            const auto& array = settings->arrays[i];
            const auto& dims = array.dimensions;
            const auto& ndims = array.dimension_count;

            const size_t bytes_of_type = zarr::bytes_of_type(array.data_type);
            const size_t frame_size_bytes = bytes_of_type *
                                            dims[ndims - 2].array_size_px *
                                            dims[ndims - 1].array_size_px;

            *usage += frame_size_bytes; // each array has a frame buffer

            size_t array_usage = bytes_of_type * dims[0].chunk_size_px;
            for (auto j = 1; j < ndims; ++j) {
                const auto& dim = dims[j];

                // arrays may be ragged, so we need to account for fill values
                const auto nchunks = zarr::parts_along_dimension(
                  dim.array_size_px, dim.chunk_size_px);
                size_t padded_array_size_px = nchunks * dim.chunk_size_px;

                array_usage *= padded_array_size_px;
            }

            // compression can instantaneously double memory usage in the worst
            // case, so we account for that here
            if (array.compression_settings) {
                array_usage *= 2;
            }

            if (array.multiscale) {
                // we can bound the memory usage of multiscale arrays by
                // observing that each downsampled level is at most half the
                // size of the previous level, so the total memory usage is at
                // most twice the size of the full-resolution, i.e.,
                // sum(1/2^n)_{n=0}^{inf} = 2
                array_usage *= 2;
            }

            *usage += array_usage;
        }

        return ZarrStatusCode_Success;
    }

    ZarrStatusCode ZarrArraySettings_create_dimension_array(
      ZarrArraySettings* settings,
      size_t dimension_count)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(
          dimension_count >= 3, "Invalid dimension count: ", dimension_count);

        ZarrDimensionProperties* dimensions = nullptr;

        try {
            dimensions = new ZarrDimensionProperties[dimension_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for dimensions");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrArraySettings_destroy_dimension_array(settings);
        settings->dimensions = dimensions;
        settings->dimension_count = dimension_count;

        return ZarrStatusCode_Success;
    }

    void ZarrArraySettings_destroy_dimension_array(ZarrArraySettings* settings)
    {
        if (settings == nullptr) {
            return;
        }

        if (settings->dimensions != nullptr) {
            delete[] settings->dimensions;
            settings->dimensions = nullptr;
        }
        settings->dimension_count = 0;
    }

    ZarrStatusCode ZarrHCSWell_create_image_array(ZarrHCSWell* well,
                                                  size_t image_count)
    {
        EXPECT_VALID_ARGUMENT(well, "Null pointer: well");
        EXPECT_VALID_ARGUMENT(
          image_count > 0, "Invalid image count: ", image_count);

        ZarrHCSFieldOfView* images = nullptr;

        try {
            images = new ZarrHCSFieldOfView[image_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for images");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrHCSWell_destroy_image_array(well);
        memset(images, 0, sizeof(ZarrHCSFieldOfView) * image_count);
        well->images = images;
        well->image_count = image_count;

        return ZarrStatusCode_Success;
    }

    void ZarrHCSWell_destroy_image_array(ZarrHCSWell* well)
    {
        if (well == nullptr) {
            return;
        }

        if (well->images != nullptr) {
            // destroy array settings for each image
            for (auto i = 0; i < well->image_count; ++i) {
                ZarrArraySettings_destroy_dimension_array(
                  well->images[i].array_settings);
                well->images[i].array_settings = nullptr;
            }
            delete[] well->images;
            well->images = nullptr;
        }
        well->image_count = 0;
    }

    ZarrStatusCode ZarrHCSPlate_create_well_array(ZarrHCSPlate* plate,
                                                  size_t well_count)
    {
        EXPECT_VALID_ARGUMENT(plate, "Null pointer: plate");
        EXPECT_VALID_ARGUMENT(
          well_count > 0, "Invalid well count: ", well_count);

        ZarrHCSWell* wells = nullptr;

        try {
            wells = new ZarrHCSWell[well_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for wells");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrHCSPlate_destroy_well_array(plate);
        memset(wells, 0, sizeof(ZarrHCSWell) * well_count);
        plate->wells = wells;
        plate->well_count = well_count;

        return ZarrStatusCode_Success;
    }

    void ZarrHCSPlate_destroy_well_array(ZarrHCSPlate* plate)
    {
        if (plate == nullptr) {
            return;
        }

        if (plate->wells != nullptr) {
            // destroy image arrays for each well
            for (auto i = 0; i < plate->well_count; ++i) {
                ZarrHCSWell_destroy_image_array(plate->wells + i);
            }
            delete[] plate->wells;
            plate->wells = nullptr;
        }
        plate->well_count = 0;
    }

    ZarrStatusCode ZarrHCSPlate_create_acquisition_array(
      ZarrHCSPlate* plate,
      size_t acquisition_count)
    {
        EXPECT_VALID_ARGUMENT(plate, "Null pointer: plate");
        EXPECT_VALID_ARGUMENT(acquisition_count > 0,
                              "Invalid acquisition count: ",
                              acquisition_count);

        ZarrHCSAcquisition* acquisitions = nullptr;

        try {
            acquisitions = new ZarrHCSAcquisition[acquisition_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for acquisitions");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrHCSPlate_destroy_acquisition_array(plate);
        memset(acquisitions, 0, sizeof(ZarrHCSAcquisition) * acquisition_count);
        plate->acquisitions = acquisitions;
        plate->acquisition_count = acquisition_count;

        return ZarrStatusCode_Success;
    }

    void ZarrHCSPlate_destroy_acquisition_array(ZarrHCSPlate* plate)
    {
        if (plate == nullptr) {
            return;
        }

        if (plate->acquisitions != nullptr) {
            delete[] plate->acquisitions;
            plate->acquisitions = nullptr;
        }
        plate->acquisition_count = 0;
    }

    ZarrStatusCode ZarrHCSPlate_create_row_name_array(ZarrHCSPlate* plate,
                                                      size_t row_count)
    {
        EXPECT_VALID_ARGUMENT(plate, "Null pointer: plate");
        EXPECT_VALID_ARGUMENT(row_count > 0, "Invalid row count: ", row_count);

        const char** row_names = nullptr;

        try {
            row_names = new const char*[row_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for row names");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrHCSPlate_destroy_row_name_array(plate);
        memset(row_names, 0, sizeof(char*) * row_count);
        plate->row_names = row_names;
        plate->row_count = row_count;

        return ZarrStatusCode_Success;
    }

    void ZarrHCSPlate_destroy_row_name_array(ZarrHCSPlate* plate)
    {
        if (plate == nullptr) {
            return;
        }

        if (plate->row_names != nullptr) {
            delete[] plate->row_names;
            plate->row_names = nullptr;
        }
        plate->row_count = 0;
    }

    ZarrStatusCode ZarrHCSPlate_create_column_name_array(ZarrHCSPlate* plate,
                                                      size_t column_count)
    {
        EXPECT_VALID_ARGUMENT(plate, "Null pointer: plate");
        EXPECT_VALID_ARGUMENT(column_count > 0, "Invalid column count: ", column_count);

        const char** column_names = nullptr;

        try {
            column_names = new const char*[column_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for column names");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrHCSPlate_destroy_column_name_array(plate);
        memset(column_names, 0, sizeof(char*) * column_count);
        plate->column_names = column_names;
        plate->column_count = column_count;

        return ZarrStatusCode_Success;
    }

    void ZarrHCSPlate_destroy_column_name_array(ZarrHCSPlate* plate)
    {
        if (plate == nullptr) {
            return;
        }

        if (plate->column_names != nullptr) {
            delete[] plate->column_names;
            plate->column_names = nullptr;
        }
        plate->column_count = 0;
    }

    ZarrStatusCode ZarrHCSSettings_create_plate_array(ZarrHCSSettings* settings,
                                                      size_t plate_count)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(
          plate_count > 0, "Invalid plate count: ", plate_count);

        ZarrHCSPlate* plates = nullptr;

        try {
            plates = new ZarrHCSPlate[plate_count];
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for plates");
            return ZarrStatusCode_OutOfMemory;
        }

        ZarrHCSSettings_destroy_plate_array(settings);
        memset(plates, 0, sizeof(ZarrHCSPlate) * plate_count);
        settings->plates = plates;
        settings->plate_count = plate_count;

        return ZarrStatusCode_Success;
    }

    void ZarrHCSSettings_destroy_plate_array(ZarrHCSSettings* settings)
    {
        if (settings == nullptr) {
            return;
        }

        if (settings->plates != nullptr) {
            // destroy well and acquisition arrays for each plate
            for (auto i = 0; i < settings->plate_count; ++i) {
                ZarrHCSPlate_destroy_well_array(settings->plates + i);
                ZarrHCSPlate_destroy_acquisition_array(settings->plates + i);
                ZarrHCSPlate_destroy_row_name_array(settings->plates + i);
                ZarrHCSPlate_destroy_column_name_array(settings->plates + i);
            }
            delete[] settings->plates;
            settings->plates = nullptr;
        }
        settings->plate_count = 0;
    }

    ZarrStream_s* ZarrStream_create(struct ZarrStreamSettings_s* settings)
    {

        ZarrStream_s* stream = nullptr;

        try {
            stream = new ZarrStream_s(settings);
        } catch (const std::bad_alloc&) {
            LOG_ERROR("Failed to allocate memory for Zarr stream");
        } catch (const std::exception& e) {
            LOG_ERROR("Error creating Zarr stream: ", e.what());
        }

        return stream;
    }

    void ZarrStream_destroy(struct ZarrStream_s* stream)
    {
        if (!finalize_stream(stream)) {
            return;
        }

        delete stream;
    }

    ZarrStatusCode ZarrStream_append(struct ZarrStream_s* stream,
                                     const void* data,
                                     size_t bytes_in,
                                     size_t* bytes_out,
                                     const char* key)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        EXPECT_VALID_ARGUMENT(data, "Null pointer: data");
        EXPECT_VALID_ARGUMENT(bytes_out, "Null pointer: bytes_out");

        // TODO (aliddell): check key first, return a specialized error code if
        // it is invalid

        try {
            *bytes_out = stream->append(key, data, bytes_in);
        } catch (const std::exception& e) {
            LOG_ERROR("Error appending data: ", e.what());
            return ZarrStatusCode_InternalError;
        }

        return ZarrStatusCode_Success;
    }

    ZarrStatusCode ZarrStream_write_custom_metadata(struct ZarrStream_s* stream,
                                                    const char* custom_metadata,
                                                    bool overwrite)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");

        ZarrStatusCode status;
        try {
            status = stream->write_custom_metadata(custom_metadata, overwrite);
        } catch (const std::exception& e) {
            LOG_ERROR("Error writing metadata: ", e.what());
            status = ZarrStatusCode_InternalError;
        }

        return status;
    }

    ZarrStatusCode ZarrStream_get_current_memory_usage(const ZarrStream* stream,
                                                       size_t* usage)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        EXPECT_VALID_ARGUMENT(usage, "Null pointer: usage");

        try {
            *usage = stream->get_memory_usage();
        } catch (const std::exception& e) {
            LOG_ERROR("Error getting memory usage: ", e.what());
            return ZarrStatusCode_InternalError;
        }

        return ZarrStatusCode_Success;
    }
}