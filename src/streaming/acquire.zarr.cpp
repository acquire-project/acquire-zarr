#include "acquire.zarr.h"
#include "zarr.stream.hh"
#include "macros.hh"

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
}