#include "acquire.zarr.h"
#include "macros.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"

#include <algorithm> // std::clamp, std::max
#include <bit>       // bit_ceil
#include <cstdint>   // uint32_t
#include <unordered_set>
#include <vector>

std::vector<std::string>
get_unique_array_keys(const ZarrStreamSettings* settings)
{
    // caller should have validated settings already
    std::unordered_set<std::string> unique_paths;

    size_t array_count = settings->array_count;

    for (size_t i = 0; i < settings->array_count; ++i) {
        unique_paths.emplace(
          zarr::regularize_key(settings->arrays[i].output_key));
    }

    // duplicate keys?
    if (unique_paths.size() != array_count) {
        LOG_WARNING("Duplicate array output keys found in settings. Expected ",
                    array_count,
                    " unique keys, but found ",
                    unique_paths.size());
    }

    std::vector paths(unique_paths.begin(), unique_paths.end());
    return paths;
}

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
                return "Error compressing";
            case ZarrStatusCode_InvalidSettings:
                return "Invalid settings";
            case ZarrStatusCode_WillNotOverwrite:
                return "Refusing to overwrite existing data";
            case ZarrStatusCode_PartialWrite:
                return "Data partially written";
            case ZarrStatusCode_WriteOutOfBounds:
                return "Attempted write beyond array boundary";
            case ZarrStatusCode_KeyNotFound:
                return "Array key not found";
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

        // frame queue: mirror init_frame_queue_ (256 MiB, clamped to [16, 512]
        // frames sized by the largest array's frame)
        size_t max_frame_size_bytes = 0;
        for (size_t i = 0; i < settings->array_count; ++i) {
            const auto& array = settings->arrays[i];
            const auto& dims = array.dimensions;
            const auto& ndims = array.dimension_count;
            const size_t frame_size_bytes =
              zarr::bytes_of_type(array.data_type) *
              dims[ndims - 2].array_size_px * dims[ndims - 1].array_size_px;
            max_frame_size_bytes =
              std::max(max_frame_size_bytes, frame_size_bytes);
        }

        constexpr uint64_t frame_queue_budget_bytes = 256ULL << 20;
        if (max_frame_size_bytes > 0) {
            const auto frame_count = std::clamp<uint64_t>(
              frame_queue_budget_bytes / max_frame_size_bytes, 16ULL, 512ULL);
            *usage = frame_count * max_frame_size_bytes;
        } else {
            *usage = 0;
        }

        for (size_t i = 0; i < settings->array_count; ++i) {
            const auto& array = settings->arrays[i];
            const auto& dims = array.dimensions;
            const auto& ndims = array.dimension_count;

            const size_t bytes_of_type = zarr::bytes_of_type(array.data_type);
            const size_t frame_size_bytes = bytes_of_type *
                                            dims[ndims - 2].array_size_px *
                                            dims[ndims - 1].array_size_px;

            *usage += frame_size_bytes; // each array has a frame buffer

            // banding bounds memory to one dim-1 band; mirror
            // supports_dim1_banding (no transposition) to stay an upper bound
            // @see czbiohub-sf/livescreen-acquisition#210
            const bool banded = dims[0].chunk_size_px == 1 && ndims >= 4 &&
                                array.storage_dimension_order == nullptr;

            size_t array_usage = bytes_of_type * dims[0].chunk_size_px;
            for (auto j = 1; j < ndims; ++j) {
                const auto& dim = dims[j];

                // arrays may be ragged, so we need to account for fill values
                const auto nchunks = zarr::parts_along_dimension(
                  dim.array_size_px, dim.chunk_size_px);
                size_t padded_array_size_px = nchunks * dim.chunk_size_px;

                if (banded && j == 1) {
                    array_usage *= dim.chunk_size_px;
                } else {
                    array_usage *= padded_array_size_px;
                }
            }

            // compression can instantaneously double memory usage in the worst
            // case, so we account for that here
            if (array.compression_settings) {
                array_usage *= 2;
            }

            if (array.downsampling_method > ZarrDownsamplingMethod_None) {
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

    size_t ZarrStreamSettings_get_array_count(
      const ZarrStreamSettings* settings)
    {
        if (settings == nullptr) {
            LOG_WARNING("Null pointer: settings");
            return 0;
        }

        return get_unique_array_keys(settings).size();
    }

    ZarrStatusCode ZarrStreamSettings_get_array_key(
      const ZarrStreamSettings* settings,
      size_t index,
      char** key)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(key, "Null pointer: key");

        auto unique_keys = get_unique_array_keys(settings);
        if (index > unique_keys.size()) {
            LOG_ERROR(
              "Index out of range: ", index, " >= ", unique_keys.size());
            return ZarrStatusCode_InvalidIndex;
        }

        *key = nullptr;
        const std::string& k = unique_keys[index];

        // round up to next power of 2 for alignment
        const size_t len = std::bit_ceil(k.length() + 1);
        *key = static_cast<char*>(malloc(len * sizeof(char)));
        if (*key == nullptr) {
            LOG_ERROR("Failed to allocate memory for array path");
            return ZarrStatusCode_OutOfMemory;
        }
        memset(*key, 0, len * sizeof(char));
        memcpy(*key, k.c_str(), k.size());
        return ZarrStatusCode_Success;
    }

    ZarrStatusCode ZarrArraySettings_create_dimension_array(
      ZarrArraySettings* settings,
      size_t dimension_count)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(
          dimension_count >= 2, "Invalid dimension count: ", dimension_count);

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

    ZarrStatusCode ZarrStream_close(struct ZarrStream_s* stream)
    {
        if (stream == nullptr) {
            return ZarrStatusCode_Success;
        }

        // an exception must not cross the C boundary, and the stream must be
        // freed either way
        bool ok = false;
        try {
            ok = finalize_stream(stream);
        } catch (const std::exception& e) {
            LOG_ERROR("Error finalizing Zarr stream: ", e.what());
        } catch (...) {
            LOG_ERROR("Unknown error finalizing Zarr stream");
        }
        delete stream;

        return ok ? ZarrStatusCode_Success : ZarrStatusCode_IOError;
    }

    void ZarrStream_destroy(struct ZarrStream_s* stream)
    {
        // Ignore the status: callers that need to detect a failed flush should
        // use ZarrStream_close instead.
        (void)ZarrStream_close(stream);
    }

    ZarrStatusCode ZarrStream_append(struct ZarrStream_s* stream,
                                     const void* data,
                                     size_t bytes_in,
                                     size_t* bytes_out,
                                     const char* key)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        EXPECT_VALID_ARGUMENT(bytes_out, "Null pointer: bytes_out");

        ZarrStatusCode result;
        try {
            result = stream->append(key, data, bytes_in, *bytes_out);
        } catch (const std::exception& e) {
            LOG_ERROR("Error appending data: ", e.what());
            result = ZarrStatusCode_InternalError;
        }

        return result;
    }

    ZarrStatusCode ZarrStream_write_custom_metadata(struct ZarrStream_s* stream,
                                                    const char* node_key,
                                                    const char* metadata_key,
                                                    const char* metadata)
    {
        EXPECT_VALID_ARGUMENT(stream, "Null pointer: stream");
        EXPECT_VALID_ARGUMENT(metadata, "Null pointer: metadata");

        std::optional<std::string> key = std::nullopt;
        if (node_key) {
            key = zarr::regularize_key(node_key);
        }

        const std::string meta_key = metadata_key ? metadata_key : "";

        ZarrStatusCode status;
        try {
            status = stream->write_custom_metadata(key, meta_key, metadata);
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