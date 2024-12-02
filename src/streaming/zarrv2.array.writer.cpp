#include "macros.hh"
#include "zarrv2.array.writer.hh"
#include "sink.creator.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>

#include <latch>
#include <stdexcept>

namespace {
[[nodiscard]]
bool
sample_type_to_dtype(ZarrDataType t, std::string& t_str)

{
    const std::string dtype_prefix =
      std::endian::native == std::endian::big ? ">" : "<";

    switch (t) {
        case ZarrDataType_uint8:
            t_str = dtype_prefix + "u1";
            break;
        case ZarrDataType_uint16:
            t_str = dtype_prefix + "u2";
            break;
        case ZarrDataType_uint32:
            t_str = dtype_prefix + "u4";
            break;
        case ZarrDataType_uint64:
            t_str = dtype_prefix + "u8";
            break;
        case ZarrDataType_int8:
            t_str = dtype_prefix + "i1";
            break;
        case ZarrDataType_int16:
            t_str = dtype_prefix + "i2";
            break;
        case ZarrDataType_int32:
            t_str = dtype_prefix + "i4";
            break;
        case ZarrDataType_int64:
            t_str = dtype_prefix + "i8";
            break;
        case ZarrDataType_float32:
            t_str = dtype_prefix + "f4";
            break;
        case ZarrDataType_float64:
            t_str = dtype_prefix + "f8";
            break;
        default:
            LOG_ERROR("Unsupported sample type: ", t);
            return false;
    }

    return true;
}
} // namespace

zarr::ZarrV2ArrayWriter::ZarrV2ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool)
  : ArrayWriter(config, thread_pool)
{
}

zarr::ZarrV2ArrayWriter::ZarrV2ArrayWriter(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayWriter(config, thread_pool, s3_connection_pool)
{
}

std::string
zarr::ZarrV2ArrayWriter::data_root_() const
{
    return config_.store_path + "/" + std::to_string(config_.level_of_detail) +
           "/" + std::to_string(append_chunk_index_);
}

std::string
zarr::ZarrV2ArrayWriter::metadata_path_() const
{
    return config_.store_path + "/" + std::to_string(config_.level_of_detail) +
           "/.zarray";
}

PartsAlongDimensionFun
zarr::ZarrV2ArrayWriter::parts_along_dimension_() const
{
    return chunks_along_dimension;
}

bool
zarr::ZarrV2ArrayWriter::flush_impl_()
{
    // create chunk files
    CHECK(data_sinks_.empty());
    if (!make_data_sinks_()) {
        return false;
    }

    CHECK(data_sinks_.size() == chunk_buffers_.size());

    std::latch latch(chunk_buffers_.size());
    {
        std::scoped_lock lock(buffers_mutex_);
        for (auto i = 0; i < data_sinks_.size(); ++i) {
            auto& chunk = chunk_buffers_.at(i);
            EXPECT(thread_pool_->push_job(
                     std::move([&sink = data_sinks_.at(i),
                                data_ = chunk.data(),
                                size = chunk.size(),
                                &latch](std::string& err) -> bool {
                         bool success = false;
                         try {
                             std::span data{
                                 reinterpret_cast<std::byte*>(data_), size
                             };
                             CHECK(sink->write(0, data));
                             success = true;
                         } catch (const std::exception& exc) {
                             err = "Failed to write chunk: " +
                                   std::string(exc.what());
                         }

                         latch.count_down();
                         return success;
                     })),
                   "Failed to push job to thread pool");
        }
    }

    // wait for all threads to finish
    latch.wait();

    return true;
}

bool
zarr::ZarrV2ArrayWriter::write_array_metadata_()
{
    if (!make_metadata_sink_()) {
        return false;
    }

    using json = nlohmann::json;

    std::string dtype;
    if (!sample_type_to_dtype(config_.dtype, dtype)) {
        return false;
    }

    std::vector<size_t> array_shape, chunk_shape;

    size_t append_size = frames_written_;
    for (auto i = config_.dimensions->ndims() - 3; i > 0; --i) {
        const auto& dim = config_.dimensions->at(i);
        const auto& array_size_px = dim.array_size_px;
        CHECK(array_size_px);
        append_size = (append_size + array_size_px - 1) / array_size_px;
    }
    array_shape.push_back(append_size);

    chunk_shape.push_back(config_.dimensions->final_dim().chunk_size_px);
    for (auto i = 1; i < config_.dimensions->ndims(); ++i) {
        const auto& dim = config_.dimensions->at(i);
        array_shape.push_back(dim.array_size_px);
        chunk_shape.push_back(dim.chunk_size_px);
    }

    json metadata;
    metadata["zarr_format"] = 2;
    metadata["shape"] = array_shape;
    metadata["chunks"] = chunk_shape;
    metadata["dtype"] = dtype;
    metadata["fill_value"] = 0;
    metadata["order"] = "C";
    metadata["filters"] = nullptr;
    metadata["dimension_separator"] = "/";

    if (config_.compression_params) {
        const BloscCompressionParams bcp = *config_.compression_params;
        metadata["compressor"] = json{ { "id", "blosc" },
                                       { "cname", bcp.codec_id },
                                       { "clevel", bcp.clevel },
                                       { "shuffle", bcp.shuffle } };
    } else {
        metadata["compressor"] = nullptr;
    }

    std::string metadata_str = metadata.dump(4);
    std::span data{ reinterpret_cast<std::byte*>(metadata_str.data()),
                    metadata_str.size() };
    return metadata_sink_->write(0, data);
}

bool
zarr::ZarrV2ArrayWriter::should_rollover_() const
{
    return true;
}
