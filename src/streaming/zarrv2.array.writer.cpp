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

bool
zarr::ZarrV2ArrayWriter::compress_and_flush_data_()
{
    // create chunk files
    CHECK(data_sinks_.empty());
    if (!make_data_sinks_()) {
        return false;
    }

    const auto n_chunks = chunk_buffers_.size();
    CHECK(data_sinks_.size() == n_chunks);

    std::atomic<char> all_successful = 1;
    std::latch latch(n_chunks);
    for (auto i = 0; i < n_chunks; ++i) {
        EXPECT(
          thread_pool_->push_job(
            std::move([this, i, &latch, &all_successful](std::string& err) {
                bool success = true;

                try {
                    if (all_successful) {
                        if (!compress_buffer_(i)) { // no-op if no compression
                            err = "Failed to compress buffer";
                            latch.count_down();
                            return false;
                        }

                        auto& chunk = chunk_buffers_.at(i);
                        auto& sink = data_sinks_.at(i);

                        if (!sink->write(
                              0,
                              { reinterpret_cast<std::byte*>(chunk.data()),
                                chunk.size() })) {
                            err = "Failed to write chunk";
                            success = false;
                        }
                    }
                } catch (const std::exception& exc) {
                    err = "Failed to flush data: " + std::string(exc.what());
                    success = false;
                }

                latch.count_down();

                all_successful.fetch_and(static_cast<char>(success));
                return success;
            })),
          "Failed to push job to thread pool");
    }

    latch.wait();
    return static_cast<bool>(all_successful);
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
