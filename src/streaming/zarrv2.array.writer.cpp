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

bool
zarr::ZarrV2ArrayWriter::make_data_sinks_()
{
    SinkCreator creator(thread_pool_, s3_connection_pool_);

    const auto data_root = data_root_();
    if (is_s3_array_()) {
        if (!creator.make_data_s3_sinks(*config_.bucket_name,
                                        data_root,
                                        config_.dimensions.get(),
                                        chunks_along_dimension,
                                        data_sinks_)) {
            LOG_ERROR("Failed to create data sinks in ",
                      data_root,
                      " for bucket ",
                      *config_.bucket_name);
            return false;
        }
    } else if (!creator.make_data_file_sinks(data_root,
                                             config_.dimensions.get(),
                                             chunks_along_dimension,
                                             data_sinks_)) {
        LOG_ERROR("Failed to create data sinks in ", data_root);
        return false;
    }

    return true;
}

void
zarr::ZarrV2ArrayWriter::compress_and_flush_()
{
    if (bytes_to_flush_ == 0) {
        LOG_DEBUG("No data to flush");
        return;
    }

    const auto n_chunks = chunk_buffers_.size();

    CHECK(data_sinks_.empty());
    CHECK(make_data_sinks_());
    CHECK(data_sinks_.size() == n_chunks);

    const auto bytes_per_px = bytes_of_type(config_.dtype);

    std::latch latch(n_chunks);
    for (auto i = 0; i < n_chunks; ++i) {
        auto& chunk = chunk_buffers_[i];

        if (config_.compression_params) {
            auto& params = *config_.compression_params;
            auto job = [&params,
                        buf = &chunk,
                        bytes_per_px,
                        &sink = data_sinks_[i],
                        thread_pool = thread_pool_,
                        &latch](std::string& err) -> bool {
                const size_t bytes_of_chunk = buf->size();

                const auto tmp_size = bytes_of_chunk + BLOSC_MAX_OVERHEAD;
                ChunkBuffer tmp(tmp_size);
                const auto nb =
                  blosc_compress_ctx(params.clevel,
                                     params.shuffle,
                                     bytes_per_px,
                                     bytes_of_chunk,
                                     buf->data(),
                                     tmp.data(),
                                     tmp_size,
                                     params.codec_id.c_str(),
                                     0 /* blocksize - 0:automatic */,
                                     1);

                if (nb <= 0) {
                    err = "Failed to compress chunk";
                    latch.count_down();
                    return false;
                }

                tmp.resize(nb);
                buf->swap(tmp);

                auto queued = thread_pool->push_job(
                  std::move([&sink, buf, &latch](std::string& err) -> bool {
                      bool success = false;

                      try {
                          success = sink->write(0, *buf);
                      } catch (const std::exception& exc) {
                          err =
                            "Failed to write chunk: " + std::string(exc.what());
                      }

                      latch.count_down();
                      return success;
                  }));

                if (!queued) {
                    err = "Failed to push job to thread pool";
                    latch.count_down();
                }

                return queued;
            };

            CHECK(thread_pool_->push_job(std::move(job)));
        } else {
            auto job = [buf = &chunk,
                        &sink = data_sinks_[i],
                        thread_pool = thread_pool_,
                        &latch](std::string& err) -> bool {
                auto queued = thread_pool->push_job(
                  std::move([&sink, buf, &latch](std::string& err) -> bool {
                      bool success = false;

                      try {
                          success = sink->write(0, *buf);
                      } catch (const std::exception& exc) {
                          err =
                            "Failed to write chunk: " + std::string(exc.what());
                      }

                      latch.count_down();
                      return success;
                  }));

                if (!queued) {
                    err = "Failed to push job to thread pool";
                    latch.count_down();
                }

                return queued;
            };

            CHECK(thread_pool_->push_job(std::move(job)));
        }
    }

    // wait for all threads to finish
    latch.wait();
}

bool
zarr::ZarrV2ArrayWriter::flush_impl_()
{
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
