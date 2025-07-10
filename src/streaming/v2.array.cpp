#include "v2.array.hh"

#include "definitions.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>

#include <latch>
#include <semaphore>
#include <stdexcept>

using json = nlohmann::json;

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

zarr::V2Array::V2Array(std::shared_ptr<ArrayConfig> config,
                       std::shared_ptr<ThreadPool> thread_pool,
                       std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : Array(config, thread_pool, s3_connection_pool)
{
}

std::vector<std::string>
zarr::V2Array::metadata_keys_() const
{
    return { ".zarray" };
}

bool
zarr::V2Array::make_metadata_()
{
    metadata_strings_.clear();

    std::string dtype;
    if (!sample_type_to_dtype(config_->dtype, dtype)) {
        return false;
    }

    std::vector<size_t> array_shape, chunk_shape;

    size_t append_size = frames_written_;
    for (auto i = config_->dimensions->ndims() - 3; i > 0; --i) {
        const auto& dim = config_->dimensions->at(i);
        const auto& array_size_px = dim.array_size_px;
        CHECK(array_size_px);
        append_size = (append_size + array_size_px - 1) / array_size_px;
    }
    array_shape.push_back(append_size);

    chunk_shape.push_back(config_->dimensions->final_dim().chunk_size_px);
    for (auto i = 1; i < config_->dimensions->ndims(); ++i) {
        const auto& dim = config_->dimensions->at(i);
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

    if (config_->compression_params) {
        const BloscCompressionParams bcp = *config_->compression_params;
        metadata["compressor"] = json{ { "id", "blosc" },
                                       { "cname", bcp.codec_id },
                                       { "clevel", bcp.clevel },
                                       { "shuffle", bcp.shuffle } };
    } else {
        metadata["compressor"] = nullptr;
    }

    metadata_strings_.emplace(".zarray", metadata.dump(4));

    return true;
}

std::string
zarr::V2Array::data_root_() const
{
    return node_path_() + "/" + std::to_string(append_chunk_index_);
}

const DimensionPartsFun
zarr::V2Array::parts_along_dimension_() const
{
    return chunks_along_dimension;
}

bool
zarr::V2Array::compress_and_flush_data_()
{
    // construct paths to chunk sinks
    CHECK(data_paths_.empty());
    make_data_paths_();

    const auto n_chunks = chunk_buffers_.size();
    CHECK(data_paths_.size() == n_chunks);

    const auto compression_params = config_->compression_params;
    const auto bytes_of_raw_chunk = config_->dimensions->bytes_per_chunk();
    const auto bytes_per_px = bytes_of_type(config_->dtype);
    const auto bucket_name = config_->bucket_name;
    auto connection_pool = s3_connection_pool_;

    // create parent directories if needed
    const auto is_s3 = is_s3_array_();
    if (!is_s3) {
        const auto parent_paths = get_parent_paths(data_paths_);
        CHECK(make_dirs(parent_paths, thread_pool_));
    }

    std::atomic<char> all_successful = 1;
    auto shared_latch = std::make_shared<std::latch>(n_chunks);  // ← Use shared_ptr
    std::counting_semaphore<MAX_CONCURRENT_FILES> semaphore(MAX_CONCURRENT_FILES);

    for (auto i = 0; i < n_chunks; ++i) {
        auto job = [bytes_per_px,
                    compression_params,
                    is_s3,
                    data_path = data_paths_[i],
                    &chunk_buffer = chunk_buffers_[i],
                    bucket_name,
                    connection_pool,
                    &semaphore,
                    shared_latch,  // ← Capture by value (shared_ptr copy)
                    &all_successful](std::string& err) {
            bool success = false;
            bool semaphore_acquired = false;

            if (!all_successful) {
                shared_latch->count_down();
                err = "Other jobs in batch have failed, not proceeding";
                return false;
            }

            try {
                std::unique_ptr<Sink> sink;
                semaphore.acquire();
                semaphore_acquired = true;

                // compress the chunk
                if (compression_params &&
                    !chunk_buffer.compress(*compression_params, bytes_per_px)) {
                    err = "Failed to compress chunk at path " + data_path;
                    success = false;
                } else {
                    if (is_s3) {
                        sink = make_s3_sink(*bucket_name, data_path, connection_pool);
                    } else {
                        sink = make_file_sink(data_path);
                    }

                    if (sink == nullptr) {
                        err = "Failed to create sink for " + data_path;
                        success = false;
                    } else {
                        // try to write the chunk to the sink
                        success = chunk_buffer.with_lock(
                          [&sink](const auto& buf) { return sink->write(0, buf); });

                        if (!success) {
                            err = "Failed to write chunk to " + data_path;
                        } else if (!finalize_sink(std::move(sink))) {
                            err = "Failed to finalize sink at path " + data_path;
                            success = false;
                        }
                    }
                }
            } catch (const std::exception& exc) {
                err = exc.what();
                success = false;
            }

            // Cleanup - single exit point
            if (semaphore_acquired) {
                semaphore.release();
            }

            all_successful.fetch_and(static_cast<char>(success));
            shared_latch->count_down();  // ← Single count_down call

            return success;
        };

        // one thread is reserved for processing the frame queue and runs the
        // entire lifetime of the stream
        if (thread_pool_->n_threads() == 1 ||
            !thread_pool_->push_job(std::move(job))) {
            std::string err;
            if (!job(err)) {
                LOG_ERROR(err);
            }
        }
    }

    shared_latch->wait();
    return static_cast<bool>(all_successful);
}

void
zarr::V2Array::close_sinks_()
{
    data_paths_.clear();
}

bool
zarr::V2Array::should_rollover_() const
{
    return true;
}
