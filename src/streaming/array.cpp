#include "array.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"

#include <nlohmann/json.hpp>
#include <crc32c/crc32c.h>

#include <algorithm> // std::fill
#include <blosc.h>
#include <cstring>
#include <functional>
#include <future>
#include <stdexcept>
#include <zstd.h>

using json = nlohmann::json;

namespace {
std::string
sample_type_to_dtype(ZarrDataType t)
{
    switch (t) {
        case ZarrDataType_uint8:
            return "uint8";
        case ZarrDataType_uint16:
            return "uint16";
        case ZarrDataType_uint32:
            return "uint32";
        case ZarrDataType_uint64:
            return "uint64";
        case ZarrDataType_int8:
            return "int8";
        case ZarrDataType_int16:
            return "int16";
        case ZarrDataType_int32:
            return "int32";
        case ZarrDataType_int64:
            return "int64";
        case ZarrDataType_float32:
            return "float32";
        case ZarrDataType_float64:
            return "float64";
        default:
            throw std::runtime_error("Invalid ZarrDataType: " +
                                     std::to_string(static_cast<int>(t)));
    }
}

std::string
shuffle_to_string(uint8_t shuffle)
{
    switch (shuffle) {
        case 0:
            return "noshuffle";
        case 1:
            return "shuffle";
        case 2:
            return "bitshuffle";
        default:
            throw std::runtime_error("Invalid shuffle value: " +
                                     std::to_string(shuffle));
    }
}

size_t
max_compressed_size(size_t uncompressed_size,
                    const std::optional<zarr::CompressionParams>& params)
{
    if (!params) {
        return uncompressed_size;
    }

    return std::visit(
      [&uncompressed_size]<typename ParamT>(const ParamT& params) {
          using T = std::decay_t<ParamT>;
          if constexpr (std::is_same_v<T, zarr::BloscCompressionParams>) {
              return uncompressed_size + BLOSC_MAX_OVERHEAD;
          } else {
              return ZSTD_compressBound(uncompressed_size);
          }
      },
      *params);
}
} // namespace

zarr::Array::Array(std::shared_ptr<ArrayConfig> config,
                   std::shared_ptr<ThreadPool> thread_pool,
                   std::shared_ptr<FileHandlePool> file_handle_pool,
                   std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : ArrayBase(config, thread_pool, file_handle_pool, s3_connection_pool)
  , chunk_mutexes_(config->dimensions->number_of_chunks_in_memory())
  , max_bytes_(config->dimensions->max_byte_count())
  , bytes_per_frame_(bytes_of_frame(*config->dimensions, config->dtype))
  , total_bytes_written_{ 0 }
  , bytes_to_flush_{ 0 }
  , append_chunk_index_{ 0 }
  , is_closing_{ false }
  , last_successful_frame_id_{ 0 }
  , current_layer_{ 0 }
  , flushed_band_count_{ 0 }
{
    const size_t n_chunks = config_->dimensions->number_of_chunks_in_memory();
    EXPECT(n_chunks > 0, "Array has zero chunks in memory");

    // allocated lazily on write, freed per band/layer on flush
    chunks_.resize(n_chunks);

    // append chunk > 1 keeps the whole inner volume resident (no banding); warn
    const auto& dims = *config_->dimensions;
    if (dims.final_dim().chunk_size_px > 1 && dims.ndims() >= 4) {
        const size_t layer_bytes =
          static_cast<size_t>(dims.number_of_chunks_in_memory()) *
          dims.bytes_per_chunk();
        constexpr size_t warn_threshold = size_t{ 1 } << 30; // 1 GiB
        if (layer_bytes > warn_threshold) {
            LOG_WARNING("Append dimension '",
                        dims.final_dim().name,
                        "' has chunk size ",
                        dims.final_dim().chunk_size_px,
                        " (> 1), so ~",
                        layer_bytes >> 20,
                        " MiB of chunk buffers must stay resident until each "
                        "append chunk completes; set its chunk size to 1 to "
                        "enable incremental flushing of large intermediate "
                        "dimensions.");
        }
    }

    // For 2D arrays, don't include append_chunk_index in the path
    if (config_->dimensions->is_2d()) {
        data_root_ = node_path_() + "/c";
    } else {
        data_root_ = node_path_() + "/c/" + std::to_string(append_chunk_index_);
    }
}

size_t
zarr::Array::memory_usage() const noexcept
{
    // size_bytes() returns the const allocation budget of each chunk and is
    // safe to read without locking.
    size_t total = 0;
    for (const auto& chunk : chunks_) {
        if (chunk) { // slots are empty until lazily allocated on write
            total += chunk->size_bytes();
        }
    }

    return total;
}

zarr::WriteResult
zarr::Array::write_frame(std::vector<uint8_t>& frame,
                         size_t& bytes_written,
                         uint64_t frame_id)
{
    bytes_written = 0;

    const auto nbytes_data = frame.size();
    const auto nbytes_frame =
      bytes_of_frame(*config_->dimensions, config_->dtype);

    if (nbytes_frame != nbytes_data) {
        LOG_ERROR("Frame size mismatch: expected ",
                  nbytes_frame,
                  ", got ",
                  nbytes_data,
                  ". Skipping");
        return WriteResult::FrameSizeMismatch;
    }

    // check that we can append
    if (max_bytes_ > 0 && total_bytes_written_ + nbytes_data > max_bytes_) {
        LOG_ERROR("Unable to write. Data would exceed bounds of array.");
        return WriteResult::OutOfBounds;
    }

    // frame out of order, try again
    auto frames_written = frames_written_();
    if (frame_id != frames_written) {
        LOG_DEBUG("Frame ID ",
                  frame_id,
                  " is out of order. Frames written: ",
                  frames_written,
                  ", last frame ID: ",
                  last_successful_frame_id_);
        return WriteResult::FrameOutOfOrder;
    }

    // split the incoming frame into tiles and write them to the chunk
    // buffers
    bytes_written = write_frame_to_chunks_(frame);
    CHECK(bytes_written <= nbytes_data);

    last_successful_frame_id_ = frame_id;
    bytes_to_flush_ += bytes_written;
    total_bytes_written_ += bytes_written;
    frames_written = frames_written_();
    LOG_DEBUG("Wrote ",
              bytes_written,
              " bytes of frame ",
              last_successful_frame_id_,
              " to LOD ",
              config_->level_of_detail,
              "; frames written: ",
              frames_written);

    if (config_->dimensions->supports_dim1_banding()) {
        CHECK(flush_completed_bands_());
    } else if (should_flush_()) {
        CHECK(compress_and_flush_data_());

        if (should_rollover_()) {
            rollover_();
            CHECK(write_metadata_());
        }
        bytes_to_flush_ = 0;
    }

    return bytes_written == frame.size() ? WriteResult::Ok
                                         : WriteResult::PartialWrite;
}

size_t
zarr::Array::max_bytes() const
{
    return max_bytes_;
}

bool
zarr::Array::make_metadata_(nlohmann::json& metadata)
{
    nlohmann::json meta_tmp;
    std::vector<size_t> array_shape, chunk_shape, shard_shape;
    const auto& dims = config_->dimensions;

    // For 2D arrays, skip the phantom singleton dimension in metadata
    const size_t start_dim = dims->is_2d() ? 1 : 0;

    if (!dims->is_2d()) {
        // Compute append dimension size for 3D+ arrays
        size_t append_size = frames_written_();
        for (auto i = dims->ndims() - 3; i > 0; --i) {
            const auto& dim = dims->at(i);
            const auto& array_size_px = dim.array_size_px;
            CHECK(array_size_px);
            append_size = (append_size + array_size_px - 1) / array_size_px;
        }
        array_shape.push_back(append_size);

        const auto& final_dim = dims->final_dim();
        chunk_shape.push_back(final_dim.chunk_size_px);
        shard_shape.push_back(final_dim.shard_size_chunks * chunk_shape.back());
    }

    for (auto i = start_dim == 0 ? 1 : start_dim; i < dims->ndims(); ++i) {
        const auto& dim = dims->at(i);
        array_shape.push_back(dim.array_size_px);
        chunk_shape.push_back(dim.chunk_size_px);
        shard_shape.push_back(dim.shard_size_chunks * chunk_shape.back());
    }

    meta_tmp["shape"] = array_shape;
    meta_tmp["chunk_grid"] = json::object({
      { "name", "regular" },
      {
        "configuration",
        json::object({ { "chunk_shape", shard_shape } }),
      },
    });
    meta_tmp["chunk_key_encoding"] = json::object({
      { "name", "default" },
      {
        "configuration",
        json::object({ { "separator", "/" } }),
      },
    });
    meta_tmp["fill_value"] = 0;

    meta_tmp["attributes"] = json::object();
    auto& attributes = meta_tmp["attributes"];

    // write custom metadata, if any
    {
        std::unique_lock lock(metadata_mutex_);
        if (!custom_metadata_.empty()) {
            for (const auto& [key, value] : custom_metadata_) {
                attributes[key] = value;
            }
        }
    }

    meta_tmp["zarr_format"] = 3;
    meta_tmp["node_type"] = "array";
    meta_tmp["storage_transformers"] = json::array();
    meta_tmp["data_type"] = sample_type_to_dtype(config_->dtype);
    meta_tmp["storage_transformers"] = json::array();

    // Skip phantom dimension (index 0) for 2D arrays in dimension names
    const size_t name_start = dims->is_2d() ? 1 : 0;
    std::vector<std::string> dimension_names(dims->ndims() - name_start);
    for (auto i = name_start; i < dims->ndims(); ++i) {
        dimension_names[i - name_start] = dims->at(i).name;
    }
    meta_tmp["dimension_names"] = dimension_names;

    auto codecs = json::array();

    auto sharding_indexed = json::object();
    sharding_indexed["name"] = "sharding_indexed";

    auto configuration = json::object();
    configuration["chunk_shape"] = chunk_shape;

    auto codec = json::object();
    codec["configuration"] = json::object({ { "endian", "little" } });
    codec["name"] = "bytes";

    auto index_codec = json::object();
    index_codec["configuration"] = json::object({ { "endian", "little" } });
    index_codec["name"] = "bytes";

    auto crc32_codec = json::object({ { "name", "crc32c" } });
    configuration["index_codecs"] = json::array({
      index_codec,
      crc32_codec,
    });

    configuration["index_location"] = "end";
    configuration["codecs"] = json::array({ codec });

    if (config_->compression_params) {
        auto compression_codec = std::visit(
          [this](const auto& params) -> json {
              using T = std::decay_t<decltype(params)>;
              if constexpr (std::is_same_v<T, zarr::BloscCompressionParams>) {
                  auto config = json::object();
                  config["blocksize"] = 0;
                  config["clevel"] = params.clevel;
                  config["cname"] = params.codec_id;
                  config["shuffle"] = shuffle_to_string(params.shuffle);
                  config["typesize"] = bytes_of_type(config_->dtype);
                  return json::object({
                    { "name", "blosc" },
                    { "configuration", config },
                  });
              } else {
                  static_assert(std::is_same_v<T, zarr::ZstdCompressionParams>);
                  return json::object({
                    { "name", "zstd" },
                    { "configuration",
                      json::object({
                        { "level", params.level },
                        { "checksum", false },
                      }) },
                  });
              }
          },
          *config_->compression_params);
        configuration["codecs"].push_back(compression_codec);
    }

    sharding_indexed["configuration"] = configuration;

    codecs.push_back(sharding_indexed);

    meta_tmp["codecs"] = codecs;
    metadata = std::move(meta_tmp);

    return true;
}

bool
zarr::Array::close_()
{
    bool retval = false;
    is_closing_ = true;
    try {
        if (bytes_to_flush_ > 0) {
            if (config_->dimensions->supports_dim1_banding()) {
                // flush the trailing bands of the open layer
                CHECK(flush_layer_remainder_());
            } else {
                CHECK(compress_and_flush_data_());
            }
        }

        {
            std::unique_lock lock(write_counter_mutex_);
            write_counter_cv_.wait(
              lock, [this]() { return write_counter_.load() == 0; });
        }

        // outstanding writes have drained; finalize any shards that were not
        // already flushed by their last chunk writer (e.g. a partial trailing
        // shard) so a flush failure is observed rather than swallowed in the
        // shard destructor
        if (!finalize_shards_()) {
            LOG_ERROR("Failed to finalize shards on close");
            return false;
        }

        close_sinks_();

        if (frames_written_() > 0 || !custom_metadata_.empty()) {
            if (!write_metadata_()) {
                LOG_ERROR("Failed to write metadata on close");
                return false;
            }

            if (!finalize_sink(std::move(metadata_sink_))) {
                LOG_ERROR("Failed to finalize metadata sink");
                return false;
            }
        }
        retval = true;
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to finalize array writer: ", exc.what());
    }

    is_closing_ = false;
    return retval;
}

bool
zarr::Array::is_s3_array_() const
{
    return config_->bucket_name.has_value();
}

void
zarr::Array::make_shards_()
{
    if (data_paths_.empty()) {
        data_paths_ = construct_data_paths(data_root_,
                                           *config_->dimensions,
                                           shards_along_dimension,
                                           !is_s3_array_());

        const size_t n_shards = data_paths_.size();
        const auto& dims = config_->dimensions;
        const size_t chunks_per_shard = dims->chunks_per_shard();
        const size_t bytes_per_chunk = dims->bytes_per_chunk();

        std::unique_lock lock(shards_mutex_);
        shards_.resize(n_shards);

        for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
            ShardConfig cfg{
                .path = data_paths_[shard_idx],
                .chunks_per_shard = chunks_per_shard,
                .bytes_per_chunk = bytes_per_chunk,
                .bucket_name = config_->bucket_name,
            };

            shards_[shard_idx] = std::make_shared<Shard>(
              std::move(cfg), file_handle_pool_, s3_connection_pool_);
        }
    }
}

std::unique_ptr<zarr::Sink>
zarr::Array::make_data_sink_(std::string_view path) const
{
    std::unique_ptr<Sink> sink;

    if (is_s3_array_()) {
        const auto bucket_name = *config_->bucket_name;
        sink = make_s3_sink(bucket_name, path, s3_connection_pool_);
    } else { // assume parent directories exist
        sink = make_file_sink(path, file_handle_pool_);
    }

    return sink;
}

namespace {
/**
 * @brief Transpose a 2D frame buffer (Y×X → X×Y).
 *
 * @param src Source buffer containing row-major data
 * @param dst Destination buffer for transposed data
 * @param src_rows Number of rows in source
 * @param src_cols Number of columns in source
 * @param bytes_per_pixel Size of each pixel in bytes
 */
void
transpose_frame(const uint8_t* src,
                uint8_t* dst,
                uint32_t src_rows,
                uint32_t src_cols,
                size_t bytes_per_pixel)
{
    // Transpose: dst[col][row] = src[row][col]
    // Output dimensions: src_cols × src_rows
    for (uint32_t row = 0; row < src_rows; ++row) {
        for (uint32_t col = 0; col < src_cols; ++col) {
            const auto src_offset = (row * src_cols + col) * bytes_per_pixel;
            const auto dst_offset = (col * src_rows + row) * bytes_per_pixel;
            std::memcpy(dst + dst_offset, src + src_offset, bytes_per_pixel);
        }
    }
}
} // namespace

size_t
zarr::Array::write_frame_to_chunks_(std::vector<uint8_t>& frame)
{
    // break the frame into tiles and write them to the chunk buffers
    const auto bytes_per_px = bytes_of_type(config_->dtype);

    const auto& dimensions = config_->dimensions;

    // Check if we need to transpose spatial dimensions (Y↔X)
    std::vector<uint8_t> transposed_frame;
    if (dimensions->needs_xy_transposition()) {
        const auto acq_rows = dimensions->acquisition_frame_rows();
        const auto acq_cols = dimensions->acquisition_frame_cols();

        // Allocate buffer for transposed frame
        transposed_frame.resize(frame.size());

        // Transpose: input is acq_rows × acq_cols, output is acq_cols ×
        // acq_rows
        transpose_frame(frame.data(),
                        transposed_frame.data(),
                        acq_rows,
                        acq_cols,
                        bytes_per_px);

        // Replace frame with transposed version
        frame = std::move(transposed_frame);
    }

    // Now use storage-order dimensions for chunking
    const auto& x_dim = dimensions->width_dim();
    const auto frame_cols = x_dim.array_size_px;
    const auto tile_cols = x_dim.chunk_size_px;

    const auto& y_dim = dimensions->height_dim();
    const auto frame_rows = y_dim.array_size_px;
    const auto tile_rows = y_dim.chunk_size_px;

    if (tile_cols == 0 || tile_rows == 0) {
        return 0;
    }

    const auto bytes_per_chunk = dimensions->bytes_per_chunk();
    const auto bytes_per_tile_row = tile_cols * bytes_per_px;

    const auto n_tiles_x = (frame_cols + tile_cols - 1) / tile_cols;
    const auto n_tiles_y = (frame_rows + tile_rows - 1) / tile_rows;

    // don't take the frame id from the incoming frame, as the camera may have
    // dropped frames
    const auto acquisition_frame_id = frames_written_();

    // Transpose frame_id from acquisition order to prescribed
    // storage_dimension_order
    const auto frame_id = dimensions->transpose_frame_id(acquisition_frame_id);

    // offset among the chunks in the lattice
    const auto group_offset = dimensions->tile_group_offset(frame_id);
    // offset within the chunk
    const auto chunk_offset = dimensions->chunk_internal_offset(frame_id);

    size_t bytes_written = 0;
    const auto n_tiles = n_tiles_x * n_tiles_y;

    const auto* data_ptr = frame.data();
    const auto data_size = frame.size();
    const auto src_row_stride = static_cast<size_t>(frame_cols) * bytes_per_px;

#pragma omp parallel for reduction(+ : bytes_written)
    for (auto tile_idx = 0; tile_idx < n_tiles; ++tile_idx) {
        auto& chunk = chunks_[tile_idx + group_offset];
        {
            std::unique_lock lock(chunk_mutexes_[tile_idx + group_offset]);
            if (chunk == nullptr) {
                chunk = std::make_shared<Chunk>(bytes_per_chunk, bytes_per_px);
            }
        }

        const auto tile_idx_y = tile_idx / n_tiles_x;
        const auto tile_idx_x = tile_idx % n_tiles_x;

        const uint32_t frame_row0 = tile_idx_y * tile_rows;
        if (frame_row0 >= frame_rows) {
            continue; // tile lies entirely below the frame: no data to copy
        }
        const uint32_t n_rows =
          std::min<uint32_t>(tile_rows, frame_rows - frame_row0);

        const auto frame_col = tile_idx_x * tile_cols;
        const auto region_width =
          std::min(frame_col + tile_cols, frame_cols) - frame_col;
        const auto copy_nbytes = static_cast<size_t>(region_width) * bytes_per_px;

        const auto region_start =
          bytes_per_px * (static_cast<size_t>(frame_row0) * frame_cols + frame_col);
        EXPECT(region_start + static_cast<size_t>(n_rows - 1) * src_row_stride +
                   copy_nbytes <=
                 data_size,
               "Buffer overflow in frame. region_start: ",
               region_start,
               ", data size: ",
               data_size);

        // Copy frame rows straight into the chunk buffer; no intermediate
        // per-tile allocation, zero-fill, or second memcpy.
        chunk->write_tile_rows(chunk_offset,
                               data_ptr + region_start,
                               src_row_stride,
                               copy_nbytes,
                               bytes_per_tile_row,
                               n_rows);
        bytes_written += copy_nbytes * n_rows;
    }

    return bytes_written;
}

void
zarr::Array::dispatch_skip_job_(std::shared_ptr<Shard> shard,
                                uint32_t internal_idx,
                                uint32_t shard_idx)
{
    write_counter_.fetch_add(1);
    write_counter_cv_.notify_all();

    auto job = [this, shard, internal_idx, shard_idx](
                 std::string& err) -> ThreadPool::TaskResult {
        ThreadPool::TaskResult result;

        try {
            if (shard->skip_chunk(internal_idx)) {
                result = ThreadPool::TaskResult::Success;
            } else { // failed to write table / flush shard
                err = "Failed to skip chunk " + std::to_string(internal_idx) +
                      " of shard " + std::to_string(shard_idx);
                result = ThreadPool::TaskResult::Fatal;
            }
        } catch (const std::exception& exc) {
            err = std::string("Failed skipping chunk: ") + exc.what();
            result = ThreadPool::TaskResult::Fatal;
        }

        write_counter_.fetch_sub(1);
        write_counter_cv_.notify_all();
        return result;
    };

    // one thread is reserved for processing the frame queue and runs the
    // entire lifetime of the stream
    if (thread_pool_->n_threads() == 1 || !thread_pool_->push_job(job)) {
        if (!thread_pool_->execute_job(std::move(job))) {
            LOG_ERROR(
              "Failed to skip chunk ", internal_idx, " of shard ", shard_idx);
        }
    }
}

void
zarr::Array::dispatch_chunk_job_(std::shared_ptr<Shard> shard,
                                 uint32_t chunk_idx,
                                 uint32_t internal_idx,
                                 uint32_t shard_idx,
                                 uint32_t chunk_offset,
                                 size_t bytes_per_chunk,
                                 size_t bytes_per_px)
{
    std::shared_ptr<Chunk> chunk;
    {
        // take the chunk out of its slot; the next layer reallocates lazily
        std::unique_lock lock(chunk_mutexes_[chunk_idx - chunk_offset]);
        chunk = std::move(chunks_[chunk_idx - chunk_offset]);
    }

    write_counter_.fetch_add(1);
    write_counter_cv_.notify_all();

    auto job = [this,
                shard,
                internal_idx,
                chunk_idx,
                shard_idx,
                chunk = std::move(chunk),
                params = config_->compression_params](
                 std::string& err) -> ThreadPool::TaskResult {
        ThreadPool::TaskResult result;

        constexpr size_t n_retries = 3;

        try {
            auto try_write = [&](const auto& write_fn) {
                for (auto retry = 0; retry < n_retries; ++retry) {
                    if (write_fn()) {
                        return true;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(
                      static_cast<int>(std::pow(10, retry))));
                }
                return false;
            };

            auto retries_exhausted_msg = [&] {
                return "Failed to write chunk " + std::to_string(chunk_idx) +
                       " of shard " + std::to_string(shard_idx) + " after " +
                       std::to_string(n_retries) + " attempts";
            };

            if (!chunk || !chunk->has_data()) {
                if (try_write(
                      [&] { return shard->skip_chunk(internal_idx); })) {
                    result = ThreadPool::TaskResult::Success;
                } else {
                    err = retries_exhausted_msg();
                    result = ThreadPool::TaskResult::Fatal;
                }
            } else {
                std::vector<uint8_t> compressed;
                if (!chunk->compress_and_take_buffer(params, compressed)) {
                    err = "Failed to compress chunk " +
                          std::to_string(chunk_idx) + " of shard " +
                          std::to_string(shard_idx);
                    result = ThreadPool::TaskResult::Fatal;
                } else if (try_write([&] {
                               return shard->write_chunk(internal_idx,
                                                         compressed);
                           })) {
                    result = ThreadPool::TaskResult::Success;
                } else {
                    err = retries_exhausted_msg();
                    result = ThreadPool::TaskResult::Fatal;
                }
            }
        } catch (const std::exception& exc) {
            err = std::string("Failed to write chunk: ") + exc.what();
            result = ThreadPool::TaskResult::Fatal;
        }

        write_counter_.fetch_sub(1);
        write_counter_cv_.notify_all();
        return result;
    };

    // one thread is reserved for processing the frame queue and runs the
    // entire lifetime of the stream
    if (thread_pool_->n_threads() == 1 || !thread_pool_->push_job(job)) {
        if (!thread_pool_->execute_job_with_retry(std::move(job), 2)) {
            LOG_ERROR("Failed to write chunk ",
                      chunk_idx,
                      ", (internal index ",
                      internal_idx,
                      ") of shard ",
                      shard_idx);
        }
    }
}

bool
zarr::Array::compress_and_flush_data_()
{
    // construct paths to shard sinks if they don't already exist
    if (data_paths_.empty()) {
        make_shards_();
    }

    const auto& dims = config_->dimensions;

    const auto n_shards = dims->number_of_shards();
    CHECK(shards_.size() == n_shards);

    const auto chunks_in_mem = dims->number_of_chunks_in_memory();
    const auto chunk_offset = current_layer_ * chunks_in_mem;

    const size_t bytes_per_chunk = dims->bytes_per_chunk();
    const size_t bytes_per_px = bytes_of_type(config_->dtype);

    // write every chunk in the layer; skip ragged padding to complete each
    // shard's countdown
    for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
        auto shard = shards_[shard_idx];

        for (const auto& internal_idx :
             dims->skipped_internal_indices_for_shard_layer(shard_idx,
                                                            current_layer_)) {
            dispatch_skip_job_(shard, internal_idx, shard_idx);
        }

        for (const auto& chunk_idx :
             dims->chunk_indices_for_shard_layer(shard_idx, current_layer_)) {
            dispatch_chunk_job_(shard,
                                chunk_idx,
                                dims->shard_internal_index(chunk_idx),
                                shard_idx,
                                chunk_offset,
                                bytes_per_chunk,
                                bytes_per_px);
        }
    }

    if (should_rollover_()) {
        current_layer_ = 0;
    } else {
        ++current_layer_;
    }

    return true;
}

bool
zarr::Array::compress_and_flush_band_(uint32_t band_idx, uint32_t n_bands)
{
    if (data_paths_.empty()) {
        make_shards_();
    }

    const auto& dims = config_->dimensions;

    const auto n_shards = dims->number_of_shards();
    CHECK(shards_.size() == n_shards);

    const auto chunks_in_mem = dims->number_of_chunks_in_memory();
    const auto chunk_offset = current_layer_ * chunks_in_mem;

    const size_t bytes_per_chunk = dims->bytes_per_chunk();
    const size_t bytes_per_px = bytes_of_type(config_->dtype);

    // a band is a contiguous block of slots (dim 1 is the slowest chunk index)
    const auto chunks_per_band = chunks_in_mem / n_bands;
    const auto local_begin = band_idx * chunks_per_band;
    const auto local_end = local_begin + chunks_per_band;

    for (auto local_idx = local_begin; local_idx < local_end; ++local_idx) {
        const auto chunk_idx = chunk_offset + local_idx;
        const auto shard_idx = dims->shard_index_for_chunk(chunk_idx);
        dispatch_chunk_job_(shards_[shard_idx],
                            chunk_idx,
                            dims->shard_internal_index(chunk_idx),
                            shard_idx,
                            chunk_offset,
                            bytes_per_chunk,
                            bytes_per_px);
    }

    // on the last band, account for ragged padding across all shards
    if (band_idx + 1 == n_bands) {
        for (auto shard_idx = 0; shard_idx < n_shards; ++shard_idx) {
            for (const auto& internal_idx :
                 dims->skipped_internal_indices_for_shard_layer(
                   shard_idx, current_layer_)) {
                dispatch_skip_job_(shards_[shard_idx], internal_idx, shard_idx);
            }
        }
    }

    return true;
}

bool
zarr::Array::flush_layer_remainder_()
{
    const auto n_bands = config_->dimensions->dim1_band_count();
    for (auto band = flushed_band_count_; band < n_bands; ++band) {
        CHECK(compress_and_flush_band_(band, n_bands));
    }
    flushed_band_count_ = n_bands;
    return true;
}

bool
zarr::Array::flush_completed_bands_()
{
    const auto& dims = config_->dimensions;

    const auto frames_per_layer = dims->frames_per_chunk_layer();
    const auto frames_per_band = dims->frames_per_dim1_band();
    const auto n_bands = dims->dim1_band_count();

    const auto frames_in_layer = frames_written_() % frames_per_layer;

    if (frames_in_layer == 0) {
        // layer complete: flush the trailing band(s) and advance/rollover
        CHECK(flush_layer_remainder_());

        if (should_rollover_()) {
            rollover_();
            CHECK(write_metadata_());
            current_layer_ = 0;
        } else {
            ++current_layer_;
        }
        bytes_to_flush_ = 0;
        flushed_band_count_ = 0;
    } else if (frames_in_layer % frames_per_band == 0) {
        // flush interior bands completed since the last flush
        const auto completed =
          static_cast<uint32_t>(frames_in_layer / frames_per_band);
        for (auto band = flushed_band_count_; band < completed; ++band) {
            CHECK(compress_and_flush_band_(band, n_bands));
        }
        flushed_band_count_ = completed;
    }

    return true;
}

bool
zarr::Array::should_flush_() const
{
    const auto& dims = config_->dimensions;
    size_t frames_before_flush = dims->final_dim().chunk_size_px;
    for (auto i = 1; i < dims->ndims() - 2; ++i) {
        frames_before_flush *= dims->at(i).array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_() % frames_before_flush == 0;
}

bool
zarr::Array::should_rollover_() const
{
    const auto& dims = config_->dimensions;
    const auto& append_dim = dims->final_dim();
    size_t frames_before_flush =
      append_dim.chunk_size_px * append_dim.shard_size_chunks;
    for (auto i = 1; i < dims->ndims() - 2; ++i) {
        frames_before_flush *= dims->at(i).array_size_px;
    }

    CHECK(frames_before_flush > 0);
    return frames_written_() % frames_before_flush == 0;
}

void
zarr::Array::rollover_()
{
    LOG_DEBUG("Rolling over");

    close_sinks_();
    ++append_chunk_index_;
    // For 2D arrays, don't include append_chunk_index in the path
    if (config_->dimensions->is_2d()) {
        data_root_ = node_path_() + "/c";
    } else {
        data_root_ = node_path_() + "/c/" + std::to_string(append_chunk_index_);
    }
}

bool
zarr::Array::finalize_shards_()
{
    std::unique_lock lock(shards_mutex_);
    bool ok = true;
    for (auto& shard : shards_) {
        if (shard && !shard->finalize()) {
            ok = false;
        }
    }
    return ok;
}

void
zarr::Array::close_sinks_()
{
    data_paths_.clear();
    shards_.clear();
}

size_t
zarr::Array::frames_written_() const
{
    return total_bytes_written_ / bytes_per_frame_;
}
