#include "macros.hh"
#include "zarr.stream.hh"
#include "acquire.zarr.h"
#include "zarr.common.hh"
#include "zarrv2.array.writer.hh"
#include "zarrv3.array.writer.hh"
#include "sink.hh"

#include <bit> // bit_ceil
#include <filesystem>

namespace fs = std::filesystem;

namespace {
bool
is_s3_acquisition(const struct ZarrStreamSettings_s* settings)
{
    return nullptr != settings->s3_settings;
}

bool
is_compressed_acquisition(const struct ZarrStreamSettings_s* settings)
{
    return nullptr != settings->compression_settings;
}

zarr::S3Settings
construct_s3_settings(const ZarrS3Settings* settings)
{
    zarr::S3Settings s3_settings{ .endpoint = zarr::trim(settings->endpoint),
                                  .bucket_name =
                                    zarr::trim(settings->bucket_name) };

    if (settings->region != nullptr) {
        s3_settings.region = zarr::trim(settings->region);
    }

    return s3_settings;
}

[[nodiscard]] bool
validate_s3_settings(const ZarrS3Settings* settings, std::string& error)
{
    if (zarr::is_empty_string(settings->endpoint, "S3 endpoint is empty")) {
        error = "S3 endpoint is empty";
        return false;
    }

    std::string trimmed = zarr::trim(settings->bucket_name);
    if (trimmed.length() < 3 || trimmed.length() > 63) {
        error = "Invalid length for S3 bucket name: " +
                std::to_string(trimmed.length()) +
                ". Must be between 3 and 63 characters";
        return false;
    }

    return true;
}

[[nodiscard]] bool
validate_filesystem_store_path(std::string_view data_root, std::string& error)
{
    fs::path path(data_root);
    fs::path parent_path = path.parent_path();
    if (parent_path.empty()) {
        parent_path = ".";
    }

    // parent path must exist and be a directory
    if (!fs::exists(parent_path) || !fs::is_directory(parent_path)) {
        error = "Parent path '" + parent_path.string() +
                "' does not exist or is not a directory";
        return false;
    }

    // parent path must be writable
    const auto perms = fs::status(parent_path).permissions();
    const bool is_writable =
      (perms & (fs::perms::owner_write | fs::perms::group_write |
                fs::perms::others_write)) != fs::perms::none;

    if (!is_writable) {
        error = "Parent path '" + parent_path.string() + "' is not writable";
        return false;
    }

    return true;
}

[[nodiscard]] bool
validate_compression_settings(const ZarrCompressionSettings* settings,
                              std::string& error)
{
    if (settings->compressor >= ZarrCompressorCount) {
        error = "Invalid compressor: " + std::to_string(settings->compressor);
        return false;
    }

    if (settings->codec >= ZarrCompressionCodecCount) {
        error = "Invalid compression codec: " + std::to_string(settings->codec);
        return false;
    }

    // if compressing, we require a compression codec
    if (settings->compressor != ZarrCompressor_None &&
        settings->codec == ZarrCompressionCodec_None) {
        error = "Compression codec must be set when using a compressor";
        return false;
    }

    if (settings->level > 9) {
        error =
          "Invalid compression level: " + std::to_string(settings->level) +
          ". Must be between 0 and 9";
        return false;
    }

    if (settings->shuffle != BLOSC_NOSHUFFLE &&
        settings->shuffle != BLOSC_SHUFFLE &&
        settings->shuffle != BLOSC_BITSHUFFLE) {
        error = "Invalid shuffle: " + std::to_string(settings->shuffle) +
                ". Must be " + std::to_string(BLOSC_NOSHUFFLE) +
                " (no shuffle), " + std::to_string(BLOSC_SHUFFLE) +
                " (byte  shuffle), or " + std::to_string(BLOSC_BITSHUFFLE) +
                " (bit shuffle)";
        return false;
    }

    return true;
}

[[nodiscard]] bool
validate_custom_metadata(std::string_view metadata)
{
    if (metadata.empty()) {
        return false;
    }

    // parse the JSON
    auto val = nlohmann::json::parse(metadata,
                                     nullptr, // callback
                                     false,   // allow exceptions
                                     true     // ignore comments
    );

    if (val.is_discarded()) {
        LOG_ERROR("Invalid JSON: '", metadata, "'");
        return false;
    }

    return true;
}

[[nodiscard]] bool
validate_dimension(const ZarrDimensionProperties* dimension,
                   ZarrVersion version,
                   bool is_append,
                   std::string& error)
{
    if (zarr::is_empty_string(dimension->name, "Dimension name is empty")) {
        error = "Dimension name is empty";
        return false;
    }

    if (dimension->type >= ZarrDimensionTypeCount) {
        error = "Invalid dimension type: " + std::to_string(dimension->type);
        return false;
    }

    if (!is_append && dimension->array_size_px == 0) {
        error = "Array size must be nonzero";
        return false;
    }

    if (dimension->chunk_size_px == 0) {
        error =
          "Invalid chunk size: " + std::to_string(dimension->chunk_size_px);
        return false;
    }

    if (version == ZarrVersion_3 && dimension->shard_size_chunks == 0) {
        error = "Shard size must be nonzero";
        return false;
    }

    if (dimension->scale < 0.0) {
        error = "Scale must be non-negative";
        return false;
    }

    return true;
}

std::string
dimension_type_to_string(ZarrDimensionType type)
{
    switch (type) {
        case ZarrDimensionType_Time:
            return "time";
        case ZarrDimensionType_Channel:
            return "channel";
        case ZarrDimensionType_Space:
            return "space";
        case ZarrDimensionType_Other:
            return "other";
        default:
            return "(unknown)";
    }
}

template<typename T>
[[nodiscard]] ByteVector
scale_image(ConstByteSpan src, size_t& width, size_t& height)
{
    const auto bytes_of_src = src.size();
    const auto bytes_of_frame = width * height * sizeof(T);

    EXPECT(bytes_of_src >= bytes_of_frame,
           "Expecting at least ",
           bytes_of_frame,
           " bytes, got ",
           bytes_of_src);

    const int downscale = 2;
    constexpr auto bytes_of_type = static_cast<double>(sizeof(T));
    const double factor = 0.25;

    const auto w_pad = static_cast<double>(width + (width % downscale));
    const auto h_pad = static_cast<double>(height + (height % downscale));

    const auto size_downscaled =
      static_cast<uint32_t>(w_pad * h_pad * factor * bytes_of_type);

    ByteVector dst(size_downscaled, static_cast<std::byte>(0));
    auto* dst_as_T = reinterpret_cast<T*>(dst.data());
    auto* src_as_T = reinterpret_cast<const T*>(src.data());

    size_t dst_idx = 0;
    for (auto row = 0; row < height; row += downscale) {
        const bool pad_height = (row == height - 1 && height != h_pad);

        for (auto col = 0; col < width; col += downscale) {
            size_t src_idx = row * width + col;
            const bool pad_width = (col == width - 1 && width != w_pad);

            auto here = static_cast<double>(src_as_T[src_idx]);
            auto right = static_cast<double>(
              src_as_T[src_idx + (1 - static_cast<int>(pad_width))]);
            auto down = static_cast<double>(
              src_as_T[src_idx + width * (1 - static_cast<int>(pad_height))]);
            auto diag = static_cast<double>(
              src_as_T[src_idx + width * (1 - static_cast<int>(pad_height)) +
                       (1 - static_cast<int>(pad_width))]);

            dst_as_T[dst_idx++] =
              static_cast<T>(factor * (here + right + down + diag));
        }
    }

    width = static_cast<size_t>(w_pad) / 2;
    height = static_cast<size_t>(h_pad) / 2;

    return dst;
}

template<typename T>
void
average_two_frames(ByteSpan& dst, ConstByteSpan src)
{
    const auto bytes_of_dst = dst.size();
    const auto bytes_of_src = src.size();
    EXPECT(bytes_of_dst == bytes_of_src,
           "Expecting %zu bytes in destination, got %zu",
           bytes_of_src,
           bytes_of_dst);

    T* dst_as_T = reinterpret_cast<T*>(dst.data());
    const T* src_as_T = reinterpret_cast<const T*>(src.data());

    const auto num_pixels = bytes_of_src / sizeof(T);
    for (auto i = 0; i < num_pixels; ++i) {
        dst_as_T[i] = static_cast<T>(0.5 * (dst_as_T[i] + src_as_T[i]));
    }
}
} // namespace

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings)
  : error_()
  , frame_buffer_offset_(0)
{
    EXPECT(validate_settings_(settings), error_);

    commit_settings_(settings);

    start_thread_pool_(settings->max_threads);

    // allocate a frame buffer
    frame_buffer_.resize(
      zarr::bytes_of_frame(*dimensions_, static_cast<ZarrDataType>(dtype_)));

    // create the data store
    EXPECT(create_store_(), error_);

    // create downsampler if needed
    EXPECT(create_downsampler_(), error_);

    // initialize the frame queue
    EXPECT(init_frame_queue_(), error_);

    // allocate writers
    EXPECT(create_writers_(), error_);

    // allocate metadata sinks
    EXPECT(create_metadata_sinks_(), error_);

    // write base metadata
    EXPECT(write_base_metadata_(), error_);

    // write group metadata
    EXPECT(write_group_metadata_(), error_);
}

size_t
ZarrStream::append(const void* data_, size_t nbytes)
{
    EXPECT(error_.empty(), "Cannot append data: ", error_.c_str());

    if (0 == nbytes) {
        return 0;
    }

    auto* data = static_cast<const std::byte*>(data_);

    const size_t bytes_of_frame = frame_buffer_.size();
    size_t bytes_written = 0; // bytes written out of the input data

    while (bytes_written < nbytes) {
        const size_t bytes_remaining = nbytes - bytes_written;

        if (frame_buffer_offset_ > 0) { // add to / finish a partial frame
            const size_t bytes_to_copy =
              std::min(bytes_of_frame - frame_buffer_offset_, bytes_remaining);

            memcpy(frame_buffer_.data() + frame_buffer_offset_,
                   data + bytes_written,
                   bytes_to_copy);
            frame_buffer_offset_ += bytes_to_copy;
            bytes_written += bytes_to_copy;

            // ready to enqueue the frame buffer
            if (frame_buffer_offset_ == bytes_of_frame) {
                std::unique_lock lock(frame_queue_mutex_);
                while (!frame_queue_->push(frame_buffer_) && process_frames_) {
                    frame_queue_not_full_cv_.wait(lock);
                }

                if (process_frames_) {
                    frame_queue_not_empty_cv_.notify_one();
                } else {
                    LOG_DEBUG("Stopping frame processing");
                    break;
                }
                data += bytes_to_copy;
                frame_buffer_offset_ = 0;
            }
        } else if (bytes_remaining < bytes_of_frame) { // begin partial frame
            memcpy(frame_buffer_.data(), data, bytes_remaining);
            frame_buffer_offset_ = bytes_remaining;
            bytes_written += bytes_remaining;
        } else { // at least one full frame
            ConstByteSpan frame(data, bytes_of_frame);

            std::unique_lock lock(frame_queue_mutex_);
            while (!frame_queue_->push(frame) && process_frames_) {
                frame_queue_not_full_cv_.wait(lock);
            }

            if (process_frames_) {
                frame_queue_not_empty_cv_.notify_one();
            } else {
                LOG_DEBUG("Stopping frame processing");
                break;
            }

            bytes_written += bytes_of_frame;
            data += bytes_of_frame;
        }
    }

    return bytes_written;
}

ZarrStatusCode
ZarrStream_s::write_custom_metadata(std::string_view custom_metadata,
                                    bool overwrite)
{
    if (!validate_custom_metadata(custom_metadata)) {
        LOG_ERROR("Invalid custom metadata: '", custom_metadata, "'");
        return ZarrStatusCode_InvalidArgument;
    }

    // check if we have already written custom metadata
    const std::string metadata_key = "acquire.json";
    if (!metadata_sinks_.contains(metadata_key)) { // create metadata sink
        std::string base_path = store_path_;
        if (base_path.starts_with("file://")) {
            base_path = base_path.substr(7);
        }
        const auto prefix = base_path.empty() ? "" : base_path + "/";
        const auto sink_path = prefix + metadata_key;

        if (is_s3_acquisition_()) {
            metadata_sinks_.emplace(
              metadata_key,
              zarr::make_s3_sink(
                s3_settings_->bucket_name, sink_path, s3_connection_pool_));
        } else {
            metadata_sinks_.emplace(metadata_key,
                                    zarr::make_file_sink(sink_path));
        }
    } else if (!overwrite) { // custom metadata already written, don't overwrite
        LOG_ERROR("Custom metadata already written, use overwrite flag");
        return ZarrStatusCode_WillNotOverwrite;
    }

    const auto& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        LOG_ERROR("Metadata sink '" + metadata_key + "' not found");
        return ZarrStatusCode_InternalError;
    }

    const auto metadata_json = nlohmann::json::parse(custom_metadata,
                                                     nullptr, // callback
                                                     false, // allow exceptions
                                                     true   // ignore comments
    );

    const auto metadata_str = metadata_json.dump(4);
    std::span data{ reinterpret_cast<const std::byte*>(metadata_str.data()),
                    metadata_str.size() };
    if (!sink->write(0, data)) {
        LOG_ERROR("Error writing custom metadata");
        return ZarrStatusCode_IOError;
    }
    return ZarrStatusCode_Success;
}

bool
ZarrStream_s::is_s3_acquisition_() const
{
    return s3_settings_.has_value();
}

bool
ZarrStream_s::is_compressed_acquisition_() const
{
    return compression_settings_.has_value();
}

bool
ZarrStream_s::validate_settings_(const struct ZarrStreamSettings_s* settings)
{
    if (!settings) {
        error_ = "Null pointer: settings";
        return false;
    }

    auto version = settings->version;
    if (version < ZarrVersion_2 || version >= ZarrVersionCount) {
        error_ = "Invalid Zarr version: " + std::to_string(version);
        return false;
    }

    if (settings->store_path == nullptr) {
        error_ = "Null pointer: store_path";
        return false;
    }
    std::string_view store_path(settings->store_path);

    // we require the store path (root of the dataset) to be nonempty
    if (store_path.empty()) {
        error_ = "Store path is empty";
        return false;
    }

    if (is_s3_acquisition(settings)) {
        if (!validate_s3_settings(settings->s3_settings, error_)) {
            return false;
        }
    } else if (!validate_filesystem_store_path(store_path, error_)) {
        return false;
    }

    if (settings->data_type >= ZarrDataTypeCount) {
        error_ = "Invalid data type: " + std::to_string(settings->data_type);
        return false;
    }

    if (is_compressed_acquisition(settings) &&
        !validate_compression_settings(settings->compression_settings,
                                       error_)) {
        return false;
    }

    if (settings->dimensions == nullptr) {
        error_ = "Null pointer: dimensions";
        return false;
    }

    // we must have at least 3 dimensions
    const size_t ndims = settings->dimension_count;
    if (ndims < 3) {
        error_ = "Invalid number of dimensions: " + std::to_string(ndims) +
                 ". Must be at least 3";
        return false;
    }

    // check the final dimension (width), must be space
    if (settings->dimensions[ndims - 1].type != ZarrDimensionType_Space) {
        error_ = "Last dimension must be of type Space";
        return false;
    }

    // check the penultimate dimension (height), must be space
    if (settings->dimensions[ndims - 2].type != ZarrDimensionType_Space) {
        error_ = "Second to last dimension must be of type Space";
        return false;
    }

    // validate the dimensions individually
    for (size_t i = 0; i < ndims; ++i) {
        if (!validate_dimension(
              settings->dimensions + i, version, i == 0, error_)) {
            return false;
        }
    }

    return true;
}

void
ZarrStream_s::commit_settings_(const struct ZarrStreamSettings_s* settings)
{
    version_ = settings->version;
    store_path_ = zarr::trim(settings->store_path);

    if (is_s3_acquisition(settings)) {
        s3_settings_ = construct_s3_settings(settings->s3_settings);
    }

    if (is_compressed_acquisition(settings)) {
        compression_settings_ = {
            .compressor = settings->compression_settings->compressor,
            .codec = settings->compression_settings->codec,
            .level = settings->compression_settings->level,
            .shuffle = settings->compression_settings->shuffle,
        };
    }

    dtype_ = settings->data_type;

    std::vector<ZarrDimension> dims;
    for (auto i = 0; i < settings->dimension_count; ++i) {
        const auto& dim = settings->dimensions[i];
        std::string unit = "";
        if (dim.unit) {
            unit = zarr::trim(dim.unit);
        }

        double scale = dim.scale == 0.0 ? 1.0 : dim.scale;

        dims.emplace_back(dim.name,
                          dim.type,
                          dim.array_size_px,
                          dim.chunk_size_px,
                          dim.shard_size_chunks,
                          unit,
                          scale);
    }
    dimensions_ = std::make_shared<ArrayDimensions>(std::move(dims), dtype_);

    multiscale_ = settings->multiscale;
}

void
ZarrStream_s::start_thread_pool_(uint32_t max_threads)
{
    max_threads =
      max_threads == 0 ? std::thread::hardware_concurrency() : max_threads;
    if (max_threads == 0) {
        LOG_WARNING("Unable to determine hardware concurrency, using 1 thread");
        max_threads = 1;
    }

    thread_pool_ = std::make_shared<zarr::ThreadPool>(
      max_threads, [this](const std::string& err) { this->set_error_(err); });
}

void
ZarrStream_s::set_error_(const std::string& msg)
{
    error_ = msg;
}

bool
ZarrStream_s::create_store_()
{
    if (is_s3_acquisition_()) {
        // spin up S3 connection pool
        try {
            s3_connection_pool_ = std::make_shared<zarr::S3ConnectionPool>(
              std::thread::hardware_concurrency(), *s3_settings_);
        } catch (const std::exception& e) {
            set_error_("Error creating S3 connection pool: " +
                       std::string(e.what()));
            return false;
        }

        // test the S3 connection
        auto conn = s3_connection_pool_->get_connection();
        if (!conn->is_connection_valid()) {
            set_error_("Failed to connect to S3");
            return false;
        }
        s3_connection_pool_->return_connection(std::move(conn));
    } else {
        if (fs::exists(store_path_)) {
            // remove everything inside the store path
            std::error_code ec;
            fs::remove_all(store_path_, ec);

            if (ec) {
                set_error_("Failed to remove existing store path '" +
                           store_path_ + "': " + ec.message());
                return false;
            }
        }

        // create the store path
        {
            std::error_code ec;
            if (!fs::create_directories(store_path_, ec)) {
                set_error_("Failed to create store path '" + store_path_ +
                           "': " + ec.message());
                return false;
            }
        }
    }

    return true;
}

bool
ZarrStream_s::init_frame_queue_()
{
    if (frame_queue_) {
        return true; // already initialized
    }

    if (!thread_pool_) {
        set_error_("Thread pool is not initialized");
        return false;
    }

    const auto frame_size = dimensions_->width_dim().array_size_px *
                            dimensions_->height_dim().array_size_px *
                            zarr::bytes_of_type(dtype_);

    // cap the frame buffer at 2 GiB, or 10 frames, whichever is larger
    const auto buffer_size_bytes = 2ULL << 30;
    const auto frame_count = std::max(10ULL, buffer_size_bytes / frame_size);

    try {
        frame_queue_ =
          std::make_unique<zarr::FrameQueue>(frame_count, frame_size);

        EXPECT(thread_pool_->push_job([this](std::string& err) {
            try {
                process_frame_queue_();
            } catch (const std::exception& e) {
                err = e.what();
                return false;
            }

            return true;
        }),
               "Failed to push job to thread pool.");
    } catch (const std::exception& e) {
        set_error_("Error creating frame queue: " + std::string(e.what()));
        return false;
    }

    return true;
}

bool
ZarrStream_s::create_writers_()
{
    writers_.clear();

    if (downsampler_) {
        const auto& configs = downsampler_->writer_configurations();
        writers_.resize(configs.size());

        for (const auto& [lod, config] : configs) {
            if (version_ == 2) {
                writers_[lod] = std::make_unique<zarr::ZarrV2ArrayWriter>(
                  config, thread_pool_, s3_connection_pool_);
            } else {
                writers_[lod] = std::make_unique<zarr::ZarrV3ArrayWriter>(
                  config, thread_pool_, s3_connection_pool_);
            }
        }
    } else {
        const auto config = make_array_writer_config_();

        if (version_ == 2) {
            writers_.push_back(std::make_unique<zarr::ZarrV2ArrayWriter>(
              config, thread_pool_, s3_connection_pool_));
        } else {
            writers_.push_back(std::make_unique<zarr::ZarrV3ArrayWriter>(
              config, thread_pool_, s3_connection_pool_));
        }
    }

    return true;
}

bool
ZarrStream_s::create_downsampler_()
{
    if (!multiscale_) {
        return true;
    }

    const auto config = make_array_writer_config_();

    try {
        downsampler_ = zarr::Downsampler(config);
    } catch (const std::exception& exc) {
        set_error_("Error creating downsampler: " + std::string(exc.what()));
        return false;
    }

    return true;
}

bool
ZarrStream_s::create_metadata_sinks_()
{
    try {
        if (s3_connection_pool_) {
            if (!make_metadata_s3_sinks(version_,
                                        s3_settings_->bucket_name,
                                        store_path_,
                                        s3_connection_pool_,
                                        metadata_sinks_)) {
                set_error_("Error creating metadata sinks");
                return false;
            }
        } else {
            if (!make_metadata_file_sinks(
                  version_, store_path_, thread_pool_, metadata_sinks_)) {
                set_error_("Error creating metadata sinks");
                return false;
            }
        }
    } catch (const std::exception& e) {
        set_error_("Error creating metadata sinks: " + std::string(e.what()));
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_base_metadata_()
{
    nlohmann::json metadata;
    std::string metadata_key;

    if (version_ == 2) {
        metadata["multiscales"] = make_ome_metadata_();

        metadata_key = ".zattrs";
    } else {
        metadata["extensions"] = nlohmann::json::array();
        metadata["metadata_encoding"] =
          "https://purl.org/zarr/spec/protocol/core/3.0";
        metadata["metadata_key_suffix"] = ".json";
        metadata["zarr_format"] =
          "https://purl.org/zarr/spec/protocol/core/3.0";

        metadata_key = "zarr.json";
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    std::span data{ reinterpret_cast<const std::byte*>(metadata_str.data()),
                    metadata_str.size() };

    if (!sink->write(0, data)) {
        set_error_("Error writing base metadata");
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_group_metadata_()
{
    nlohmann::json metadata;
    std::string metadata_key;

    if (version_ == 2) {
        metadata = { { "zarr_format", 2 } };

        metadata_key = ".zgroup";
    } else {
        const auto multiscales = make_ome_metadata_();
        metadata["attributes"]["ome"] = multiscales;
        metadata["zarr_format"] = 3;
        metadata["consolidated_metadata"] = nullptr;
        metadata["node_type"] = "group";

        metadata_key = "zarr.json";
    }

    const std::unique_ptr<zarr::Sink>& sink = metadata_sinks_.at(metadata_key);
    if (!sink) {
        set_error_("Metadata sink '" + metadata_key + "'not found");
        return false;
    }

    const std::string metadata_str = metadata.dump(4);
    std::span data{ reinterpret_cast<const std::byte*>(metadata_str.data()),
                    metadata_str.size() };
    if (!sink->write(0, data)) {
        set_error_("Error writing group metadata");
        return false;
    }

    return true;
}

nlohmann::json
ZarrStream_s::make_ome_metadata_() const
{
    nlohmann::json multiscales;
    const auto ndims = dimensions_->ndims();

    auto& axes = multiscales[0]["axes"];
    for (auto i = 0; i < ndims; ++i) {
        const auto& dim = dimensions_->at(i);
        const auto type = dimension_type_to_string(dim.type);
        const std::string unit = dim.unit.has_value() ? *dim.unit : "";

        if (!unit.empty()) {
            axes.push_back({
              { "name", dim.name.c_str() },
              { "type", type },
              { "unit", unit.c_str() },
            });
        } else {
            axes.push_back({ { "name", dim.name.c_str() }, { "type", type } });
        }
    }

    // spatial multiscale metadata
    std::vector<double> scales(ndims);
    for (auto i = 0; i < ndims; ++i) {
        const auto& dim = dimensions_->at(i);
        scales[i] = dim.scale;
    }

    multiscales[0]["datasets"] = {
        {
          { "path", "0" },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        },
    };

    for (auto i = 1; i < writers_.size(); ++i) {
        const auto& config = downsampler_->writer_configurations().at(i);

        for (auto j = 0; j < ndims; ++j) {
            const auto& base_dim = dimensions_->at(j);
            const auto& down_dim = config.dimensions->at(j);
            if (base_dim.type != ZarrDimensionType_Space) {
                continue;
            }

            const auto base_size = base_dim.array_size_px;
            const auto down_size = down_dim.array_size_px;
            const auto ratio = (base_size + down_size - 1) / down_size;

            // scale by next power of 2
            scales[j] = base_dim.scale * std::bit_ceil(ratio);
        }

        multiscales[0]["datasets"].push_back({
          { "path", std::to_string(i) },
          { "coordinateTransformations",
            {
              {
                { "type", "scale" },
                { "scale", scales },
              },
            } },
        });

        // downsampling metadata
        multiscales[0]["type"] = "local_mean";
        multiscales[0]["metadata"] = {
            { "description",
              "The fields in the metadata describe how to reproduce this "
              "multiscaling in scikit-image. The method and its parameters "
              "are "
              "given here." },
            { "method", "skimage.transform.downscale_local_mean" },
            { "version", "0.21.0" },
            { "args", "[2]" },
            { "kwargs", { "cval", 0 } },
        };
    }

    if (version_ == ZarrVersion_2) {
        multiscales[0]["version"] = "0.4";
        multiscales[0]["name"] = "/";
        return multiscales;
    }

    nlohmann::json ome;
    ome["version"] = "0.5";
    ome["name"] = "/";
    ome["multiscales"] = multiscales;

    return ome;
}

zarr::ArrayWriterConfig
ZarrStream_s::make_array_writer_config_() const
{
    // construct Blosc compression parameters
    std::optional<zarr::BloscCompressionParams> blosc_compression_params;
    if (is_compressed_acquisition_()) {
        blosc_compression_params = zarr::BloscCompressionParams(
          zarr::blosc_codec_to_string(compression_settings_->codec),
          compression_settings_->level,
          compression_settings_->shuffle);
    }

    std::optional<std::string> s3_bucket_name;
    if (is_s3_acquisition_()) {
        s3_bucket_name = s3_settings_->bucket_name;
    }

    return {
        .dimensions = dimensions_,
        .dtype = dtype_,
        .level_of_detail = 0,
        .bucket_name = s3_bucket_name,
        .store_path = store_path_,
        .compression_params = blosc_compression_params,
    };
}

void
ZarrStream_s::process_frame_queue_()
{
    if (!frame_queue_) {
        set_error_("Frame queue is not initialized");
        return;
    }

    const auto bytes_of_frame = frame_buffer_.size();

    std::vector<std::byte> frame;
    while (process_frames_ || !frame_queue_->empty()) {
        std::unique_lock lock(frame_queue_mutex_);
        while (frame_queue_->empty() && process_frames_) {
            frame_queue_not_empty_cv_.wait(lock);
        }

        if (!process_frames_ && frame_queue_->empty()) {
            break;
        }

        if (!frame_queue_->pop(frame)) {
            continue;
        }

        // Signal that there's space available in the queue
        frame_queue_not_full_cv_.notify_one();

        EXPECT(write_frame_(frame) == bytes_of_frame,
               "Failed to write frame to writer");
    }

    CHECK(frame_queue_->empty());
    std::unique_lock lock(frame_queue_mutex_);
    frame_queue_finished_cv_.notify_all();
}

void
ZarrStream_s::finalize_frame_queue_()
{
    process_frames_ = false;

    // Wake up all potentially waiting threads
    {
        std::unique_lock lock(frame_queue_mutex_);
        frame_queue_not_empty_cv_.notify_all();
        frame_queue_not_full_cv_.notify_all();
    }

    // Wait for frame processing to complete
    std::unique_lock lock(frame_queue_mutex_);
    frame_queue_finished_cv_.wait(lock,
                                  [this] { return frame_queue_->empty(); });
}

size_t
ZarrStream_s::write_frame_(ConstByteSpan data)
{
    const auto bytes_of_full_frame = frame_buffer_.size();
    const auto n_bytes = writers_[0]->write_frame(data);
    EXPECT(n_bytes == bytes_of_full_frame, "");

    if (n_bytes != data.size()) {
        set_error_("Incomplete write to full-resolution array.");
        return n_bytes;
    }

    write_multiscale_frames_(data);
    return n_bytes;
}

void
ZarrStream_s::write_multiscale_frames_(ConstByteSpan data)
{
    if (!multiscale_) {
        return;
    }

    CHECK(downsampler_.has_value());
    downsampler_->add_frame(data);

    for (auto i = 1; i < writers_.size(); ++i) {
        ByteVector downsampled_frame;
        if (downsampler_->get_downsampled_frame(i, downsampled_frame)) {
            const auto n_bytes = writers_[i]->write_frame(downsampled_frame);
            EXPECT(n_bytes == downsampled_frame.size(),
                   "Expected to write ",
                   downsampled_frame.size(),
                   " bytes to multiscale array ",
                   i,
                   "wrote ",
                   n_bytes);
        }
    }
}

bool
finalize_stream(struct ZarrStream_s* stream)
{
    if (stream == nullptr) {
        LOG_INFO("Stream is null. Nothing to finalize.");
        return true;
    }

    if (!stream->write_group_metadata_()) {
        LOG_ERROR("Error finalizing Zarr stream: ", stream->error_);
        return false;
    }

    for (auto& [sink_name, sink] : stream->metadata_sinks_) {
        if (!finalize_sink(std::move(sink))) {
            LOG_ERROR("Error finalizing Zarr stream. Failed to write ",
                      sink_name);
            return false;
        }
    }
    stream->metadata_sinks_.clear();

    stream->finalize_frame_queue_();

    for (auto i = 0; i < stream->writers_.size(); ++i) {
        if (!finalize_array(std::move(stream->writers_[i]))) {
            LOG_ERROR("Error finalizing Zarr stream. Failed to write array ",
                      i);
            return false;
        }
    }
    stream->writers_.clear(); // flush before shutting down thread pool
    stream->thread_pool_->await_stop();

    return true;
}
