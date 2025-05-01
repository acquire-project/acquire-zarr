#include "acquire.zarr.h"
#include "macros.hh"
#include "sink.hh"
#include "v2.group.hh"
#include "v3.group.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"

#include <bit> // bit_ceil
#include <filesystem>
#include <regex>

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

std::optional<zarr::BloscCompressionParams>
make_compression_params(const ZarrCompressionSettings* settings)
{
    if (!settings) {
        return std::nullopt;
    }

    return zarr::BloscCompressionParams(
      zarr::blosc_codec_to_string(settings->codec),
      settings->level,
      settings->shuffle);
}

std::shared_ptr<ArrayDimensions>
make_array_dimensions(const ZarrDimensionProperties* dimensions,
                      size_t dimension_count,
                      ZarrDataType data_type)
{
    std::vector<ZarrDimension> dims;
    for (auto i = 0; i < dimension_count; ++i) {
        const auto& dim = dimensions[i];
        std::string unit;
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
    return std::make_shared<ArrayDimensions>(std::move(dims), data_type);
}

bool
is_valid_zarr_key(const std::string& key, std::string& error)
{
    // https://zarr-specs.readthedocs.io/en/latest/v3/core/index.html#node-names

    // key cannot be empty
    if (key.empty()) {
        error = "Key is empty";
        return false;
    }

    // key cannot end with '/'
    if (key.back() == '/') {
        error = "Key ends in '/'";
        return false;
    }

    if (key.find('/') != std::string::npos) {
        // path has slashes, check each segment
        std::string segment;
        std::istringstream stream(key);

        while (std::getline(stream, segment, '/')) {
            // skip empty segments (like in "/foo" where there's an empty
            // segment at start)
            if (segment.empty()) {
                continue;
            }

            // segment must not be composed only of periods
            if (std::regex_match(segment, std::regex("^\\.+$"))) {
                error = "Invalid key segment '" + segment + "'";
                return false;
            }

            // segment must not start with "__"
            if (segment.substr(0, 2) == "__") {
                error =
                  "Key segment '" + segment + "' has reserved prefix '__'";
                return false;
            }
        }
    } else { // simple name, apply node name rules
        // must not be composed only of periods
        if (std::regex_match(key, std::regex("^\\.+$"))) {
            error = "Invalid key '" + key + "'";
            return false;
        }

        // must not start with "__"
        if (key.substr(0, 2) == "__") {
            error = " Key '" + key + "' has reserved prefix '__'";
            return false;
        }
    }

    // check that all characters are in recommended set
    std::regex valid_chars("^[a-zA-Z0-9_.-]*$");

    // for paths, apply to each segment
    if (key.find('/') != std::string::npos) {
        std::string segment;
        std::istringstream stream(key);

        while (std::getline(stream, segment, '/')) {
            if (!segment.empty() && !std::regex_match(segment, valid_chars)) {
                error = "Key segment '" + segment +
                        "' contains invalid characters (should use only a-z, "
                        "A-Z, 0-9, -, _, .)";
                return false;
            }
        }
    } else {
        // for simple names
        if (!std::regex_match(key, valid_chars)) {
            error = "Key '" + key +
                    "' contains invalid characters (should use only a-z, A-Z, "
                    "0-9, -, _, .)";
            return false;
        }
    }

    return true;
}

std::string
regularize_key(std::string_view key)
{
    std::string regularized_key = zarr::trim(key);

    // replace multiple consecutive slashes with single slashes
    regularized_key =
      std::regex_replace(regularized_key, std::regex("\\/+"), "/");

    // remove leading slash
    regularized_key =
      std::regex_replace(regularized_key, std::regex("^\\/"), "");

    // remove trailing slash
    regularized_key =
      std::regex_replace(regularized_key, std::regex("\\/$"), "");

    return regularized_key;
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

    version_ = settings->version;
    store_path_ = zarr::trim(settings->store_path);

    if (is_s3_acquisition(settings)) {
        s3_settings_ = construct_s3_settings(settings->s3_settings);
    }

    start_thread_pool_(settings->max_threads);

    // create the data store
    EXPECT(create_store_(), error_);

    EXPECT(create_root_group_(settings), error_);

    // compute frame size
    const auto& width_dim = settings->dimensions[settings->dimension_count - 1];
    const auto& height_dim =
      settings->dimensions[settings->dimension_count - 2];

    const auto frame_size = width_dim.array_size_px * height_dim.array_size_px *
                            zarr::bytes_of_type(settings->data_type);

    // allocate the frame buffer
    frame_buffer_.resize(frame_size);

    // initialize the frame queue
    EXPECT(init_frame_queue_(frame_size), error_);

    // allocate metadata sinks
    EXPECT(create_metadata_sinks_(), error_);

    // write base metadata
    EXPECT(write_base_metadata_(), error_);
}

size_t
ZarrStream::append_to_node(std::string_view key_view,
                           const void* data_,
                           size_t nbytes)
{
    EXPECT(error_.empty(), "Cannot append data: ", error_.c_str());

    if (0 == nbytes) {
        return 0;
    }

    const std::string key = regularize_key(key_view);
    EXPECT(switch_node_(key), "Failed to switch to node '" + key + "'");

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

ZarrStatusCode
ZarrStream_s::configure_group(const ZarrGroupProperties* properties)
{
    const std::string key = regularize_key(properties->store_key);

    std::string error;
    if (!key.empty() && !is_valid_zarr_key(key, error)) {
        return ZarrStatusCode_InvalidArgument;
    }

    if (has_node_(key)) {
        error = "Node with key '" + key + "' already exists.";
        return ZarrStatusCode_InvalidArgument;
    }

    if (!ensure_parent_groups_exist_(key, error)) {
        return ZarrStatusCode_InternalError;
    }

    if (!validate_node_properties_(properties, version_, error)) {
        LOG_ERROR(error);
        return ZarrStatusCode_InvalidArgument;
    }

    auto dimensions = make_array_dimensions(properties->dimensions,
                                            properties->dimension_count,
                                            properties->data_type);

    std::optional<std::string> s3_bucket_name;
    if (is_s3_acquisition_()) {
        s3_bucket_name = s3_settings_->bucket_name;
    }

    auto blosc_compression_params =
      make_compression_params(properties->compression_settings);

    zarr::GroupConfig config{
        .dimensions = dimensions,
        .dtype = properties->data_type,
        .multiscale = properties->multiscale,
        .bucket_name = s3_bucket_name,
        .store_root = store_path_,
        .group_key = key,
        .compression_params = blosc_compression_params,
    };

    std::unique_ptr<zarr::Group> group;
    try {
        if (version_ == ZarrVersion_2) {
            group = std::make_unique<zarr::V2Group>(
              config, thread_pool_, s3_connection_pool_);
        } else {
            group = std::make_unique<zarr::V3Group>(
              config, thread_pool_, s3_connection_pool_);
        }

        groups_.emplace(key, std::move(group));
    } catch (const std::exception& exc) {
        set_error_("Failed to create group: " + std::string(exc.what()));
        return ZarrStatusCode_InternalError;
    }

    return ZarrStatusCode_Success;
}

ZarrStatusCode
ZarrStream_s::configure_array(const ZarrArrayProperties* properties)
{
    const std::string key = regularize_key(properties->store_key);

    std::string error;
    if (!is_valid_zarr_key(key, error)) {
        return ZarrStatusCode_InvalidArgument;
    }

    if (has_node_(key)) {
        error = "Node with key '" + key + "' already exists.";
        return ZarrStatusCode_InvalidArgument;
    }

    if (!ensure_parent_groups_exist_(key, error)) {
        return ZarrStatusCode_InternalError;
    }

    auto dimensions = make_array_dimensions(properties->dimensions,
                                            properties->dimension_count,
                                            properties->data_type);

    std::optional<std::string> s3_bucket_name;
    if (is_s3_acquisition_()) {
        s3_bucket_name = s3_settings_->bucket_name;
    }

    auto blosc_compression_params =
      make_compression_params(properties->compression_settings);

    zarr::ArrayConfig config{
        .dimensions = dimensions,
        .dtype = properties->data_type,
        .bucket_name = s3_bucket_name,
        .store_root = store_path_,
        .group_key = key, // FIXME: should be the parent of this
        .compression_params = blosc_compression_params,
    };

    std::unique_ptr<zarr::Array> array;
    try {
        if (version_ == ZarrVersion_2) {
            array = std::make_unique<zarr::V2Array>(
              config, thread_pool_, s3_connection_pool_);
        } else {
            array = std::make_unique<zarr::V3Array>(
              config, thread_pool_, s3_connection_pool_);
        }

        arrays_.emplace(key, std::move(array));
    } catch (const std::exception& exc) {
        set_error_("Failed to create array: " + std::string(exc.what()));
        return ZarrStatusCode_InternalError;
    }

    return ZarrStatusCode_Success;
}

bool
ZarrStream_s::is_s3_acquisition_() const
{
    return s3_settings_.has_value();
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

    ZarrGroupProperties root_group_properties{
        .store_key = "",
        .data_type = settings->data_type,
        .multiscale = settings->multiscale,
        .compression_settings = settings->compression_settings,
        .dimensions = settings->dimensions,
        .dimension_count = settings->dimension_count,
    };

    return validate_node_properties_(
      &root_group_properties,
      static_cast<ZarrVersion>(settings->version),
      error_);
}

bool
ZarrStream_s::create_root_group_(const struct ZarrStreamSettings_s* settings)
{
    ZarrGroupProperties root_group_props{
        .store_key = "",
        .data_type = settings->data_type,
        .multiscale = settings->multiscale,
        .compression_settings = settings->compression_settings,
        .dimensions = settings->dimensions,
        .dimension_count = settings->dimension_count,
    };

    if (configure_group(&root_group_props) != ZarrStatusCode_Success) {
        set_error_("Failed to configure root group");
        return false;
    }

    return true;
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
ZarrStream_s::init_frame_queue_(size_t frame_size)
{
    if (frame_queue_) {
        return true; // already initialized
    }

    if (!thread_pool_) {
        set_error_("Thread pool is not initialized");
        return false;
    }

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
        metadata["multiscales"] = groups_.at("")->get_ome_metadata();

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

        EXPECT(groups_.at("")->write_frame(frame) == bytes_of_frame,
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

bool
ZarrStream_s::has_node_(std::string_view key)
{
    std::string key_str(key);

    const auto group_it = groups_.find(key_str);
    if (group_it != groups_.end()) {
        return true;
    }

    const auto array_it = arrays_.find(key_str);
    if (array_it != arrays_.end()) {
        return true;
    }

    return false;
}

void
ZarrStream_s::close_current_node_()
{
    if (!active_node_key_) { // no current active node
        return;
    }

    const auto group_it = groups_.find(*active_node_key_);
    if (group_it != groups_.end()) {
        EXPECT(groups_.at(*active_node_key_)->close(),
               "Failed to close group ",
               *active_node_key_);
    }

    const auto array_it = arrays_.find(*active_node_key_);
    if (array_it != arrays_.end()) {
        EXPECT(arrays_.at(*active_node_key_)->close(),
               "Failed to close array ",
               *active_node_key_);
    }

    active_node_key_.reset();
}

bool
ZarrStream_s::switch_node_(std::string_view key)
{
    if (key == active_node_key_) {
        return true; // already on the right node
    }
    const std::string key_str(key);

    const auto group_it = groups_.find(key_str);
    if (group_it != groups_.end()) {
        close_current_node_();
        active_node_key_ = key;
        return true;
    }

    const auto array_it = arrays_.find(key_str);
    if (array_it != arrays_.end()) {
        close_current_node_();
        active_node_key_ = key;
        return true;
    }

    LOG_ERROR("Node not found: ", key);
    return false;
}

bool
ZarrStream_s::validate_compression_settings_(
  const ZarrCompressionSettings* settings,
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

bool
ZarrStream_s::validate_dimension_(const ZarrDimensionProperties* dimension,
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

    if (version_ == ZarrVersion_3 && dimension->shard_size_chunks == 0) {
        error = "Shard size must be nonzero";
        return false;
    }

    if (dimension->scale < 0.0) {
        error = "Scale must be non-negative";
        return false;
    }

    return true;
}

bool
ZarrStream_s::ensure_parent_groups_exist_(const std::string& key,
                                          std::string& error)
{
    // Skip if key is empty or just the root
    if (key.empty() || key == "/") {
        return true;
    }

    // Split the key into components
    std::vector<std::string> segments;
    size_t start = 0;
    size_t end = key.find('/');

    while (end != std::string::npos) {
        if (end > start) {                          // akip empty segments
            segments.push_back(key.substr(0, end)); // get all parent paths
        }
        start = end + 1;
        end = key.find('/', start);
    }

    std::optional<std::string> bucket_name;
    if (is_s3_acquisition_()) {
        bucket_name = s3_settings_->bucket_name;
    }

    // create any missing parent groups
    for (const auto& parent_path : segments) {
        // skip if this group already exists
        if (has_node_(parent_path)) {
            // check that it's not an array (arrays can't have children)
            if (arrays_.find(parent_path) != arrays_.end()) {
                error = "Path conflict: '" + parent_path +
                        "' is an array and cannot contain other nodes.";
                return false;
            }
            // it's a group, so we can continue
            continue;
        }

        // create the group at this path level
        zarr::GroupConfig config{
            .dimensions = nullptr,
            .dtype = ZarrDataTypeCount,
            .multiscale = false,
            .bucket_name = bucket_name,
            .store_root = store_path_,
            .group_key = parent_path,
            .compression_params = std::nullopt,
        };

        // use default properties for intermediate groups
        std::unique_ptr<zarr::Group> parent_group;
        if (version_ == ZarrVersion_2) {
            parent_group = std::make_unique<zarr::V2Group>(
              config, thread_pool_, s3_connection_pool_);
        } else {
            parent_group = std::make_unique<zarr::V3Group>(
              config, thread_pool_, s3_connection_pool_);
        }

        groups_.emplace(parent_path, std::move(parent_group));
    }

    return true;
}

bool
finalize_stream(struct ZarrStream_s* stream)
{
    if (stream == nullptr) {
        LOG_INFO("Stream is null. Nothing to finalize.");
        return true;
    }

    for (auto& [sink_name, sink] : stream->metadata_sinks_) {
        if (!zarr::finalize_sink(std::move(sink))) {
            LOG_ERROR("Error finalizing Zarr stream. Failed to write ",
                      sink_name);
            return false;
        }
    }
    stream->metadata_sinks_.clear();

    stream->finalize_frame_queue_();

    for (auto& [group_key, group] : stream->groups_) {
        if (!zarr::finalize_group(std::move(group))) {
            std::string err_msg;
            if (group_key.empty()) {
                err_msg = "Failed to close root group";
            } else {
                err_msg = "Failed to close group '" + group_key + "'";
            }
            LOG_ERROR("Error finalizing Zarr stream: " + err_msg);
            return false;
        }
    }
    stream->groups_.clear(); // flush before shutting down thread pool

    for (auto& [array_key, array] : stream->arrays_) {
        if (!zarr::finalize_array(std::move(array))) {
            std::string err_msg = "Failed to close array '" + array_key + "'";
            LOG_ERROR("Error finalizing Zarr stream: " + err_msg);
            return false;
        }
    }
    stream->arrays_.clear(); // flush before shutting down thread pool

    stream->thread_pool_->await_stop();

    return true;
}
