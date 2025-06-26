#include "acquire.zarr.h"
#include "array.base.hh"
#include "macros.hh"
#include "sink.hh"
#include "v2.multiscale.array.hh"
#include "v3.multiscale.array.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"

#include <blosc.h>

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

zarr::S3Settings
make_s3_settings(const ZarrS3Settings* settings)
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
    if (settings == nullptr) { // no compression, OK
        return true;
    }

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
                error = "Key segment contains only periods";
                return false;
            }

            // segment must not start with "__"
            if (segment.substr(0, 2) == "__") {
                error = "Key segment has reserved prefix '__'";
                return false;
            }
        }
    } else { // simple name, apply node name rules
        // must not be composed only of periods
        if (std::regex_match(key, std::regex("^\\.+$"))) {
            error = "Key contains only periods";
            return false;
        }

        // must not start with "__"
        if (key.substr(0, 2) == "__") {
            error = " Key has reserved prefix '__'";
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
                error = "Key segment contains invalid characters (should use "
                        "only a-z, A-Z, 0-9, -, _, .)";
                return false;
            }
        }
    } else {
        // for simple names
        if (!std::regex_match(key, valid_chars)) {
            error = "Key contains invalid characters (should use only a-z, "
                    "A-Z, 0-9, -, _, .)";
            return false;
        }
    }

    return true;
}

std::string
regularize_key(const char* key)
{
    if (key == nullptr) {
        return "";
    }

    std::string regularized_key{ key };

    // replace leading and trailing whitespace and/or slashes
    regularized_key = std::regex_replace(
      regularized_key, std::regex(R"(^(\s|\/)+|(\s|\/)+$)"), "");

    // replace multiple consecutive slashes with single slashes
    regularized_key =
      std::regex_replace(regularized_key, std::regex(R"(\/+)"), "/");

    return regularized_key;
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

[[nodiscard]] bool
validate_array_settings(const ZarrArraySettings* settings,
                        ZarrVersion version,
                        std::string& error)
{
    if (settings == nullptr) {
        error = "Null pointer: settings";
        return false;
    }

    std::string key = regularize_key(settings->output_key);
    if (!key.empty() && !is_valid_zarr_key(key, error)) {
        error = "Invalid output key: '" + key + "': " + error;
        return false;
    }

    if (!validate_compression_settings(settings->compression_settings, error)) {
        return false;
    }

    if (settings->dimensions == nullptr) {
        error = "Null pointer: dimensions";
        return false;
    }

    // we must have at least 3 dimensions
    const size_t ndims = settings->dimension_count;
    if (ndims < 3) {
        error = "Invalid number of dimensions: " + std::to_string(ndims) +
                ". Must be at least 3";
        return false;
    }

    // check the final dimension (width), must be space
    if (settings->dimensions[ndims - 1].type != ZarrDimensionType_Space) {
        error = "Last dimension must be of type Space";
        return false;
    }

    // check the penultimate dimension (height), must be space
    if (settings->dimensions[ndims - 2].type != ZarrDimensionType_Space) {
        error = "Second to last dimension must be of type Space";
        return false;
    }

    // validate the dimensions individually
    for (size_t i = 0; i < ndims; ++i) {
        if (!validate_dimension(
              settings->dimensions + i, version, i == 0, error)) {
            return false;
        }
    }

    // we don't care about downsampling method if not multiscale
    if (settings->multiscale &&
        settings->downsampling_method >= ZarrDownsamplingMethodCount) {
        error = "Invalid downsampling method: " +
                std::to_string(settings->downsampling_method);
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

    ByteVector dst(size_downscaled, 0);
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
{
    EXPECT(validate_settings_(settings), error_);

    start_thread_pool_(settings->max_threads);

    // commit settings and create the output store
    EXPECT(commit_settings_(settings), error_);

    // initialize the frame queue
    EXPECT(init_frame_queue_(), error_);
}

size_t
ZarrStream::append(const void* data_, size_t nbytes)
{
    const std::string output_key = output_arrays_.begin()->first;
    return append(output_key, data_, nbytes);
}

size_t
ZarrStream::append(const std::string& key, const void* data_, size_t nbytes)
{
    EXPECT(error_.empty(), "Cannot append data: ", error_.c_str());
    auto it = output_arrays_.find(key);
    EXPECT(it != output_arrays_.end(),
           "Cannot append data: key '",
           key,
           "' not configured");
    EXPECT(data_ != nullptr, "Cannot append data: data pointer is null");

    if (nbytes == 0) {
        return 0;
    }

    auto& output = it->second;
    auto& frame_buffer = output.frame_buffer;
    auto& frame_buffer_offset = output.frame_buffer_offset;

    auto* data = static_cast<const uint8_t*>(data_);

    const size_t bytes_of_frame = frame_buffer.size();
    size_t bytes_written = 0; // bytes written out of the input data

    while (bytes_written < nbytes) {
        const size_t bytes_remaining = nbytes - bytes_written;

        if (frame_buffer_offset > 0) { // add to / finish a partial frame
            const size_t bytes_to_copy =
              std::min(bytes_of_frame - frame_buffer_offset, bytes_remaining);

            frame_buffer.assign_at(frame_buffer_offset,
                                   { data + bytes_written, bytes_to_copy });
            frame_buffer_offset += bytes_to_copy;
            bytes_written += bytes_to_copy;

            // ready to enqueue the frame buffer
            if (frame_buffer_offset == bytes_of_frame) {
                std::unique_lock lock(frame_queue_mutex_);
                while (!frame_queue_->push(frame_buffer, key) &&
                       process_frames_) {
                    frame_queue_not_full_cv_.wait(lock);
                }
                frame_buffer.resize(bytes_of_frame);

                if (process_frames_) {
                    frame_queue_not_empty_cv_.notify_one();
                } else {
                    LOG_DEBUG("Stopping frame processing");
                    break;
                }
                data += bytes_to_copy;
                frame_buffer_offset = 0;
            }
        } else if (bytes_remaining < bytes_of_frame) { // begin partial frame
            frame_buffer.assign_at(0, { data, bytes_remaining });
            frame_buffer_offset = bytes_remaining;
            bytes_written += bytes_remaining;
        } else { // at least one full frame
            zarr::LockedBuffer frame;
            frame.assign({ data, bytes_of_frame });

            std::unique_lock lock(frame_queue_mutex_);
            while (!frame_queue_->push(frame, key) && process_frames_) {
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
    if (!custom_metadata_sink_) {
        const std::string metadata_key = "acquire.json";
        std::string base_path = store_path_;
        if (base_path.starts_with("file://")) {
            base_path = base_path.substr(7);
        }
        const auto prefix = base_path.empty() ? "" : base_path + "/";
        const auto sink_path = prefix + metadata_key;

        if (is_s3_acquisition_()) {
            custom_metadata_sink_ = zarr::make_s3_sink(
              s3_settings_->bucket_name, sink_path, s3_connection_pool_);
        } else {
            custom_metadata_sink_ = zarr::make_file_sink(sink_path);
        }
    } else if (!overwrite) { // custom metadata already written, don't overwrite
        LOG_ERROR("Custom metadata already written, use overwrite flag");
        return ZarrStatusCode_WillNotOverwrite;
    }

    if (!custom_metadata_sink_) {
        LOG_ERROR("Custom metadata sink not found");
        return ZarrStatusCode_InternalError;
    }

    const auto metadata_json = nlohmann::json::parse(custom_metadata,
                                                     nullptr, // callback
                                                     false, // allow exceptions
                                                     true   // ignore comments
    );

    const auto metadata_str = metadata_json.dump(4);
    std::span data{ reinterpret_cast<const uint8_t*>(metadata_str.data()),
                    metadata_str.size() };
    if (!custom_metadata_sink_->write(0, data)) {
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
    } else if (version == ZarrVersion_2) {
        LOG_WARNING("Zarr version 2 is deprecated and will be removed in a "
                    "future release");
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

    if (!validate_compression_settings(settings->compression_settings,
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

    if (settings->downsampling_method >= ZarrDownsamplingMethodCount) {
        error_ = "Invalid downsampling method: " +
                 std::to_string(settings->downsampling_method);
        return false;
    }

    return true;
}

bool
ZarrStream_s::configure_array_(const struct ZarrStreamSettings_s* settings)
{
    std::string key = regularize_key(settings->output_key);
    if (!key.empty() && !is_valid_zarr_key(key, error_)) {
        set_error_("Invalid output key: '" + key + "'");
        return false;
    }

    std::optional<std::string> bucket_name;
    if (s3_settings_) {
        bucket_name = s3_settings_->bucket_name;
    }
    auto compression_settings =
      make_compression_params(settings->compression_settings);

    auto dims = make_array_dimensions(
      settings->dimensions, settings->dimension_count, settings->data_type);

    std::optional<ZarrDownsamplingMethod> downsampling_method;
    if (settings->multiscale) {
        downsampling_method = settings->downsampling_method;
    }
    auto config = std::make_shared<zarr::ArrayConfig>(store_path_,
                                                      key,
                                                      bucket_name,
                                                      compression_settings,
                                                      dims,
                                                      settings->data_type,
                                                      downsampling_method,
                                                      0);

    ZarrOutputArray output_node{ .output_key = key, .frame_buffer_offset = 0 };
    try {
        output_node.array =
          zarr::make_array(config, thread_pool_, s3_connection_pool_, version_);
    } catch (const std::exception& exc) {
        set_error_(exc.what());
    }

    if (output_node.array == nullptr) {
        set_error_("Failed to create output node: " + error_);
        return false;
    }

    // initialize frame buffer
    const auto frame_size_bytes = dims->width_dim().array_size_px *
                                  dims->height_dim().array_size_px *
                                  zarr::bytes_of_type(settings->data_type);

    output_node.frame_buffer.resize_and_fill(frame_size_bytes, 0);
    output_arrays_.emplace(key, std::move(output_node));

    return true;
}

bool
ZarrStream_s::commit_settings_(const struct ZarrStreamSettings_s* settings)
{
    version_ = settings->version;
    store_path_ = zarr::trim(settings->store_path);

    std::optional<std::string> bucket_name;
    if (is_s3_acquisition(settings)) {
        s3_settings_ = make_s3_settings(settings->s3_settings);
    }

    // create the data store
    if (!create_store_(settings->overwrite)) {
        set_error_("Failed to create the data store: " + error_);
        return false;
    }

    if (!configure_array_(settings)) {
        set_error_("Failed to configure array: " + error_);
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
ZarrStream_s::create_store_(bool overwrite)
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
        if (!overwrite) {
            if (fs::is_directory(store_path_)) {
                return true;
            } else if (fs::exists(store_path_)) {
                set_error_("Store path '" + store_path_ +
                           "' already exists and is "
                           "not a directory, and we "
                           "are not overwriting.");
                return false;
            }
        } else if (fs::exists(store_path_)) {
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
ZarrStream_s::write_intermediate_metadata_()
{
    const std::string output_key =
      output_arrays_.begin()->first; // TODO (aliddell): remove
    if (output_key.empty()) {
        return true; // no need to write metadata
    }

    std::optional<std::string> bucket_name;
    if (s3_settings_) {
        bucket_name = s3_settings_->bucket_name;
    }

    std::vector<std::string> paths = zarr::get_parent_paths({ output_key });
    while (!paths.back().empty()) {
        std::string parent_path = paths.back();
        paths.push_back(
          zarr::get_parent_paths({ parent_path })[0]); // get parent path
    }

    std::string metadata_key;
    nlohmann::json group_metadata;
    if (version_ == ZarrVersion_2) {
        metadata_key = ".zgroup";
        group_metadata = { { "zarr_format", 2 } };
    } else {
        metadata_key = "zarr.json";
        group_metadata = {
            { "zarr_format", 3 },
            { "consolidated_metadata", nullptr },
            { "node_type", "group" },
            { "attributes", nlohmann::json::object() },
        };
    }
    const std::string metadata_str = group_metadata.dump(4);
    ConstByteSpan metadata_span(
      reinterpret_cast<const uint8_t*>(metadata_str.data()),
      metadata_str.size());

    for (const auto& parent_group_key : paths) {
        const std::string sink_path =
          store_path_ + "/" + parent_group_key + "/" + metadata_key;

        std::unique_ptr<zarr::Sink> metadata_sink;
        if (is_s3_acquisition_()) {
            metadata_sink = zarr::make_s3_sink(
              bucket_name.value(), sink_path, s3_connection_pool_);
        } else {
            metadata_sink = zarr::make_file_sink(sink_path);
        }

        if (!metadata_sink->write(0, metadata_span) ||
            !zarr::finalize_sink(std::move(metadata_sink))) {
            set_error_("Failed to write intermediate metadata for group '" +
                       parent_group_key + "'");
            return false;
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

    size_t frame_size_bytes = 0;
    for (auto& [key, output] : output_arrays_) {
        frame_size_bytes =
          std::max(frame_size_bytes, output.frame_buffer.size());
    }

    // cap the frame buffer at 1 GiB, or 10 frames, whichever is larger
    const auto buffer_size_bytes = 1ULL << 30;
    const auto frame_count =
      std::max(10ULL, buffer_size_bytes / frame_size_bytes);

    try {
        frame_queue_ =
          std::make_unique<zarr::FrameQueue>(frame_count, frame_size_bytes);

        auto job = [this](std::string& err) {
            try {
                process_frame_queue_();
            } catch (const std::exception& e) {
                err = "Error processing frame queue: " + std::string(e.what());
                set_error_(err);

                return false;
            }

            return true;
        };

        EXPECT(thread_pool_->push_job(std::move(job)),
               "Failed to push frame processing job to thread pool.");
    } catch (const std::exception& e) {
        set_error_("Error creating frame queue: " + std::string(e.what()));
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

    std::string output_key;

    zarr::LockedBuffer frame;
    while (process_frames_ || !frame_queue_->empty()) {
        {
            std::unique_lock lock(frame_queue_mutex_);
            while (frame_queue_->empty() && process_frames_) {
                frame_queue_not_empty_cv_.wait_for(
                  lock, std::chrono::milliseconds(100));
            }

            if (frame_queue_->empty()) {
                frame_queue_empty_cv_.notify_all();

                // If we should stop processing and the queue is empty, we're
                // done
                if (!process_frames_) {
                    break;
                } else {
                    continue;
                }
            }
        }

        if (!frame_queue_->pop(frame, output_key)) {
            continue;
        }

        if (auto it = output_arrays_.find(output_key);
            it == output_arrays_.end()) {
            // If we have gotten here, something has gone seriously wrong
            set_error_("Output node not found for key: '" + output_key + "'");
            std::unique_lock lock(frame_queue_mutex_);
            frame_queue_finished_cv_.notify_all();
            return;
        } else {
            auto& output_node = it->second;

            if (output_node.array->write_frame(frame) != frame.size()) {
                set_error_("Failed to write frame to writer for key: " +
                           output_key);
                std::unique_lock lock(frame_queue_mutex_);
                frame_queue_finished_cv_.notify_all();
                return;
            }
        }

        {
            // Signal that there's space available in the queue
            std::unique_lock lock(frame_queue_mutex_);
            frame_queue_not_full_cv_.notify_one();

            // Signal that the queue is empty, if applicable
            if (frame_queue_->empty()) {
                frame_queue_empty_cv_.notify_all();
            }
        }
    }

    if (!frame_queue_->empty()) {
        LOG_WARNING("Reached end of frame queue processing with ",
                    frame_queue_->size(),
                    " frames remaining on queue");
        frame_queue_->clear();
    }

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
finalize_stream(struct ZarrStream_s* stream)
{
    if (stream == nullptr) {
        LOG_INFO("Stream is null. Nothing to finalize.");
        return true;
    }

    if (stream->custom_metadata_sink_ &&
        !zarr::finalize_sink(std::move(stream->custom_metadata_sink_))) {
        LOG_ERROR(
          "Error finalizing Zarr stream. Failed to write custom metadata");
    }

    stream->finalize_frame_queue_();

    for (auto& [key, output] : stream->output_arrays_) {
        if (!zarr::finalize_node(std::move(output.array))) {
            LOG_ERROR(
              "Error finalizing Zarr stream. Failed to finalize array '",
              key,
              "'");
            return false;
        }
    }

    if (!stream->write_intermediate_metadata_()) {
        LOG_ERROR(stream->error_);
        return false;
    }

    stream->thread_pool_->await_stop();

    return true;
}
