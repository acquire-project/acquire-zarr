#include "acquire.zarr.h"
#include "array.base.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"

#include <blosc.h>

#include <bit> // bit_ceil
#include <filesystem>
#include <regex>
#include <stack>
#include <unordered_set>

namespace fs = std::filesystem;

namespace {
std::optional<zarr::S3Settings>
make_s3_settings(const ZarrS3Settings* settings)
{
    if (!settings) {
        return std::nullopt;
    }

    zarr::S3Settings s3_settings{ .endpoint = zarr::trim(settings->endpoint),
                                  .bucket_name =
                                    zarr::trim(settings->bucket_name) };

    if (settings->region != nullptr) {
        s3_settings.region = zarr::trim(settings->region);
    }

    return { s3_settings };
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
make_array_dimensions(const ZarrArraySettings* settings)
{
    std::vector<ZarrDimension> dims;
    for (auto i = 0; i < settings->dimension_count; ++i) {
        const auto& dim = settings->dimensions[i];
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

    // Build target dimension order vector if specified
    std::vector<size_t> target_order;
    if (settings->storage_dimension_order) {
        target_order.reserve(settings->dimension_count);
        for (size_t i = 0; i < settings->dimension_count; ++i) {
            target_order.push_back(settings->storage_dimension_order[i]);
        }
    }

    return std::make_shared<ArrayDimensions>(
      std::move(dims), settings->data_type, target_order);
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

std::shared_ptr<zarr::ArrayConfig>
make_array_config(const ZarrArraySettings* settings,
                  const std::string& store_root,
                  const std::string& parent_path,
                  std::optional<std::string> array_key,
                  const std::optional<std::string>& bucket_name, std::string& error)
{
    // remove leading/trailing slashes and whitespace
    std::string key = zarr::regularize_key(settings->output_key);
    if (array_key) {
        key = zarr::regularize_key(*array_key);
    }
    key = parent_path + "/" + key;
    key = zarr::regularize_key(key);

    if (!key.empty() && !is_valid_zarr_key(key, error)) {
        error = "Invalid output key: '" + key + "': " + error;
        return nullptr;
    }

    std::optional<zarr::BloscCompressionParams> compression_params =
      make_compression_params(settings->compression_settings);

    std::shared_ptr<ArrayDimensions> dimensions =
      make_array_dimensions(settings);

    std::optional<ZarrDownsamplingMethod> downsampling_method = std::nullopt;
    if (settings->multiscale) {
        downsampling_method = settings->downsampling_method;
    }

    return std::make_shared<zarr::ArrayConfig>(store_root,
                                               key,
                                               bucket_name,
                                               compression_params,
                                               dimensions,
                                               settings->data_type,
                                               downsampling_method,
                                               0);
}

[[nodiscard]] bool
validate_dimension(const ZarrDimensionProperties* dimension,
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

    if (dimension->shard_size_chunks == 0) {
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
                        const std::string& parent_path,
                        std::string& error)
{
    if (settings == nullptr) {
        error = "Null pointer: settings";
        return false;
    }

    std::string key = zarr::regularize_key(settings->output_key);
    key = parent_path + "/" + key;
    key = zarr::regularize_key(key);

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
        if (!validate_dimension(settings->dimensions + i, i == 0, error)) {
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

[[nodiscard]] bool
is_numeric_string(const std::string& str)
{
    if (str.empty()) {
        return false;
    }

    for (char c : str) {
        if (c < '0' || c > '9') {
            return false;
        }
    }

    return true;
}

[[nodiscard]] bool
is_reserved_metadata_file(const std::string& name)
{
    return name == ".zarray" || name == ".zattrs" || name == ".zgroup" ||
           name == "zarr.json";
}

[[nodiscard]] bool
validate_hcs_settings(const ZarrHCSSettings* settings, std::string& error)
{
    if (settings == nullptr) {
        return true; // HCS settings are optional
    }

    if (settings->plate_count == 0) {
        error = "HCS settings given, but no plates specified";
        return false;
    }
    if (settings->plates == nullptr) {
        error = "Null pointer: plates";
        return false;
    }

    for (auto i = 0; i < settings->plate_count; ++i) {
        const auto& plate = settings->plates[i];
        if (plate.path == nullptr) {
            error = "Null pointer: path for plate " + std::to_string(i);
            return false;
        }

        if (plate.name == nullptr) {
            error = "Null pointer: name for plate " + std::to_string(i);
            return false;
        }

        std::unordered_set<uint32_t> acquisition_ids;
        std::unordered_set<std::string> row_names;
        std::unordered_set<std::string> column_names;

        // check acquisitions
        if (plate.acquisition_count > 0 && plate.acquisitions == nullptr) {
            error = "Null pointer: acquisitions in plate " + std::to_string(i);
            return false;
        }
        for (auto j = 0; j < plate.acquisition_count; ++j) {
            const auto& acquisition = plate.acquisitions[j];

            if (acquisition_ids.contains(acquisition.id)) {
                error = "Duplicate acquisition ID: " +
                        std::to_string(acquisition.id) + " in plate " +
                        std::to_string(i);
                return false;
            }
            acquisition_ids.insert(acquisition.id);
        }

        // check row names
        if (plate.row_names == nullptr) {
            error = "Null pointer: row names for plate " + std::to_string(i);
            return false;
        }

        if (plate.row_count == 0) {
            error = "No rows specified for plate " + std::to_string(i);
            return false;
        }

        for (auto j = 0; j < plate.row_count; ++j) {
            const std::string row_name =
              zarr::regularize_key(plate.row_names[j]);
            if (!is_valid_zarr_key(row_name, error)) {
                error = "Invalid row name in plate " + std::to_string(i) +
                        ": " + error;
                return false;
            }

            if (row_names.contains(row_name)) {
                error = "Duplicate row name: '" + row_name + "' in plate " +
                        std::to_string(i);
            }
            row_names.insert(row_name);
        }

        // check column names
        if (plate.column_names == nullptr) {
            error = "Null pointer: column names for plate " + std::to_string(i);
            return false;
        }

        if (plate.column_count == 0) {
            error = "No columns specified for plate " + std::to_string(i);
            return false;
        }

        for (auto j = 0; j < plate.column_count; ++j) {
            const std::string column_name =
              zarr::regularize_key(plate.column_names[j]);
            if (!is_valid_zarr_key(column_name, error)) {
                error = "Invalid column name in plate " + std::to_string(i) +
                        ": " + error;
                return false;
            }

            if (column_names.contains(column_name)) {
                error = "Duplicate column name: '" + column_name +
                        "' in plate " + std::to_string(i);
            }
            column_names.insert(column_name);
        }

        // check wells
        if (plate.wells == nullptr) {
            error = "Null pointer: well in plate " + std::to_string(i);
        }
        for (auto j = 0; j < plate.well_count; ++j) {
            const auto& well = plate.wells[j];

            if (well.row_name == nullptr) {
                error = "Null pointer: row name for well " + std::to_string(j) +
                        " in plate " + std::to_string(i);
                return false;
            }
            if (const std::string row_name(well.row_name);
                !row_names.contains(row_name)) {
                error = "Row name '" + row_name + "' for well " +
                        std::to_string(j) + " in plate " + std::to_string(i) +
                        " not found in plate row names";
                return false;
            }

            if (well.column_name == nullptr) {
                error = "Null pointer: column name for well " +
                        std::to_string(j) + " in plate " + std::to_string(i);
                return false;
            }
            if (const std::string column_name(well.column_name);
                !column_names.contains(column_name)) {
                error = "Column name '" + column_name + "' for well " +
                        std::to_string(j) + " in plate " + std::to_string(i) +
                        " not found in plate column names";
                return false;
            }

            // check fields of view
            if (well.images == nullptr) {
                error = "Null pointer: images in well " + std::to_string(j) +
                        " in plate " + std::to_string(i);
                return false;
            }
            std::unordered_set<std::string> fields_of_view;
            for (auto k = 0; k < well.image_count; ++k) {
                const auto& fov = well.images[k];

                if (fov.path == nullptr) {
                    error = "Null pointer: path for image " +
                            std::to_string(k) + " in well " +
                            std::to_string(j) + " in plate " +
                            std::to_string(i);
                    return false;
                }
                const std::string fov_path = zarr::regularize_key(fov.path);

                if (!is_valid_zarr_key(fov_path, error)) {
                    error = "Invalid path for image " + std::to_string(k) +
                            " in well " + std::to_string(j) + " in plate " +
                            std::to_string(i) + ": " + error;
                    return false;
                }

                if (fields_of_view.contains(fov_path)) {
                    error = "Duplicate path '" + fov_path + "' for image " +
                            std::to_string(k) + " in well " +
                            std::to_string(j) + " in plate " +
                            std::to_string(i);
                    return false;
                }

                if (fov.array_settings != nullptr &&
                    fov.array_settings->output_key != nullptr) {
                    error = "Non-NULL output_key in ArraySettings for image " +
                            std::to_string(k) + " in well " +
                            std::to_string(j) + " in plate " +
                            std::to_string(i);
                }

                fields_of_view.insert(fov_path);
            }
        }
    }

    return true;
}

[[nodiscard]] bool
check_array_structure(std::vector<std::shared_ptr<zarr::ArrayConfig>> arrays,
                      std::vector<std::string>& needs_metadata_paths,
                      std::string& error)
{
    if (arrays.empty()) {
        error = "No arrays provided";
        return false;
    }

    enum class DatasetNodeType
    {
        Directory,
        Array,
        MultiscaleArray,
    };

    struct DatasetNode
    {
        std::string name;
        DatasetNodeType type;
        std::unordered_map<std::string, std::shared_ptr<DatasetNode>> children;
        std::shared_ptr<DatasetNode> parent = nullptr;
    };

    auto tree = std::make_shared<DatasetNode>();
    tree->name = "";
    tree->type = DatasetNodeType::Directory;
    tree->children = {};

    std::unordered_set<std::string> seen_keys;

    // check that if the root node is not multiscale, there are no other arrays
    for (auto i = 0; i < arrays.size(); ++i) {
        const auto& array = arrays[i];
        const bool is_multiscale_array = array->downsampling_method.has_value();

        const std::string& key = array->node_key;

        // ensure that we don't have a duplicate key
        if (seen_keys.contains(key)) {
            error = "Duplicate output key: '" + key + "'";
            return false;
        }
        seen_keys.insert(key);

        if (key.empty()) {
            tree->type = is_multiscale_array ? DatasetNodeType::MultiscaleArray
                                             : DatasetNodeType::Array;
        } else {
            // break down the key into segments
            std::string parent_path = key;
            std::stack<std::string> segments;

            // walk up the parent chain
            while (true) {
                auto last_slash = parent_path.find_last_of('/');
                if (last_slash == std::string::npos) {
                    segments.push(parent_path);
                    break; // reached root
                }

                std::string segment = parent_path.substr(last_slash + 1);
                // check if the segment is reserved
                if (is_reserved_metadata_file(segment)) {
                    error = "Reserved metadata file name '" + segment +
                            "' in path '" + key + "'";
                    return false;
                }

                segments.push(parent_path.substr(last_slash + 1));
                parent_path = parent_path.substr(0, last_slash);
            }

            // now we have all segments in reverse order, build the tree
            auto current_node = tree;
            while (!segments.empty()) {
                std::string segment = segments.top();
                segments.pop();

                // check if this segment already exists
                if (auto it = current_node->children.find(segment);
                    it == current_node->children.end()) {
                    // Create a new node for this segment
                    auto new_node = std::make_shared<DatasetNode>();
                    new_node->name = segment;
                    new_node->parent = current_node;

                    if (segments.empty()) { // Last segment
                        new_node->type = is_multiscale_array
                                           ? DatasetNodeType::MultiscaleArray
                                           : DatasetNodeType::Array;
                    } else {
                        new_node->type = DatasetNodeType::Directory;
                    }
                    current_node->children.emplace(segment, new_node);
                }

                // Move to the child node
                current_node = current_node->children[segment];
            }
        }
    }

    // now validate the structure
    // enforce two rules:
    // 1. if a parent is not multiscale, it cannot have any children
    // 2. if a parent is multiscale, it cannot have any children with numeric
    //    names
    // we also construct the paths where we need to write group-level metadata
    needs_metadata_paths.clear();

    std::stack<std::shared_ptr<DatasetNode>> nodes_to_visit;
    nodes_to_visit.push(tree);

    while (!nodes_to_visit.empty()) {
        const auto current_node = nodes_to_visit.top();
        nodes_to_visit.pop();

        bool is_multiscale =
          (current_node->type == DatasetNodeType::MultiscaleArray);
        bool is_directory = (current_node->type == DatasetNodeType::Directory);

        // directories and multiscale arrays
        // can have children
        bool can_have_children = is_multiscale || is_directory;

        // if the parent is not multiscale, it must not have any children
        if (!can_have_children && !current_node->children.empty()) {
            error = "Directory node '" + current_node->name +
                    "' cannot have children";
            return false;
        }

        // a pure directory node needs to have a metadata file
        if (is_directory) {
            std::stack<std::string> path_segments;
            std::shared_ptr<DatasetNode> node = current_node;
            while (node != nullptr && !node->name.empty()) {
                path_segments.push(node->name);
                node = node->parent;
            }

            std::string path;
            while (!path_segments.empty()) {
                path += path_segments.top();
                path_segments.pop();
                if (!path_segments.empty()) {
                    path += "/";
                }
            }

            // add the path to the group metadata paths
            needs_metadata_paths.push_back(path);
        }

        for (const auto& [child_name, child_node] : current_node->children) {
            // If the parent is multiscale, it cannot have numeric children
            if (is_multiscale && is_numeric_string(child_name)) {
                error = "Multiscale parent '" + child_name +
                        "' cannot have numeric children";
                return false;
            }

            // add the child to the stack for further validation
            nodes_to_visit.push(child_node);
        }
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
} // namespace

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(struct ZarrStreamSettings_s* settings)
{
    EXPECT(validate_settings_(settings), error_);

    start_thread_pool_(settings->max_threads);

    // commit settings and create the output store
    EXPECT(commit_settings_(settings), error_);

    // initialize the frame queue
    EXPECT(init_frame_queue_(), error_);
}

size_t
ZarrStream::append(const char* key_, const void* data_, size_t nbytes)
{
    EXPECT(error_.empty(), "Cannot append data: ", error_.c_str());

    // if the key is null and we have only one output array, use that
    std::string key;
    if (key_ == nullptr && output_arrays_.size() == 1) {
        key = output_arrays_.begin()->first;
    } else {
        key = zarr::regularize_key(key_);
    }

    auto it = output_arrays_.find(key);
    EXPECT(it != output_arrays_.end(),
           "Cannot append data: array at '",
           key,
           "' not found");
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
            custom_metadata_sink_ =
              zarr::make_file_sink(sink_path, file_handle_pool_);
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

size_t
ZarrStream_s::get_memory_usage() const noexcept
{
    size_t usage = frame_queue_->bytes_used();
    for (const auto& [key, output] : output_arrays_) {
        const auto frame_buffer_size = output.frame_buffer.size();
        const auto array_memory_usage = output.array->memory_usage();
        usage += (frame_buffer_size + array_memory_usage);
    }

    return usage;
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

    if (const auto version =
          settings->version == 0 ? ZarrVersion_3 : settings->version;
        version != ZarrVersion_3) {
        error_ = "Invalid Zarr version: " + std::to_string(version);
        return false;
    }

    if (settings->store_path == nullptr) {
        error_ = "Null pointer: store_path";
        return false;
    }
    const std::string store_path(settings->store_path);

    // we require the store path (root of the dataset) to be nonempty
    if (store_path.empty()) {
        error_ = "Store path is empty";
        return false;
    }

    if (settings->s3_settings != nullptr) {
        if (!validate_s3_settings(settings->s3_settings, error_)) {
            return false;
        }
    } else if (!validate_filesystem_store_path(store_path, error_)) {
        return false;
    }

    // validate the arrays individually
    for (auto i = 0; i < settings->array_count; ++i) {
        const auto& array_settings = settings->arrays[i];
        if (!validate_array_settings(&array_settings, "", error_)) {
            return false;
        }
    }

    // validate the HCS settings if present
    if (!validate_hcs_settings(settings->hcs_settings, error_)) {
        return false;
    }

    std::vector<std::shared_ptr<zarr::ArrayConfig>> arrays(
      settings->array_count);
    for (auto i = 0; i < settings->array_count; ++i) {
        auto config = make_array_config(
          settings->arrays + i, store_path, "", std::nullopt, std::nullopt, error_);
        if (!config) {
            return false;
        }

        arrays[i] = config;
    }

    // add HCS settings arrays if present
    std::vector<std::string> hcs_array_keys;
    if (settings->hcs_settings) {
        const auto& hcs = settings->hcs_settings;
        if (hcs->plates == nullptr) {
            error_ = "Null pointer: plates in HCS settings";
            return false;
        }

        for (auto i = 0; i < hcs->plate_count; ++i) {
            const auto& plate = hcs->plates[i];
            if (plate.wells == nullptr) {
                error_ =
                  "Null pointer: wells in plate at index " + std::to_string(i);
                return false;
            }

            const std::string plate_path = zarr::regularize_key(plate.path);
            if (!plate_path.empty() && !is_valid_zarr_key(plate_path, error_)) {
                error_ = "Invalid plate path: '" + plate_path + "': " + error_;
                return false;
            }

            const std::string plate_name(plate.name);

            for (auto j = 0; j < plate.well_count; ++j) {
                const auto& well = plate.wells[j];
                if (well.images == nullptr) {
                    error_ = "Null pointer: images in well at index " +
                             std::to_string(j) + " of plate " + plate_name;
                    return false;
                }

                const std::string row_name(well.row_name);
                if (!is_valid_zarr_key(row_name, error_)) {
                    error_ = "Invalid well row name: '" + row_name +
                             "' at index " + std::to_string(j) + " of plate " +
                             plate_name + ": " + error_;
                    return false;
                }

                const std::string col_name(well.column_name);
                if (!is_valid_zarr_key(col_name, error_)) {
                    error_ = "Invalid well column name: '" + col_name +
                             "' at index " + std::to_string(j) + " of plate " +
                             plate_name + ": " + error_;
                    return false;
                }

                for (auto k = 0; k < well.image_count; ++k) {
                    const auto& field = well.images[k];

                    if (field.array_settings == nullptr) {
                        error_ = "Null pointer: array_settings for field " +
                                 std::to_string(k) + " of well " +
                                 std::to_string(j) + " of plate " + plate_name;
                        return false;
                    }

                    // array key here is relative to the plate/well/field,
                    // so we need to account for that here
                    std::string parent_path = plate_path;
                    parent_path += "/" + row_name + "/" + col_name;
                    if (!validate_array_settings(
                          field.array_settings, parent_path, error_)) {
                        return false;
                    }

                    auto config = make_array_config(field.array_settings,
                                                    store_path,
                                                    parent_path,
                                                    field.path,
                                                    std::nullopt, error_);
                    if (config == nullptr) {
                        return false;
                    }
                    arrays.push_back(config);
                }
            }
        }
    }

    // validate the arrays as a collection
    if (!check_array_structure(arrays, intermediate_group_paths_, error_)) {
        return false;
    }

    return true;
}

bool
ZarrStream_s::configure_array_(const ZarrArraySettings* settings,
                               const std::string& parent_path,
                               bool is_hcs_array)
{
    std::optional<std::string> bucket_name;
    if (s3_settings_) {
        bucket_name = s3_settings_->bucket_name;
    }

    auto config = make_array_config(
        settings, store_path_, parent_path, std::nullopt, bucket_name, error_);
    if (config == nullptr) {
        return false;
    }

    ZarrOutputArray output_node{ .output_key = config->node_key,
                                 .frame_buffer_offset = 0 };
    try {
        output_node.array = zarr::make_array(config,
                                             thread_pool_,
                                             file_handle_pool_,
                                             s3_connection_pool_,
                                             is_hcs_array);
    } catch (const std::exception& exc) {
        set_error_(exc.what());
    }

    if (output_node.array == nullptr) {
        set_error_("Failed to create output node: " + error_);
        return false;
    }

    // initialize frame buffer
    const auto& dims = config->dimensions;
    const auto frame_size_bytes = dims->width_dim().array_size_px *
                                  dims->height_dim().array_size_px *
                                  zarr::bytes_of_type(settings->data_type);

    output_node.frame_buffer.resize_and_fill(frame_size_bytes, 0);
    output_arrays_.emplace(output_node.output_key, std::move(output_node));

    return true;
}

bool
ZarrStream_s::commit_hcs_settings_(const ZarrHCSSettings* hcs_settings)
{
    if (hcs_settings == nullptr) {
        return true; // nothing to do
    }

    plates_.clear();
    wells_.clear();

    for (auto i = 0; i < hcs_settings->plate_count; ++i) {
        const auto& plate_in = hcs_settings->plates[i];
        const std::string plate_name(hcs_settings->plates[i].name);
        const std::string plate_path = zarr::regularize_key(plate_in.path);

        std::vector<std::string> row_names(plate_in.row_count);
        for (auto j = 0; j < plate_in.row_count; ++j) {
            row_names[j] = zarr::regularize_key(plate_in.row_names[j]);
        }

        std::vector<std::string> column_names(plate_in.column_count);
        for (auto j = 0; j < plate_in.column_count; ++j) {
            column_names[j] = zarr::regularize_key(plate_in.column_names[j]);
        }

        // collect acquisitions
        std::vector<zarr::Acquisition> acqs_out(plate_in.acquisition_count);
        for (auto j = 0; j < plate_in.acquisition_count; ++j) {
            const auto& acq_in = plate_in.acquisitions[j];
            auto& acq_out = acqs_out[j];

            std::optional<std::string> name, description;
            if (acq_in.name) {
                name = acq_in.name;
            }

            if (acq_in.description) {
                description = acq_in.description;
            }

            std::optional<uint64_t> start_time, end_time;
            if (acq_in.has_start_time) {
                start_time = acq_in.start_time;
            }

            if (acq_in.has_end_time) {
                end_time = acq_in.end_time;
            }

            acq_out.id = acq_in.id;
            acq_out.name = name;
            acq_out.description = description;
            acq_out.start_time = start_time;
            acq_out.end_time = end_time;
        }

        // collect wells
        std::vector<zarr::Well> wells_out(plate_in.well_count);
        for (auto j = 0; j < plate_in.well_count; ++j) {
            const auto& well_in = plate_in.wells[j];
            auto& well_out = wells_out[j];

            well_out.row_name = zarr::regularize_key(well_in.row_name);
            well_out.column_name = zarr::regularize_key(well_in.column_name);
            well_out.images.resize(well_in.image_count);

            std::string well_key =
              plate_path + "/" + well_out.row_name + "/" + well_out.column_name;
            well_key = zarr::regularize_key(well_key);

            for (auto k = 0; k < well_in.image_count; ++k) {
                const auto& image_in = well_in.images[k];
                auto& image_out = well_out.images[k];

                if (image_in.has_acquisition_id) {
                    image_out.acquisition_id = image_in.acquisition_id;
                }
                image_out.path = zarr::regularize_key(image_in.path);

                if (image_in.array_settings) {
                    image_in.array_settings->output_key = image_in.path;
                }

                if (!configure_array_(
                      image_in.array_settings, well_key, true)) {
                    set_error_("Failed to configure array for field of view " +
                               std::to_string(k) + " in well " +
                               std::to_string(j) + " in plate " +
                               std::to_string(i) + ": " + error_);
                    return false;
                }
            }
        }

        zarr::Plate plate_out(
          plate_path, plate_name, row_names, column_names, wells_out, acqs_out);

        plates_.emplace(plate_path, plate_out);
    }

    // collect references to wells
    for (const auto& plate : plates_ | std::views::values) {
        for (const auto& well : plate.wells()) {
            auto well_key =
              plate.path() + "/" + well.row_name + "/" + well.column_name;
            well_key = zarr::regularize_key(well_key);

            wells_.emplace(well_key, well);
        }
    }

    return true;
}

bool
ZarrStream_s::commit_settings_(const struct ZarrStreamSettings_s* settings)
{
    store_path_ = zarr::trim(settings->store_path);

    std::optional<std::string> bucket_name;
    s3_settings_ = make_s3_settings(settings->s3_settings);

    // create the data store
    if (!create_store_(settings->overwrite)) {
        set_error_("Failed to create the data store: " + error_);
        return false;
    }

    // configure flat arrays
    for (auto i = 0; i < settings->array_count; ++i) {
        const auto& array_settings = settings->arrays[i];
        if (!configure_array_(&array_settings, "", false)) {
            set_error_("Failed to configure array '" +
                       std::string(array_settings.output_key) + "': " + error_);
            return false;
        }
    }

    // configure HCS settings
    if (!commit_hcs_settings_(settings->hcs_settings)) {
        set_error_("Failed to configure HCS: " + error_);
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
    } else {
        try {
            file_handle_pool_ = std::make_shared<zarr::FileHandlePool>();
        } catch (const std::exception& e) {
            set_error_("Error creating file handle pool: " +
                       std::string(e.what()));
            return false;
        }

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
    std::optional<std::string> bucket_name;
    if (s3_settings_) {
        bucket_name = s3_settings_->bucket_name;
    }

    const nlohmann::json group_metadata = nlohmann::json({
      { "zarr_format", 3 },
      { "consolidated_metadata", nullptr },
      { "node_type", "group" },
      { "attributes", nlohmann::json::object() },
    });
    const std::string metadata_key = "zarr.json";
    std::string metadata_str;

    for (const auto& parent_group_key : intermediate_group_paths_) {
        const std::string relative_path =
          (parent_group_key.empty() ? "" : parent_group_key);

        if (auto pit = plates_.find(relative_path); // is it a plate?
            pit != plates_.end()) {
            const auto& plate = pit->second;
            nlohmann::json plate_metadata(
              group_metadata); // make a copy to modify

            // not supported for Zarr V2 / NGFF 0.4
            plate_metadata["attributes"]["ome"] = {
                { "version", "0.5" },
                { "plate", plate.to_json() },
            };

            metadata_str = plate_metadata.dump(4);
        } else if (auto wit = wells_.find(relative_path); // is it a well?
                   wit != wells_.end()) {
            const auto& well = wit->second;
            nlohmann::json well_metadata(
              group_metadata); // make a copy to modify

            // not supported for Zarr V2 / NGFF 0.4
            well_metadata["attributes"]["ome"] = {
                { "version", "0.5" },
                { "well", well.to_json() },
            };

            metadata_str = well_metadata.dump(4);
        } else { // generic group
            metadata_str = group_metadata.dump(4);
        }

        ConstByteSpan metadata_span(
          reinterpret_cast<const uint8_t*>(metadata_str.data()),
          metadata_str.size());

        const std::string sink_path =
          store_path_ + "/" + relative_path + "/" + metadata_key;
        std::unique_ptr<zarr::Sink> metadata_sink;
        if (is_s3_acquisition_()) {
            metadata_sink = zarr::make_s3_sink(
              bucket_name.value(), sink_path, s3_connection_pool_);
        } else {
            metadata_sink = zarr::make_file_sink(sink_path, file_handle_pool_);
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

        EXPECT(thread_pool_->push_job(job),
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

    // clear out the frame queue first
    stream->finalize_frame_queue_();

    // shut down the thread pool, let everything after this run in the main
    // thread
    stream->thread_pool_->await_stop();

    if (stream->custom_metadata_sink_ &&
        !zarr::finalize_sink(std::move(stream->custom_metadata_sink_))) {
        LOG_ERROR(
          "Error finalizing Zarr stream. Failed to write custom metadata");
    }

    for (auto& [key, output] : stream->output_arrays_) {
        if (!zarr::finalize_array(std::move(output.array))) {
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

    return true;
}
