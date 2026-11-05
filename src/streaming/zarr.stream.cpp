#include "acquire.zarr.h"
#include "array.base.hh"
#include "macros.hh"
#include "sink.hh"
#include "zarr.common.hh"
#include "zarr.stream.hh"

#include <blosc.h>

#include <algorithm> // clamp
#include <bit>       // bit_ceil
#include <charconv>  // from_chars
#include <filesystem>
#include <fstream>
#include <regex>
#include <stack>
#include <unordered_set>

namespace fs = std::filesystem;

namespace {
/**
 * @brief Parse @p text as a Zarr v3 group node that a reader can open.
 * @details Applies the rules of the v3 core spec, as zarr-python 3.4 enforces
 * them: `zarr_format` is the integer 3, `node_type` is "group", `attributes`
 * (if present) is an object, and any other key is `consolidated_metadata` or an
 * extension object with `"must_understand": false`. A document that only names
 * itself a group, such as one with `"attributes": []`, would be kept and then
 * fail to open, so it is rejected here and replaced.
 * @returns The document, or nullopt if it is not such a node, including text
 * that does not parse.
 */
std::optional<nlohmann::json>
parse_v3_group_node(std::string_view text)
{
    auto doc = nlohmann::json::parse(text, nullptr, false);
    if (!doc.is_object() || !doc.contains("zarr_format") ||
        !doc.contains("node_type")) {
        return std::nullopt;
    }

    for (const auto& [key, value] : doc.items()) {
        if (key == "zarr_format") {
            if (!value.is_number_integer() || value != 3) {
                return std::nullopt;
            }
        } else if (key == "node_type") {
            if (value != "group") {
                return std::nullopt;
            }
        } else if (key == "attributes") {
            if (!value.is_object()) {
                return std::nullopt;
            }
        } else if (key != "consolidated_metadata") {
            if (!value.is_object()) {
                return std::nullopt;
            }
            const auto must_understand = value.find("must_understand");
            if (must_understand == value.end() ||
                !must_understand->is_boolean() ||
                must_understand->get<bool>()) {
                return std::nullopt;
            }
        }
    }

    return doc;
}

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

    // defer to the client, so validation cannot accept an endpoint the client
    // would then throw on
    if (!zarr::is_valid_s3_endpoint(zarr::trim(settings->endpoint), error)) {
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

    // validate compressor/codec compatibility and per-compressor constraints
    switch (settings->compressor) {
        case ZarrCompressor_Blosc1:
            if (settings->codec != ZarrCompressionCodec_BloscLZ4 &&
                settings->codec != ZarrCompressionCodec_BloscZstd) {
                error =
                  "Blosc1 compressor requires BloscLZ4 or BloscZstd codec";
                return false;
            }
            if (settings->level > 9) {
                error = "Invalid compression level: " +
                        std::to_string(settings->level) +
                        ". Blosc supports levels 0-9";
                return false;
            }
            if (settings->shuffle != BLOSC_NOSHUFFLE &&
                settings->shuffle != BLOSC_SHUFFLE &&
                settings->shuffle != BLOSC_BITSHUFFLE) {
                error =
                  "Invalid shuffle: " + std::to_string(settings->shuffle) +
                  ". Must be " + std::to_string(BLOSC_NOSHUFFLE) +
                  " (no shuffle), " + std::to_string(BLOSC_SHUFFLE) +
                  " (byte shuffle), or " + std::to_string(BLOSC_BITSHUFFLE) +
                  " (bit shuffle)";
                return false;
            }
            break;
        case ZarrCompressor_Zstd:
            if (settings->codec != ZarrCompressionCodec_Zstd) {
                error = "Zstd compressor requires Zstd codec";
                return false;
            }
            if (settings->level > 22) {
                error = "Invalid compression level: " +
                        std::to_string(settings->level) +
                        ". Zstd supports levels 0-22";
                return false;
            }
            if (settings->shuffle != 0) {
                error = "Shuffle is not supported with stock zstd compression";
                return false;
            }
            break;
        case ZarrCompressor_None:
            break;
        default:
            error =
              "Invalid compressor: " + std::to_string(settings->compressor);
            return false;
    }

    return true;
}

[[nodiscard]] bool
parse_attributes(std::string_view text, nlohmann::json& value)
{
    if (text.empty()) {
        return false;
    }

    // parse the JSON
    auto val = nlohmann::json::parse(text,
                                     nullptr, // callback
                                     false,   // allow exceptions
                                     true     // ignore comments
    );

    if (val.is_discarded()) {
        LOG_ERROR("Invalid JSON: '", text, "'");
        return false;
    }
    value = std::move(val);

    return true;
}

std::optional<zarr::CompressionParams>
make_compression_params(const ZarrCompressionSettings* settings)
{
    if (!settings || settings->compressor == ZarrCompressor_None) {
        return std::nullopt;
    }

    switch (settings->compressor) {
        case ZarrCompressor_Blosc1:
            return zarr::BloscCompressionParams(
              zarr::blosc_codec_to_string(settings->codec),
              settings->level,
              settings->shuffle);
        case ZarrCompressor_Zstd:
            return zarr::ZstdCompressionParams{ settings->level };
        default:
            return std::nullopt;
    }
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
                  const std::optional<std::string>& bucket_name,
                  std::string& error)
{
    // remove leading/trailing slashes and whitespace
    const std::string key = zarr::regularize_key(settings->output_key);

    if (!key.empty() && !is_valid_zarr_key(key, error)) {
        error = "Invalid output key: '" + key + "': " + error;
        return nullptr;
    }

    std::optional<zarr::CompressionParams> compression_params =
      make_compression_params(settings->compression_settings);

    std::shared_ptr<ArrayDimensions> dimensions =
      make_array_dimensions(settings);

    std::optional<ZarrDownsamplingMethod> downsampling_method = std::nullopt;
    if (settings->downsampling_method > ZarrDownsamplingMethod_None) {
        downsampling_method = settings->downsampling_method;
    }

    return std::make_shared<zarr::ArrayConfig>(
      store_root,
      key,
      bucket_name,
      compression_params,
      dimensions,
      settings->data_type,
      downsampling_method,
      0,
      downsampling_method.has_value() || settings->is_ngff,
      settings->max_levels);
}

std::unique_ptr<zarr::ArrayBase>
make_array(std::shared_ptr<zarr::ArrayConfig> config,
           std::shared_ptr<zarr::ThreadPool> thread_pool,
           std::shared_ptr<zarr::FileHandlePool> file_handle_pool,
           std::shared_ptr<zarr::S3Client> s3_client)
{
    const auto ngff = config->downsampling_method || config->is_ngff;

    std::unique_ptr<zarr::ArrayBase> array;
    if (ngff) {
        array = std::make_unique<zarr::MultiscaleArray>(
          config, thread_pool, file_handle_pool, s3_client);
    } else {
        array = std::make_unique<zarr::Array>(
          config, thread_pool, file_handle_pool, s3_client);
    }

    return array;
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
validate_array_settings(const ZarrArraySettings* settings, std::string& error)
{
    if (settings == nullptr) {
        error = "Null pointer: settings";
        return false;
    }

    const std::string key = zarr::regularize_key(settings->output_key);

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

    // we must have at least 2 dimensions
    const size_t ndims = settings->dimension_count;
    if (ndims < 2) {
        error = "Invalid number of dimensions: " + std::to_string(ndims) +
                ". Must be at least 2";
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

    if (settings->downsampling_method >= ZarrDownsamplingMethodCount) {
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
        const bool is_multiscale_array = array->is_ngff;

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

                const bool is_last_segment = segments.empty();
                const auto array_node_type =
                  is_multiscale_array ? DatasetNodeType::MultiscaleArray
                                      : DatasetNodeType::Array;

                // check if this segment already exists
                if (auto it = current_node->children.find(segment);
                    it == current_node->children.end()) {
                    // Create a new node for this segment
                    auto new_node = std::make_shared<DatasetNode>();
                    new_node->name = segment;
                    new_node->parent = current_node;

                    new_node->type = is_last_segment
                                       ? array_node_type
                                       : DatasetNodeType::Directory;
                    current_node->children.emplace(segment, new_node);
                } else if (is_last_segment) {
                    // a longer key processed earlier created this node as a
                    // placeholder directory, but it is really an array
                    it->second->type = array_node_type;
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
            error =
              "Array node '" + current_node->name + "' cannot have children";
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
/// Check each attribute entry without writing anything: the node exists
/// and is addressable, the metadata is JSON, and no reserved key is used. A
/// level's index is checked when the entry is written, because the number of
/// levels is known only when the arrays exist.
[[nodiscard]] bool
check_attribute_entries(
  const ZarrStreamSettings* settings,
  const std::vector<std::shared_ptr<zarr::ArrayConfig>>& arrays,
  const std::vector<std::string>& group_paths,
  ZarrIntermediateGroups intermediate_groups,
  std::string& error)
{
    // the keys that a multiscale group writes under `ome`
    const nlohmann::json multiscale_ome = {
        { "version", zarr::MultiscaleArray::ome_version },
        { "name", nullptr },
        { "multiscales", nullptr },
    };

    for (size_t i = 0; i < settings->attributes_count; ++i) {
        const auto& entry = settings->attributes[i];
        const std::string ctx = "attributes[" + std::to_string(i) + "]";

        std::string key;
        if (entry.node_key) {
            key = zarr::regularize_key(entry.node_key);
        } else if (arrays.size() == 1) {
            key = arrays.front()->node_key;
        } else {
            error = ctx + ": a node key is required when there are multiple "
                          "arrays";
            return false;
        }

        bool found = false;
        nlohmann::json library_ome = nullptr;
        for (const auto& array : arrays) {
            if (array->node_key == key) {
                found = true;
                if (array->is_ngff) {
                    library_ome = multiscale_ome;
                }
            }
        }

        if (!found) { // a level of detail?
            const auto slash = key.find_last_of('/');
            const std::string parent =
              slash == std::string::npos ? "" : key.substr(0, slash);
            const std::string level =
              slash == std::string::npos ? key : key.substr(slash + 1);
            found = is_numeric_string(level) &&
                    std::ranges::any_of(arrays, [&](const auto& array) {
                        return array->node_key == parent && array->is_ngff;
                    });
        }

        if (!found &&
            std::ranges::find(group_paths, key) != group_paths.end()) {
            if (intermediate_groups == ZarrIntermediateGroups_Never) {
                error = ctx + ": group '" + key +
                        "' is not written by this stream (intermediate "
                        "groups: never)";
                return false;
            }
            found = true;
        }

        if (!found) {
            error = ctx + ": '" + key +
                    "' is not an array, a group, or a level of detail in this "
                    "stream";
            return false;
        }

        nlohmann::json value;
        if (!parse_attributes(entry.value ? entry.value : "", value)) {
            error = ctx + ": value is not valid JSON";
            return false;
        }

        std::unordered_map<std::string, nlohmann::json> scratch;
        if (!zarr::merge_attributes(
              scratch,
              zarr::regularize_key(entry.key ? entry.key : ""),
              value,
              library_ome)) {
            error = ctx + ": value refused for '" + key + "' (see the log)";
            return false;
        }
    }

    return true;
}
} // namespace

/* ZarrStream_s implementation */

ZarrStream::ZarrStream_s(const ZarrStreamSettings_s* settings)
{
    EXPECT(validate_settings_(settings), error_);

    start_thread_pool_(settings->max_threads);

    // commit settings and create the output store
    EXPECT(commit_settings_(settings), error_);

    EXPECT(write_intermediate_metadata_(), error_);

    EXPECT(write_initial_attributes_(settings), error_);

    // initialize the frame queue
    EXPECT(init_frame_queue_(), error_);
}

ZarrStatusCode
ZarrStream::append(const char* key_,
                   const void* data_,
                   size_t bytes_in,
                   size_t& bytes_out)
{
    bytes_out = 0; // bytes written out of the input data

    if (!error_.empty()) {
        LOG_ERROR("Cannot append data: ", error_);
        return ZarrStatusCode_InternalError;
    }

    // if the key is null and we have only one output array, use that
    std::string key;
    if (key_ == nullptr && arrays_.size() == 1) {
        key = arrays_.begin()->first;
    } else {
        key = zarr::regularize_key(key_);
    }

    const auto array_it = arrays_.find(key);
    if (array_it == arrays_.end()) {
        return ZarrStatusCode_KeyNotFound;
    }

    if (bytes_in == 0) {
        LOG_INFO("Skipping append to array '", key, "': no data");
        return ZarrStatusCode_Success;
    }

    auto& output = array_it->second;

    if (output->max_array_size_bytes > 0 &&
        output->bytes_written + bytes_in > output->max_array_size_bytes) {
        LOG_ERROR("Incoming byte count ",
                  bytes_in,
                  " will overflow array (bytes written: ",
                  output->bytes_written,
                  ", maximum bytes: ",
                  output->max_array_size_bytes,
                  ")");
        return ZarrStatusCode_WriteOutOfBounds;
    }
    auto& frame_buffer = output->frame_buffer;
    auto& frame_buffer_offset = output->frame_buffer_offset;
    const size_t frame_size_bytes = output->frame_size_bytes;

    if (frame_buffer.empty() && frame_buffer_offset > 0) {
        set_error_("Corrupted frame buffer for key '" + key + "'");
        return ZarrStatusCode_InternalError;
    }

    auto* data = data_ ? static_cast<const uint8_t*>(data_) : nullptr;
    while (bytes_out < bytes_in) {
        const size_t bytes_remaining = bytes_in - bytes_out;

        if (frame_buffer_offset > 0) { // add to / finish a partial frame
            const size_t bytes_to_copy =
              std::min(frame_size_bytes - frame_buffer_offset, bytes_remaining);

            if (data) {
                memcpy(frame_buffer.data() + frame_buffer_offset,
                       data + bytes_out,
                       bytes_to_copy);
            } else {
                memset(frame_buffer.data() + frame_buffer_offset,
                       0,
                       bytes_to_copy);
            }

            frame_buffer_offset += bytes_to_copy;
            bytes_out += bytes_to_copy;

            // ready to enqueue the frame buffer
            if (frame_buffer_offset == frame_size_bytes) {
                std::unique_lock lock(frame_queue_mutex_);
                while (!frame_queue_->push(
                         frame_buffer, key, output->frames_queued) &&
                       process_frames_) {
                    frame_queue_not_full_cv_.wait(lock);
                }
                ++output->frames_queued;

                if (process_frames_) {
                    frame_queue_not_empty_cv_.notify_one();
                } else {
                    LOG_DEBUG("Stopping frame processing");
                    break;
                }
                data = data ? data + bytes_to_copy : data;
                frame_buffer_offset = 0;
            }
        } else if (bytes_remaining < frame_size_bytes) { // begin partial frame
            if (frame_buffer.empty()) {
                frame_buffer.resize(frame_size_bytes, 0);
                output->frame_buffer_bytes.store(frame_buffer.size());
            }

            // the buffer still holds the last partial frame
            if (data) {
                memcpy(frame_buffer.data(), data, bytes_remaining);
            } else {
                memset(frame_buffer.data(), 0, bytes_remaining);
            }

            frame_buffer_offset = bytes_remaining;
            bytes_out += bytes_remaining;
        } else { // at least one full frame
            std::span frame(data, frame_size_bytes);

            std::unique_lock lock(frame_queue_mutex_);
            while (!frame_queue_->push(frame, key, output->frames_queued) &&
                   process_frames_) {
                frame_queue_not_full_cv_.wait(lock);
            }
            ++output->frames_queued;

            if (process_frames_) {
                frame_queue_not_empty_cv_.notify_one();
            } else {
                LOG_DEBUG("Stopping frame processing");
                break;
            }

            bytes_out += frame_size_bytes;
            data = data ? data + frame_size_bytes : data;
        }
    }
    output->bytes_written += bytes_out;

    CHECK(bytes_out <= bytes_in);
    if (bytes_out < bytes_in) {
        return ZarrStatusCode_PartialWrite;
    }
    return ZarrStatusCode_Success;
}

ZarrStatusCode
ZarrStream_s::write_attributes(const std::optional<std::string>& node_key,
                               std::string_view attribute_key,
                               std::string_view value)
{

    std::string key;
    if (node_key.has_value()) {
        key = zarr::regularize_key(*node_key);
    } else if (arrays_.size() == 1) {
        key = arrays_.begin()->first;
    } else {
        LOG_ERROR("Node key is required when there are multiple output arrays");
        return ZarrStatusCode_InvalidArgument;
    }
    const std::string attr_key = zarr::regularize_key(attribute_key);

    // parsed before the group lock is taken; reported after the lookup, so
    // that an unknown key is reported first
    nlohmann::json value_json;
    const bool value_is_valid = parse_attributes(value, value_json);

    zarr::ArrayBase* array = nullptr;
    if (const auto it = arrays_.find(key); it != arrays_.end()) {
        array = it->second->array.get();
    } else {
        array = find_level_(key);
    }

    std::unique_lock groups_lock(groups_mutex_, std::defer_lock);
    OutputGroup* group = nullptr;
    if (!array) {
        groups_lock.lock();
        if (const auto it = groups_.find(key); it != groups_.end()) {
            group = &it->second;
        } else if (std::ranges::find(intermediate_group_paths_, key) !=
                   intermediate_group_paths_.end()) {
            LOG_ERROR("Group '",
                      key,
                      "' is not written by this stream (intermediate groups: "
                      "never), so it cannot take attributes");
            return ZarrStatusCode_InvalidArgument;
        } else {
            LOG_ERROR("Key '",
                      key,
                      "' is not an array, a group, or a level of detail in "
                      "this stream");
            return ZarrStatusCode_KeyNotFound;
        }
    }

    if (!value_is_valid) {
        LOG_ERROR("Invalid attributes: '", value, "'");
        return ZarrStatusCode_InvalidArgument;
    }

    if (array) {
        // held in memory, and written with the array's metadata
        if (!array->write_attributes(attr_key, value_json)) {
            LOG_ERROR("Error writing attributes for '", key, "'");
            return ZarrStatusCode_InvalidArgument;
        }
        return ZarrStatusCode_Success;
    }

    // a group is written now, so that a reader sees its metadata during the
    // acquisition; the merge is kept only if the write succeeds
    auto attributes = group->attributes;
    // the stream writes no `ome` on a group, so no `ome` key of the group is
    // ours to protect
    if (!zarr::merge_attributes(attributes, attr_key, value_json, nullptr)) {
        LOG_ERROR("Error writing attributes for '", key, "'");
        return ZarrStatusCode_InvalidArgument;
    }

    if (!write_group_(key, group->document, attributes)) {
        LOG_ERROR("Failed to write attributes for group '", key, "'");
        return ZarrStatusCode_IOError;
    }
    group->attributes = std::move(attributes);

    return ZarrStatusCode_Success;
}

zarr::ArrayBase*
ZarrStream_s::find_level_(const std::string& key) const
{
    const auto slash = key.find_last_of('/');
    const std::string parent_key =
      slash == std::string::npos ? "" : key.substr(0, slash);
    const std::string level_name =
      slash == std::string::npos ? key : key.substr(slash + 1);

    if (!is_numeric_string(level_name)) {
        return nullptr;
    }

    const auto it = arrays_.find(parent_key);
    if (it == arrays_.end()) {
        return nullptr;
    }

    const auto* multiscale =
      dynamic_cast<const zarr::MultiscaleArray*>(it->second->array.get());
    if (!multiscale) {
        return nullptr;
    }

    // "01" is not the path of level 1
    size_t level = 0;
    const auto [end, ec] = std::from_chars(
      level_name.data(), level_name.data() + level_name.size(), level);
    if (ec != std::errc{} || std::to_string(level) != level_name) {
        return nullptr;
    }

    return multiscale->level(level);
}

size_t
ZarrStream_s::get_memory_usage() const noexcept
{
    size_t usage = frame_queue_->bytes_used();

    // try_lock: finalize_stream holds this across every array's close, and an
    // estimate must not block on that. Under contention the arrays are omitted,
    // which matches ArrayBase::memory_usage()'s own best-effort contract.
    std::unique_lock arrays_lock(arrays_mutex_, std::try_to_lock);
    if (!arrays_lock.owns_lock()) {
        return usage;
    }

    for (const auto& [key, output] : arrays_) {
        // frame_buffer is resized on the appending thread, so its size is read
        // from an atomic rather than from the vector
        usage += output->frame_buffer_bytes.load();

        // finalize_stream has moved the array out by the time it releases the
        // lock above, so the slot can legitimately be empty
        if (output->array) {
            usage += output->array->memory_usage();
        }
    }

    return usage;
}

bool
ZarrStream_s::is_s3_acquisition_() const
{
    return s3_settings_.has_value();
}

bool
ZarrStream_s::validate_settings_(const ZarrStreamSettings* settings)
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

    if (settings->attributes_count > 0 && settings->attributes == nullptr) {
        error_ = "Null pointer: attributes";
        return false;
    }

    if (settings->intermediate_groups >= ZarrIntermediateGroupsCount) {
        error_ = "Invalid intermediate groups mode: " +
                 std::to_string(settings->intermediate_groups);
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
        if (!validate_array_settings(&array_settings, error_)) {
            return false;
        }
    }

    std::vector<std::shared_ptr<zarr::ArrayConfig>> arrays(
      settings->array_count);
    for (auto i = 0; i < settings->array_count; ++i) {
        auto config = make_array_config(
          settings->arrays + i, store_path, std::nullopt, error_);
        if (!config) {
            return false;
        }

        arrays[i] = config;
    }

    // validate the arrays as a collection
    if (!check_array_structure(arrays, intermediate_group_paths_, error_)) {
        return false;
    }

    // before the store is touched, so that a mistake fails create without
    // changing the store
    if (!check_attribute_entries(settings,
                                 arrays,
                                 intermediate_group_paths_,
                                 settings->intermediate_groups,
                                 error_)) {
        return false;
    }

    return true;
}

bool
ZarrStream_s::configure_array_(const ZarrArraySettings* settings)
{
    std::optional<std::string> bucket_name;
    if (s3_settings_) {
        bucket_name = s3_settings_->bucket_name;
    }

    const auto config =
      make_array_config(settings, store_path_, bucket_name, error_);
    if (config == nullptr) {
        return false;
    }

    const auto& dims = config->dimensions;
    const size_t frame_size_bytes = dims->width_dim().array_size_px *
                                    dims->height_dim().array_size_px *
                                    zarr::bytes_of_type(settings->data_type);

    auto output = std::make_unique<OutputArray>(config->node_key,
                                                std::vector<uint8_t>(),
                                                0,
                                                frame_size_bytes,
                                                nullptr,
                                                0,
                                                0,
                                                0);
    try {
        output->array =
          make_array(config, thread_pool_, file_handle_pool_, s3_client_);
    } catch (const std::exception& exc) {
        set_error_(exc.what());
    }

    if (output->array == nullptr) {
        set_error_("Failed to create output node: " + error_);
        return false;
    }

    output->max_array_size_bytes = output->array->max_bytes();
    arrays_.emplace(output->key, std::move(output));

    return true;
}

bool
ZarrStream_s::commit_settings_(const ZarrStreamSettings* settings)
{
    store_path_ = zarr::trim(settings->store_path);

    std::optional<std::string> bucket_name;
    s3_settings_ = make_s3_settings(settings->s3_settings);
    overwrite_ = settings->overwrite;
    intermediate_groups_ = settings->intermediate_groups;

    // create the data store
    if (!create_store_(settings->overwrite)) {
        set_error_("Failed to create the data store: " + error_);
        return false;
    }

    // configure flat arrays
    for (auto i = 0; i < settings->array_count; ++i) {
        const auto& array_settings = settings->arrays[i];
        if (!configure_array_(&array_settings)) {
            set_error_("Failed to configure array '" +
                       std::string(array_settings.output_key) + "': " + error_);
            return false;
        }
    }

    return true;
}

void
ZarrStream_s::start_thread_pool_(uint32_t max_threads)
{
    max_threads = zarr::resolve_max_threads(max_threads);
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
    // never store an empty error: callers key "did something fail?" off a
    // non-empty error_, so an empty message would read as a clean close
    error_ = msg.empty() ? "Unspecified error during streaming" : msg;
    frame_queue_processing_done_ = true;
}

bool
ZarrStream_s::create_store_(bool overwrite)
{
    if (is_s3_acquisition_()) {
        try {
            s3_client_ = std::make_shared<zarr::S3Client>(*s3_settings_);
        } catch (const std::exception& e) {
            set_error_("Error creating S3 client: " + std::string(e.what()));
            return false;
        }

        if (!s3_client_->bucket_exists(s3_settings_->bucket_name)) {
            set_error_("S3 bucket '" + s3_settings_->bucket_name +
                       "' does not exist or is not accessible.");
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
    const nlohmann::json group_metadata = nlohmann::json({
      { "zarr_format", 3 },
      { "consolidated_metadata", nullptr },
      { "node_type", "group" },
      { "attributes", nlohmann::json::object() },
    });

    std::unique_lock lock(groups_mutex_);
    groups_.clear();

    // a group carries nothing of ours, so the caller decides whether we write
    // it (#186)
    if (intermediate_groups_ == ZarrIntermediateGroups_Never) {
        return true;
    }

    for (const auto& relative_path : intermediate_group_paths_) {
        const std::string sink_path = group_sink_path_(relative_path);
        nlohmann::json document(group_metadata); // make a copy to modify

        // with overwrite, the filesystem store starts empty; write
        // unconditionally so that S3, which is not cleared, matches it
        if (intermediate_groups_ == ZarrIntermediateGroups_IfMissing &&
            !overwrite_) {
            if (!write_group_metadata_if_missing_(sink_path, document)) {
                return false;
            }
        } else if (!write_metadata_object_(sink_path, document.dump(4))) {
            set_error_("Failed to write intermediate metadata for group '" +
                       relative_path + "'");
            return false;
        }

        groups_.emplace(relative_path,
                        OutputGroup{ .document = std::move(document) });
    }

    return true;
}

bool
ZarrStream_s::write_initial_attributes_(const ZarrStreamSettings* settings)
{
    for (size_t i = 0; i < settings->attributes_count; ++i) {
        const auto& entry = settings->attributes[i];

        std::optional<std::string> node_key;
        if (entry.node_key) {
            node_key = entry.node_key;
        }

        const auto status = write_attributes(
          node_key, entry.key ? entry.key : "", entry.value ? entry.value : "");
        if (status != ZarrStatusCode_Success) {
            set_error_("Failed to write attributes[" + std::to_string(i) +
                       "] to '" + node_key.value_or("(NULL)") +
                       "': " + Zarr_get_status_message(status));
            return false;
        }
    }

    return true;
}

bool
ZarrStream_s::write_groups_with_attributes_()
{
    std::unique_lock lock(groups_mutex_);
    for (const auto& [relative_path, group] : groups_) {
        if (group.attributes.empty()) {
            continue;
        }

        if (!write_group_(relative_path, group.document, group.attributes)) {
            set_error_("Failed to write attributes for group '" +
                       relative_path + "'");
            return false;
        }
    }

    return true;
}

bool
ZarrStream_s::write_group_(
  const std::string& relative_path,
  const nlohmann::json& document,
  const std::unordered_map<std::string, nlohmann::json>& user_attributes)
{
    nlohmann::json out(document);
    auto& attributes = out["attributes"];
    if (!attributes.is_object()) {
        attributes = nlohmann::json::object();
    }
    zarr::apply_attributes(attributes, user_attributes);

    return write_metadata_object_(group_sink_path_(relative_path), out.dump(4));
}

std::string
ZarrStream_s::group_sink_path_(const std::string& relative_path) const
{
    // root group has an empty relative_path; can't regularize_key the join
    // because store_path_ may be absolute (#247)
    std::string sink_path = store_path_;
    if (!relative_path.empty()) {
        sink_path += "/" + relative_path;
    }
    return sink_path + "/zarr.json";
}

bool
ZarrStream_s::write_group_metadata_if_missing_(const std::string& sink_path,
                                               nlohmann::json& document)
{
    const std::string metadata_str = document.dump(4);

    if (is_s3_acquisition_()) {
        const auto& bucket_name = s3_settings_->bucket_name;
        ConstByteSpan metadata_span(
          reinterpret_cast<const uint8_t*>(metadata_str.data()),
          metadata_str.size());

        switch (s3_client_->put_object_if_absent(
          bucket_name, sink_path, metadata_span)) {
            case zarr::S3Client::PutIfAbsentResult::Stored:
                return true;
            case zarr::S3Client::PutIfAbsentResult::Failed:
                set_error_("Failed to write group metadata at '" + sink_path +
                           "'");
                return false;
            case zarr::S3Client::PutIfAbsentResult::Exists:
                break;
        }

        const auto existing = s3_client_->get_object(bucket_name, sink_path);
        if (!existing) {
            set_error_("Failed to read existing group metadata at '" +
                       sink_path + "'");
            return false;
        }
        if (auto group = parse_v3_group_node(
              std::string_view(reinterpret_cast<const char*>(existing->data()),
                               existing->size()))) {
            if (!keep_group_metadata_(sink_path, *group)) {
                return false;
            }
            document = std::move(*group);
            return true;
        }
    } else {
        std::error_code ec;
        const bool exists = fs::exists(sink_path, ec);
        if (ec) {
            set_error_("Failed to check for group metadata at '" + sink_path +
                       "': " + ec.message());
            return false;
        }

        if (!exists) {
            if (!write_metadata_object_(sink_path, metadata_str)) {
                set_error_("Failed to write group metadata at '" + sink_path +
                           "'");
                return false;
            }
            return true;
        }

        std::ifstream file(sink_path, std::ios::binary);
        if (!file.is_open()) {
            set_error_("Failed to open existing group metadata at '" +
                       sink_path + "'");
            return false;
        }
        const std::string existing((std::istreambuf_iterator<char>(file)),
                                   std::istreambuf_iterator<char>());
        if (file.bad()) {
            set_error_("Failed to read existing group metadata at '" +
                       sink_path + "'");
            return false;
        }
        file.close(); // before a replacement opens it for writing
        if (auto group = parse_v3_group_node(existing)) {
            if (!keep_group_metadata_(sink_path, *group)) {
                return false;
            }
            document = std::move(*group);
            return true;
        }
    }

    LOG_WARNING(
      "Replacing metadata at ", sink_path, " that is not a Zarr v3 group node");
    if (!write_metadata_object_(sink_path, metadata_str)) {
        set_error_("Failed to write group metadata at '" + sink_path + "'");
        return false;
    }

    return true;
}

bool
ZarrStream_s::keep_group_metadata_(const std::string& sink_path,
                                   nlohmann::json& group)
{
    const auto consolidated = group.find("consolidated_metadata");
    if (consolidated == group.end() || consolidated->is_null()) {
        LOG_DEBUG("Keeping existing group metadata at ", sink_path);
        return true;
    }

    LOG_INFO("Clearing consolidated metadata at ",
             sink_path,
             ", which would hide the arrays that this stream writes");
    *consolidated = nullptr;
    if (!write_metadata_object_(sink_path, group.dump(4))) {
        set_error_("Failed to write group metadata at '" + sink_path + "'");
        return false;
    }

    return true;
}

bool
ZarrStream_s::write_metadata_object_(const std::string& sink_path,
                                     const std::string& metadata_str)
{
    ConstByteSpan metadata_span(
      reinterpret_cast<const uint8_t*>(metadata_str.data()),
      metadata_str.size());

    std::unique_ptr<zarr::Sink> metadata_sink;
    if (is_s3_acquisition_()) {
        metadata_sink =
          zarr::make_s3_sink(s3_settings_->bucket_name, sink_path, s3_client_);
    } else {
        metadata_sink = zarr::make_file_sink(
          sink_path, file_handle_pool_, /*truncate_to_fit=*/true);
    }

    return metadata_sink && metadata_sink->write(0, metadata_span) &&
           zarr::finalize_sink(std::move(metadata_sink));
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
    for (const auto& output : arrays_ | std::views::values) {
        frame_size_bytes = std::max(frame_size_bytes, output->frame_size_bytes);
    }

    // Bound the frame queue so a fast producer decouples from the consumer
    // without buffering the whole acquisition in RAM (otherwise peak RSS grows
    // to ~the dataset size). Target 256 MiB, clamped to [16, 512] frames so
    // tiny frames don't explode the slot count and huge frames still get
    // enough buffering to absorb bursts.
    constexpr uint64_t buffer_size_bytes = 256ULL << 20;
    const auto frame_count =
      std::clamp<uint64_t>(buffer_size_bytes / frame_size_bytes, 16ULL, 512ULL);

    try {
        frame_queue_ =
          std::make_unique<zarr::FrameQueue>(frame_count, frame_size_bytes);

        auto job = [this](std::string& err) -> zarr::ThreadPool::TaskResult {
            try {
                process_frame_queue_();
            } catch (const std::exception& e) {
                err = "Error processing frame queue: " + std::string(e.what());
                set_error_(err);

                return zarr::ThreadPool::TaskResult::Fatal;
            }

            return zarr::ThreadPool::TaskResult::Success;
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
    uint64_t frame_id;

    std::vector<uint8_t> frame;
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
                }
                continue;
            }
        }

        if (!frame_queue_->pop(frame, output_key, frame_id)) {
            continue;
        }

        std::string err;
        if (auto it = arrays_.find(output_key); it == arrays_.end()) {
            // If we have gotten here, something has gone seriously wrong
            err = "Output node not found for key: '" + output_key + "'";
        } else {
            const auto& output = it->second;
            const auto& array = output->array;

            try {
                switch (size_t n_bytes;
                        array->write_frame(frame, n_bytes, frame_id)) {
                    case zarr::WriteResult::Ok:
                        break;
                    case zarr::WriteResult::PartialWrite:
                        err = "Partial write for key '" + output_key + "'";
                        break;
                    case zarr::WriteResult::OutOfBounds:
                        err = "OOB write for key '" + output_key + "'";
                        break;
                    case zarr::WriteResult::FrameSizeMismatch:
                        err =
                          "Frame size mismatch for key '" + output_key + "'";
                        break;
                    case zarr::WriteResult::FrameOutOfOrder:
                        err = "Frame id " + std::to_string(frame_id) +
                              " out of order for array '" + output_key + "'";
                        break;
                    default:
                        err = "Unidentified error for key '" + output_key + "'";
                }
            } catch (const std::exception& exc) {
                err = "Failed to write frame to writer for key '" + output_key +
                      "': " + exc.what();
            }

            if (!err.empty()) {
                set_error_(err);

                std::unique_lock lock(frame_queue_mutex_);
                process_frames_ = false;
                frame_queue_->clear();
                frame_queue_not_full_cv_.notify_all();
                frame_queue_empty_cv_.notify_all();
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

    // Wait for frame processing to complete. Use a timeout so the destructor
    // cannot hang indefinitely if a future change reintroduces a missing
    // notify on one of the worker-thread early-exit paths.
    std::unique_lock lock(frame_queue_mutex_);
    if (!frame_queue_finished_cv_.wait_for(
          lock, std::chrono::seconds(300), [this] {
              return frame_queue_processing_done_.load() ||
                     frame_queue_->empty();
          })) {
        LOG_ERROR("Timed out waiting for frame queue to finalize after 300s; "
                  "proceeding with shutdown");
    }
}

bool
finalize_stream(ZarrStream* stream)
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

    // a worker job (e.g. a shard flush during streaming) may have failed
    // asynchronously; surface it rather than reporting a clean close
    if (!stream->error_.empty()) {
        LOG_ERROR("Error finalizing Zarr stream: ", stream->error_);
        return false;
    }

    {
        // get_memory_usage() may be polled from another thread right up to
        // here; keep it out of the slots being emptied
        std::unique_lock lock(stream->arrays_mutex_);
        for (auto& [key, output] : stream->arrays_) {
            if (!zarr::finalize_array(std::move(output->array))) {
                LOG_ERROR(
                  "Error finalizing Zarr stream. Failed to finalize array '",
                  key,
                  "'");
                return false;
            }
        }
    }

    // a group is written each time it gets attributes; write it once more
    // so that a write that failed part-way does not leave it broken
    if (!stream->write_groups_with_attributes_()) {
        LOG_ERROR("Error finalizing Zarr stream: ", stream->error_);
        return false;
    }

    return true;
}

uint32_t
stream_thread_count(const ZarrStream* stream)
{
    return stream->thread_pool_->n_threads();
}
