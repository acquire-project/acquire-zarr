#include "settings.io.hh"
#include "acquire.zarr.h"
#include "macros.hh"

#include <yaml-cpp/yaml.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

using json = nlohmann::json;

// ---------------------------------------------------------------------------
// YAML <-> JSON bridge (JSON is a subset of YAML, so one parse path serves
// both)
// ---------------------------------------------------------------------------
namespace {
json
scalar_to_json(const YAML::Node& node)
{
    const auto& s = node.Scalar();
    // yaml-cpp tags quoted scalars "!"; an explicit !!str tag becomes
    // "tag:yaml.org,2002:str". Either means "treat as string verbatim",
    // e.g. a numeric well name or an intentional empty string.
    if (node.Tag() == "!" || node.Tag() == "tag:yaml.org,2002:str") {
        return s;
    }

    // Strict JSON-style booleans only. yaml-cpp follows YAML 1.1, where y/yes/
    // no/on/off are booleans -- that would turn a dimension named "y" into
    // true.
    if (s == "true") {
        return true;
    }
    if (s == "false") {
        return false;
    }
    int64_t i;
    if (YAML::convert<int64_t>::decode(node, i)) {
        return i;
    }
    uint64_t u;
    if (YAML::convert<uint64_t>::decode(node, u)) {
        return u;
    }
    double d;
    if (YAML::convert<double>::decode(node, d)) {
        return d;
    }
    if (s.empty() || s == "~" || s == "null") {
        return nullptr;
    }
    return s;
}

json
yaml_to_json(const YAML::Node& node)
{
    switch (node.Type()) {
        case YAML::NodeType::Null:
        case YAML::NodeType::Undefined:
            return nullptr;
        case YAML::NodeType::Scalar:
            return scalar_to_json(node);
        case YAML::NodeType::Sequence: {
            auto arr = json::array();
            for (const auto& child : node) {
                arr.push_back(yaml_to_json(child));
            }
            return arr;
        }
        case YAML::NodeType::Map: {
            auto obj = json::object();
            for (const auto& kv : node) {
                obj[kv.first.as<std::string>()] = yaml_to_json(kv.second);
            }
            return obj;
        }
    }
    return nullptr;
}

// A string needs quoting on emit iff loading it back would not yield a string
// (mirrors scalar_to_json). Otherwise a well named "5" would round-trip to int.
bool
reparses_as_nonstring(const std::string& s)
{
    if (s == "true" || s == "false") {
        return true;
    }
    if (s.empty() || s == "~" || s == "null") {
        return true;
    }
    const YAML::Node n(s);

    if (int64_t i; YAML::convert<int64_t>::decode(n, i)) {
        return true;
    }

    if (uint64_t u; YAML::convert<uint64_t>::decode(n, u)) {
        return true;
    }

    if (double d; YAML::convert<double>::decode(n, d)) {
        return true;
    }

    return false;
}

void
emit_json(YAML::Emitter& e, const json& doc)
{
    switch (doc.type()) {
        case json::value_t::object:
            e << YAML::BeginMap;
            for (const auto& [key, value] : doc.items()) {
                e << YAML::Key << key << YAML::Value;
                emit_json(e, value);
            }
            e << YAML::EndMap;
            break;
        case json::value_t::array:
            e << YAML::BeginSeq;
            for (const auto& value : doc) {
                emit_json(e, value);
            }
            e << YAML::EndSeq;
            break;
        case json::value_t::string: {
            const auto s = doc.get<std::string>();
            if (reparses_as_nonstring(s)) {
                e << YAML::DoubleQuoted; // one-shot: applies to the next scalar
            }
            e << s;
            break;
        }
        case json::value_t::boolean:
            e << doc.get<bool>();
            break;
        case json::value_t::number_integer:
            e << doc.get<int64_t>();
            break;
        case json::value_t::number_unsigned:
            e << doc.get<uint64_t>();
            break;
        case json::value_t::number_float:
            e << doc.get<double>();
            break;
        case json::value_t::null:
            e << YAML::Null;
            break;
        default:
            e << doc.dump();
            break;
    }
}
} // namespace

// ---------------------------------------------------------------------------
// Schema version, enum tables, and typed JSON accessors
// ---------------------------------------------------------------------------
namespace {
// version 1 shipped in 0.9.0 and gated downsampling behind `multiscale`;
// version 2 replaced that flag with `is_ngff` (#213). Version-1 configs are
// still accepted and translated in migrate_v1(). Version 3 removed `plates`
// with the typed HCS settings, and added `attributes`; a config of any
// version that has `plates` is refused with a message.
constexpr uint64_t kSchemaVersion = 3;
constexpr uint64_t kMinSchemaVersion = 1;

struct EnumEntry
{
    const char* name;
    int value;
};

constexpr EnumEntry kDataTypes[] = {
    { "uint8", ZarrDataType_uint8 },     { "uint16", ZarrDataType_uint16 },
    { "uint32", ZarrDataType_uint32 },   { "uint64", ZarrDataType_uint64 },
    { "int8", ZarrDataType_int8 },       { "int16", ZarrDataType_int16 },
    { "int32", ZarrDataType_int32 },     { "int64", ZarrDataType_int64 },
    { "float32", ZarrDataType_float32 }, { "float64", ZarrDataType_float64 },
};

constexpr EnumEntry kDimensionTypes[] = {
    { "space", ZarrDimensionType_Space },
    { "channel", ZarrDimensionType_Channel },
    { "time", ZarrDimensionType_Time },
    { "other", ZarrDimensionType_Other },
};

constexpr EnumEntry kCompressors[] = {
    { "none", ZarrCompressor_None },
    { "blosc1", ZarrCompressor_Blosc1 },
    { "zstd", ZarrCompressor_Zstd },
};

constexpr EnumEntry kCodecs[] = {
    { "none", ZarrCompressionCodec_None },
    { "blosc-lz4", ZarrCompressionCodec_BloscLZ4 },
    { "blosc-zstd", ZarrCompressionCodec_BloscZstd },
    { "zstd", ZarrCompressionCodec_Zstd },
};

constexpr EnumEntry kDownsamplingMethods[] = {
    { "none", ZarrDownsamplingMethod_None },
    { "decimate", ZarrDownsamplingMethod_Decimate },
    { "mean", ZarrDownsamplingMethod_Mean },
    { "min", ZarrDownsamplingMethod_Min },
    { "max", ZarrDownsamplingMethod_Max },
};

constexpr EnumEntry kIntermediateGroups[] = {
    { "if_missing", ZarrIntermediateGroups_IfMissing },
    { "always", ZarrIntermediateGroups_Always },
    { "never", ZarrIntermediateGroups_Never },
};

template<size_t N>
int
to_enum(const EnumEntry (&table)[N], const std::string& s, const char* what)
{
    for (const auto& e : table) {
        if (s == e.name) {
            return e.value;
        }
    }
    throw std::runtime_error("invalid " + std::string(what) + ": '" + s + "'");
}

template<size_t N>
const char*
from_enum(const EnumEntry (&table)[N], int value, const char* what)
{
    for (const auto& e : table) {
        if (value == e.value) {
            return e.name;
        }
    }
    throw std::runtime_error("unmappable " + std::string(what) +
                             " value: " + std::to_string(value));
}

[[noreturn]] void
fail(const std::string& ctx, const std::string& msg)
{
    throw std::runtime_error(ctx.empty() ? msg : ctx + ": " + msg);
}

const json&
require(const json& obj, const char* key, const std::string& ctx)
{
    if (!obj.is_object() || !obj.contains(key)) {
        fail(ctx, "missing required field '" + std::string(key) + "'");
    }
    return obj.at(key);
}

std::string
as_string(const json& j, const std::string& ctx)
{
    if (j.is_string()) {
        return j.get<std::string>();
    }
    if (j.is_number_unsigned()) {
        return std::to_string(j.get<uint64_t>());
    }
    if (j.is_number_integer()) {
        return std::to_string(j.get<int64_t>());
    }
    if (j.is_boolean()) {
        return j.get<bool>() ? "true" : "false";
    }
    fail(ctx, "expected a string");
}

template<typename T>
T as_uint(const json& j, const std::string& ctx);

template<>
uint64_t as_uint<uint64_t>(const json& j, const std::string& ctx);

template<typename T>
T
as_uint(const json& j, const std::string& ctx)
{
    static_assert(std::is_unsigned_v<T>);
    const auto v = as_uint<uint64_t>(j, ctx);
    if (v > std::numeric_limits<T>::max()) {
        fail(ctx,
             "value out of range (max " +
               std::to_string(std::numeric_limits<T>::max()) + ")");
    }
    return static_cast<T>(v);
}

template<>
uint64_t
as_uint(const json& j, const std::string& ctx)
{
    if (j.is_number_unsigned()) {
        return j.get<uint64_t>();
    }
    if (j.is_number_integer()) {
        const auto v = j.get<int64_t>();
        if (v < 0) {
            fail(ctx, "expected a non-negative integer");
        }
        return static_cast<uint64_t>(v);
    }
    if (j.is_string()) {
        try {
            return std::stoull(j.get<std::string>());
        } catch (...) {
            fail(ctx, "expected an integer");
        }
    }
    fail(ctx, "expected an integer");
}

bool
as_bool(const json& j, const std::string& ctx)
{
    if (j.is_boolean()) {
        return j.get<bool>();
    }
    fail(ctx, "expected a boolean");
}

double
as_double(const json& j, const std::string& ctx)
{
    if (j.is_number()) {
        return j.get<double>();
    }
    if (j.is_string()) {
        try {
            return std::stod(j.get<std::string>());
        } catch (...) {
            fail(ctx, "expected a number");
        }
    }
    fail(ctx, "expected a number");
}

// ---------------------------------------------------------------------------
// Owning allocation helpers (freed by destroy_loaded_settings)
// ---------------------------------------------------------------------------
char*
dup_cstr(const std::string& s)
{
    auto* p = static_cast<char*>(std::malloc(s.size() + 1));
    if (!p) {
        throw std::bad_alloc();
    }
    std::memcpy(p, s.c_str(), s.size() + 1);
    return p;
}

void
free_cstr(const char* p)
{
    std::free(const_cast<char*>(p));
}

template<typename T>
T*
alloc_zeroed(size_t n)
{
    if (n == 0) {
        return nullptr;
    }
    auto* p = static_cast<T*>(std::calloc(n, sizeof(T)));
    if (!p) {
        throw std::bad_alloc();
    }
    return p;
}
} // namespace

// ---------------------------------------------------------------------------
// Schema version 1 -> 2 migration
// ---------------------------------------------------------------------------
namespace {
void
migrate_array_v1(json& j, const std::string& ctx)
{
    if (!j.is_object()) {
        return; // load_array will report the real problem
    }

    // migration writes `is_ngff`, so a version-1 config that sets it would
    // have its value silently discarded
    if (j.contains("is_ngff")) {
        fail(ctx + ".is_ngff",
             "'is_ngff' requires schema version 2; this config declares "
             "version 1, where the equivalent field is 'multiscale'");
    }

    const bool multiscale = j.contains("multiscale") &&
                            as_bool(j.at("multiscale"), ctx + ".multiscale");
    j.erase("multiscale");

    j["is_ngff"] = multiscale;
    if (!multiscale) {
        // version 1 read `downsampling_method` only when `multiscale` was true
        j["downsampling_method"] = "none";
    } else if (!j.contains("downsampling_method")) {
        // version 1's zero value was Decimate
        j["downsampling_method"] = "decimate";
    }
}

void
migrate_v1(json& doc)
{
    if (doc.contains("arrays") && doc.at("arrays").is_array()) {
        auto& arrays = doc.at("arrays");
        for (size_t i = 0; i < arrays.size(); ++i) {
            migrate_array_v1(arrays[i], "arrays[" + std::to_string(i) + "]");
        }
    }
}
} // namespace

// ---------------------------------------------------------------------------
// JSON -> settings (owning)
// ---------------------------------------------------------------------------
namespace {
void
load_dimension(const json& j,
               ZarrDimensionProperties* dim,
               const std::string& ctx)
{
    dim->name = dup_cstr(as_string(require(j, "name", ctx), ctx + ".name"));
    dim->type = static_cast<ZarrDimensionType>(
      to_enum(kDimensionTypes,
              as_string(require(j, "type", ctx), ctx + ".type"),
              "dimension type"));
    dim->array_size_px =
      as_uint<uint32_t>(require(j, "array_size_px", ctx), ctx + ".array_size_px");
    dim->chunk_size_px =
      as_uint<uint32_t>(require(j, "chunk_size_px", ctx), ctx + ".chunk_size_px");
    dim->shard_size_chunks =
      as_uint<uint32_t>(require(j, "shard_size_chunks", ctx),
                        ctx + ".shard_size_chunks");

    if (j.contains("unit") && !j.at("unit").is_null()) {
        dim->unit = dup_cstr(as_string(j.at("unit"), ctx + ".unit"));
    }
    dim->scale =
      j.contains("scale") ? as_double(j.at("scale"), ctx + ".scale") : 1.0;
}

void
load_array(const json& j, ZarrArraySettings* arr, const std::string& ctx)
{
    if (j.contains("output_key") && !j.at("output_key").is_null()) {
        arr->output_key =
          dup_cstr(as_string(j.at("output_key"), ctx + ".output_key"));
    }

    arr->data_type = static_cast<ZarrDataType>(
      to_enum(kDataTypes,
              as_string(require(j, "data_type", ctx), ctx + ".data_type"),
              "data type"));

    if (j.contains("multiscale")) {
        throw std::runtime_error(
          ctx +
          ".multiscale: 'multiscale' has been replaced by 'is_ngff'; set "
          "'is_ngff' and/or 'downsampling_method' instead, or declare "
          "'version: 1' to load this config with version-1 semantics");
    }

    arr->is_ngff =
      j.contains("is_ngff") ? as_bool(j.at("is_ngff"), ctx + ".is_ngff") : false;
    arr->downsampling_method = static_cast<ZarrDownsamplingMethod>(
      j.contains("downsampling_method")
        ? to_enum(kDownsamplingMethods,
                  as_string(j.at("downsampling_method"),
                            ctx + ".downsampling_method"),
                  "downsampling method")
        : ZarrDownsamplingMethod_None);
    arr->max_levels = j.contains("max_levels")
                        ? as_uint<uint32_t>(j.at("max_levels"),
                                            ctx + ".max_levels")
                        : 0;

    if (j.contains("compression") && !j.at("compression").is_null()) {
        const auto& c = j.at("compression");
        const auto cctx = ctx + ".compression";
        auto* cs = alloc_zeroed<ZarrCompressionSettings>(1);
        arr->compression_settings = cs;
        cs->compressor = static_cast<ZarrCompressor>(to_enum(
          kCompressors,
          as_string(require(c, "compressor", cctx), cctx + ".compressor"),
          "compressor"));
        cs->codec = static_cast<ZarrCompressionCodec>(
          to_enum(kCodecs,
                  as_string(require(c, "codec", cctx), cctx + ".codec"),
                  "codec"));
        cs->level =
          c.contains("level") ? as_uint<uint8_t>(c.at("level"), cctx + ".level")
                              : 0;
        cs->shuffle = c.contains("shuffle")
                        ? as_uint<uint8_t>(c.at("shuffle"), cctx + ".shuffle")
                        : 0;
    }

    const auto& dims = require(j, "dimensions", ctx);
    if (!dims.is_array() || dims.empty()) {
        fail(ctx + ".dimensions", "expected a non-empty list");
    }
    arr->dimension_count = dims.size();
    arr->dimensions = alloc_zeroed<ZarrDimensionProperties>(dims.size());
    for (size_t i = 0; i < dims.size(); ++i) {
        load_dimension(dims[i],
                       &arr->dimensions[i],
                       ctx + ".dimensions[" + std::to_string(i) + "]");
    }

    if (j.contains("storage_dimension_order") &&
        !j.at("storage_dimension_order").is_null()) {
        const auto& order = j.at("storage_dimension_order");
        if (!order.is_array() || order.size() != arr->dimension_count) {
            fail(ctx + ".storage_dimension_order",
                 "must have one entry per dimension");
        }
        auto* buf = alloc_zeroed<size_t>(order.size());
        arr->storage_dimension_order = buf;
        for (size_t i = 0; i < order.size(); ++i) {
            buf[i] = static_cast<size_t>(
              as_uint<uint64_t>(order[i], ctx + ".storage_dimension_order"));
        }
    }
}

void
load_attributes(const json& j, ZarrAttributes* entry, const std::string& ctx)
{
    if (!j.is_object()) {
        fail(ctx, "expected a mapping");
    }
    if (j.contains("node_key") && !j.at("node_key").is_null()) {
        entry->node_key =
          dup_cstr(as_string(j.at("node_key"), ctx + ".node_key"));
    }
    if (j.contains("key") && !j.at("key").is_null()) {
        entry->key = dup_cstr(as_string(j.at("key"), ctx + ".key"));
    }

    // any JSON value; the stream checks it at create
    entry->value = dup_cstr(require(j, "value", ctx).dump());
}
} // namespace

// ---------------------------------------------------------------------------
// settings -> JSON (dump)
// ---------------------------------------------------------------------------
namespace {
json
dump_dimension(const ZarrDimensionProperties& d)
{
    json j;
    j["name"] = d.name ? d.name : "";
    j["type"] = from_enum(kDimensionTypes, d.type, "dimension type");
    j["array_size_px"] = d.array_size_px;
    j["chunk_size_px"] = d.chunk_size_px;
    j["shard_size_chunks"] = d.shard_size_chunks;
    if (d.unit) {
        j["unit"] = d.unit;
    }
    j["scale"] = d.scale;
    return j;
}

json
dump_array(const ZarrArraySettings& a)
{
    json j;
    if (a.output_key) {
        j["output_key"] = a.output_key;
    }
    j["data_type"] = from_enum(kDataTypes, a.data_type, "data type");
    j["is_ngff"] = a.is_ngff;
    j["downsampling_method"] = from_enum(
      kDownsamplingMethods, a.downsampling_method, "downsampling method");
    if (a.downsampling_method > ZarrDownsamplingMethod_None) {
        j["max_levels"] = a.max_levels;
    }
    if (a.compression_settings) {
        const auto& c = *a.compression_settings;
        j["compression"] = {
            { "compressor",
              from_enum(kCompressors, c.compressor, "compressor") },
            { "codec", from_enum(kCodecs, c.codec, "codec") },
            { "level", c.level },
            { "shuffle", c.shuffle },
        };
    }
    j["dimensions"] = json::array();
    for (size_t i = 0; i < a.dimension_count; ++i) {
        j["dimensions"].push_back(dump_dimension(a.dimensions[i]));
    }
    if (a.storage_dimension_order) {
        auto order = json::array();
        for (size_t i = 0; i < a.dimension_count; ++i) {
            order.push_back(a.storage_dimension_order[i]);
        }
        j["storage_dimension_order"] = order;
    }
    return j;
}

} // namespace

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------
namespace zarr {
json
config_text_to_json(const std::string& text)
{
    return yaml_to_json(YAML::Load(text));
}

std::string
json_to_yaml(const json& doc)
{
    YAML::Emitter emitter;
    emit_json(emitter, doc);
    return emitter.c_str();
}

void
json_to_settings(const json& doc_in, ZarrStreamSettings* out)
{
    // zero first so a throw anywhere leaves a struct that is safe to pass to
    // destroy_loaded_settings (free(nullptr) is a no-op; zeroed counts skip)
    std::memset(out, 0, sizeof(*out));

    if (!doc_in.is_object()) {
        fail("", "config root must be a mapping");
    }

    // an absent 'version' means the current schema
    uint64_t version = kSchemaVersion;
    if (doc_in.contains("version")) {
        const auto v = as_uint<uint64_t>(doc_in.at("version"), "version");
        if (v < kMinSchemaVersion || v > kSchemaVersion) {
            fail("version",
                 "unsupported schema version " + std::to_string(v) +
                   " (supported: " + std::to_string(kMinSchemaVersion) +
                   " through " + std::to_string(kSchemaVersion) + ")");
        }
        version = v;
    }

    if (doc_in.contains("plates")) {
        fail("plates",
             "HCS plate settings were removed in schema version 3. Configure "
             "each field of view as an array with output_key "
             "'<plate>/<row>/<column>/<field>' and is_ngff set, and give the "
             "plate and well metadata under 'attributes' (see "
             "examples/config/hcs.yaml)");
    }

    // rewrite an older config into the current shape, so the loaders below
    // only ever see one schema
    json migrated;
    if (version == 1) {
        migrated = doc_in;
        migrate_v1(migrated);
    }
    const json& doc = migrated.is_null() ? doc_in : migrated;

    out->store_path =
      dup_cstr(as_string(require(doc, "store_path", ""), "store_path"));
    out->overwrite = doc.contains("overwrite")
                       ? as_bool(doc.at("overwrite"), "overwrite")
                       : false;
    out->intermediate_groups = static_cast<ZarrIntermediateGroups>(
      doc.contains("intermediate_groups")
        ? to_enum(
            kIntermediateGroups,
            as_string(doc.at("intermediate_groups"), "intermediate_groups"),
            "intermediate groups mode")
        : ZarrIntermediateGroups_IfMissing);
    out->max_threads = doc.contains("max_threads")
                         ? static_cast<unsigned int>(
                             as_uint<uint32_t>(doc.at("max_threads"), "max_threads"))
                         : 0;

    if (doc.contains("s3") && !doc.at("s3").is_null()) {
        const auto& s = doc.at("s3");
        auto* s3 = alloc_zeroed<ZarrS3Settings>(1);
        out->s3_settings = s3;
        s3->endpoint =
          dup_cstr(as_string(require(s, "endpoint", "s3"), "s3.endpoint"));
        s3->bucket_name = dup_cstr(
          as_string(require(s, "bucket_name", "s3"), "s3.bucket_name"));
        if (s.contains("region") && !s.at("region").is_null()) {
            s3->region = dup_cstr(as_string(s.at("region"), "s3.region"));
        }
    }

    if (doc.contains("arrays") && !doc.at("arrays").is_null()) {
        const auto& arrays = doc.at("arrays");
        if (!arrays.is_array()) {
            fail("arrays", "expected a list");
        }
        out->array_count = arrays.size();
        out->arrays = alloc_zeroed<ZarrArraySettings>(arrays.size());
        for (size_t i = 0; i < arrays.size(); ++i) {
            load_array(
              arrays[i], &out->arrays[i], "arrays[" + std::to_string(i) + "]");
        }
    }

    if (doc.contains("attributes") && !doc.at("attributes").is_null()) {
        // a reader of an earlier schema would ignore the key and lose the
        // metadata, so a config that has it must declare the schema that does
        if (version < 3) {
            fail("attributes",
                 "'attributes' requires schema version 3; this config "
                 "declares version " +
                   std::to_string(version));
        }
        const auto& entries = doc.at("attributes");
        if (!entries.is_array()) {
            fail("attributes", "expected a list");
        }
        out->attributes_count = entries.size();
        out->attributes = alloc_zeroed<ZarrAttributes>(entries.size());
        for (size_t i = 0; i < entries.size(); ++i) {
            load_attributes(entries[i],
                            &out->attributes[i],
                            "attributes[" + std::to_string(i) + "]");
        }
    }

    if (!out->arrays) {
        fail("", "config must define 'arrays'");
    }
}

namespace {
json
dump_attributes(const ZarrAttributes& entry, const std::string& ctx)
{
    json j = json::object();
    if (entry.node_key) {
        j["node_key"] = entry.node_key;
    }
    if (entry.key) {
        j["key"] = entry.key;
    }

    // text that is not JSON has no faithful form in the file: as a YAML
    // string it would reload as a valid JSON string, so refuse it
    auto metadata =
      json::parse(entry.value ? entry.value : "", nullptr, false, true);
    if (metadata.is_discarded()) {
        fail(ctx, "value is not valid JSON");
    }
    j["value"] = std::move(metadata);
    return j;
}
} // namespace

json
settings_to_json(const ZarrStreamSettings* s)
{
    json doc;
    doc["version"] = kSchemaVersion;
    doc["store_path"] = s->store_path ? s->store_path : "";
    doc["overwrite"] = s->overwrite;
    doc["intermediate_groups"] = from_enum(
      kIntermediateGroups, s->intermediate_groups, "intermediate groups mode");
    doc["max_threads"] = s->max_threads;

    if (s->s3_settings) {
        const auto& s3 = *s->s3_settings;
        doc["s3"] = {
            { "endpoint", s3.endpoint ? s3.endpoint : "" },
            { "bucket_name", s3.bucket_name ? s3.bucket_name : "" },
        };
        if (s3.region) {
            doc["s3"]["region"] = s3.region;
        }
    }

    if (s->arrays && s->array_count) {
        doc["arrays"] = json::array();
        for (size_t i = 0; i < s->array_count; ++i) {
            doc["arrays"].push_back(dump_array(s->arrays[i]));
        }
    }

    if (s->attributes && s->attributes_count) {
        doc["attributes"] = json::array();
        for (size_t i = 0; i < s->attributes_count; ++i) {
            doc["attributes"].push_back(dump_attributes(
              s->attributes[i], "attributes[" + std::to_string(i) + "]"));
        }
    }

    return doc;
}

namespace {
void
free_array_contents(ZarrArraySettings* a)
{
    if (!a) {
        return;
    }
    free_cstr(a->output_key);
    std::free(a->compression_settings);
    std::free(const_cast<size_t*>(a->storage_dimension_order));
    if (a->dimensions) {
        for (size_t i = 0; i < a->dimension_count; ++i) {
            free_cstr(a->dimensions[i].name);
            free_cstr(a->dimensions[i].unit);
        }
        std::free(a->dimensions);
    }
}
} // namespace

void
destroy_loaded_settings(ZarrStreamSettings* s)
{
    if (!s) {
        return;
    }

    free_cstr(s->store_path);

    if (s->s3_settings) {
        free_cstr(s->s3_settings->endpoint);
        free_cstr(s->s3_settings->bucket_name);
        free_cstr(s->s3_settings->region);
        std::free(s->s3_settings);
    }

    if (s->arrays) {
        for (size_t i = 0; i < s->array_count; ++i) {
            free_array_contents(&s->arrays[i]);
        }
        std::free(s->arrays);
    }

    if (s->attributes) {
        for (size_t i = 0; i < s->attributes_count; ++i) {
            free_cstr(s->attributes[i].node_key);
            free_cstr(s->attributes[i].key);
            free_cstr(s->attributes[i].value);
        }
        std::free(s->attributes);
    }

    std::memset(s, 0, sizeof(*s));
}
} // namespace zarr

// ---------------------------------------------------------------------------
// C entrypoints
// ---------------------------------------------------------------------------
namespace {
std::string
read_file(const std::string& path)
{
    std::ifstream f(path, std::ios::binary);
    if (!f) {
        throw std::runtime_error("Failed to open config file: " + path);
    }
    std::ostringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

ZarrStatusCode
load_from(const std::string& text, ZarrStreamSettings* settings)
{
    try {
        zarr::json_to_settings(zarr::config_text_to_json(text), settings);
    } catch (const std::exception& exc) {
        LOG_ERROR("Failed to load settings: ", exc.what());
        zarr::destroy_loaded_settings(
          settings); // release any partial allocation
        return ZarrStatusCode_InvalidSettings;
    }
    return ZarrStatusCode_Success;
}
} // namespace

extern "C"
{
    ZarrStatusCode ZarrStreamSettings_load_from_file(
      ZarrStreamSettings* settings,
      const char* path)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(path, "Null pointer: path");
        try {
            return load_from(read_file(path), settings);
        } catch (const std::exception& exc) {
            LOG_ERROR("Failed to read config file: ", exc.what());
            return ZarrStatusCode_IOError;
        }
    }

    ZarrStatusCode ZarrStreamSettings_load_from_string(
      ZarrStreamSettings* settings,
      const char* text)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(text, "Null pointer: text");
        return load_from(text, settings);
    }

    void ZarrStreamSettings_destroy_loaded(ZarrStreamSettings* settings)
    {
        if (settings) {
            zarr::destroy_loaded_settings(settings);
        }
    }

    ZarrStatusCode ZarrStreamSettings_dump_to_file(
      const ZarrStreamSettings* settings,
      const char* path)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(path, "Null pointer: path");

        std::string p(path);
        const bool js = p.size() >= 5 && p.substr(p.size() - 5) == ".json";

        try {
            const auto doc = zarr::settings_to_json(settings);
            const std::string out = js ? doc.dump(2) : zarr::json_to_yaml(doc);
            std::ofstream f(p, std::ios::binary);
            if (!f) {
                LOG_ERROR("Failed to open output file: ", p);
                return ZarrStatusCode_IOError;
            }
            f << out;
        } catch (const std::exception& exc) {
            LOG_ERROR("Failed to dump settings: ", exc.what());
            return ZarrStatusCode_InternalError;
        }
        return ZarrStatusCode_Success;
    }

    ZarrStatusCode ZarrStreamSettings_dump_to_string(
      const ZarrStreamSettings* settings,
      char** text,
      ZarrConfigFormat format)
    {
        EXPECT_VALID_ARGUMENT(settings, "Null pointer: settings");
        EXPECT_VALID_ARGUMENT(text, "Null pointer: text");
        EXPECT_VALID_ARGUMENT(format < ZarrConfigFormatCount,
                              "Invalid config format: ",
                              static_cast<int>(format));

        try {
            const auto doc = zarr::settings_to_json(settings);
            const std::string out = format == ZarrConfigFormat_Json
                                      ? doc.dump(2)
                                      : zarr::json_to_yaml(doc);

            auto* buf = static_cast<char*>(std::malloc(out.size() + 1));
            if (!buf) {
                return ZarrStatusCode_OutOfMemory;
            }
            std::memcpy(buf, out.c_str(), out.size() + 1);
            *text = buf;
        } catch (const std::exception& exc) {
            LOG_ERROR("Failed to dump settings: ", exc.what());
            return ZarrStatusCode_InternalError;
        }
        return ZarrStatusCode_Success;
    }
}
