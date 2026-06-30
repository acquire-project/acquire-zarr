#include "settings.io.hh"
#include "acquire.zarr.h"
#include "macros.hh"

#include <yaml-cpp/yaml.h>

#include <cstdlib>
#include <cstring>
#include <fstream>
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
    if (node.Tag() == "tag:yaml.org,2002:str") {
        return s; // explicitly tagged string, e.g. a numeric well name
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
constexpr int kSchemaVersion = 1;

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
    { "decimate", ZarrDownsamplingMethod_Decimate },
    { "mean", ZarrDownsamplingMethod_Mean },
    { "min", ZarrDownsamplingMethod_Min },
    { "max", ZarrDownsamplingMethod_Max },
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

uint64_t
as_u64(const json& j, const std::string& ctx)
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

uint32_t
as_u32(const json& j, const std::string& ctx)
{
    return static_cast<uint32_t>(as_u64(j, ctx));
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
      as_u32(require(j, "array_size_px", ctx), ctx + ".array_size_px");
    dim->chunk_size_px =
      as_u32(require(j, "chunk_size_px", ctx), ctx + ".chunk_size_px");
    dim->shard_size_chunks =
      as_u32(require(j, "shard_size_chunks", ctx), ctx + ".shard_size_chunks");

    if (j.contains("unit") && !j.at("unit").is_null()) {
        dim->unit = dup_cstr(as_string(j.at("unit"), ctx + ".unit"));
    }
    dim->scale =
      j.contains("scale") ? as_double(j.at("scale"), ctx + ".scale") : 1.0;
}

void
load_array(const json& j,
           ZarrArraySettings* arr,
           const std::string& ctx,
           bool allow_output_key)
{
    if (allow_output_key && j.contains("output_key") &&
        !j.at("output_key").is_null()) {
        arr->output_key =
          dup_cstr(as_string(j.at("output_key"), ctx + ".output_key"));
    }

    arr->data_type = static_cast<ZarrDataType>(
      to_enum(kDataTypes,
              as_string(require(j, "data_type", ctx), ctx + ".data_type"),
              "data type"));

    arr->multiscale = j.contains("multiscale")
                        ? as_bool(j.at("multiscale"), ctx + ".multiscale")
                        : false;
    arr->downsampling_method = static_cast<ZarrDownsamplingMethod>(
      j.contains("downsampling_method")
        ? to_enum(kDownsamplingMethods,
                  as_string(j.at("downsampling_method"),
                            ctx + ".downsampling_method"),
                  "downsampling method")
        : ZarrDownsamplingMethod_Decimate);
    arr->max_levels = j.contains("max_levels")
                        ? as_u32(j.at("max_levels"), ctx + ".max_levels")
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
          c.contains("level")
            ? static_cast<uint8_t>(as_u32(c.at("level"), cctx + ".level"))
            : 0;
        cs->shuffle =
          c.contains("shuffle")
            ? static_cast<uint8_t>(as_u32(c.at("shuffle"), cctx + ".shuffle"))
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
              as_u64(order[i], ctx + ".storage_dimension_order"));
        }
    }
}

void
load_acquisition(const json& j, ZarrHCSAcquisition* acq, const std::string& ctx)
{
    acq->id = as_u32(require(j, "id", ctx), ctx + ".id");
    if (j.contains("name") && !j.at("name").is_null()) {
        acq->name = dup_cstr(as_string(j.at("name"), ctx + ".name"));
    }
    if (j.contains("description") && !j.at("description").is_null()) {
        acq->description =
          dup_cstr(as_string(j.at("description"), ctx + ".description"));
    }
    if (j.contains("start_time") && !j.at("start_time").is_null()) {
        acq->start_time = as_u64(j.at("start_time"), ctx + ".start_time");
        acq->has_start_time = true;
    }
    if (j.contains("end_time") && !j.at("end_time").is_null()) {
        acq->end_time = as_u64(j.at("end_time"), ctx + ".end_time");
        acq->has_end_time = true;
    }
}

void
load_fov(const json& j, ZarrHCSFieldOfView* fov, const std::string& ctx)
{
    fov->path = dup_cstr(as_string(require(j, "path", ctx), ctx + ".path"));
    if (j.contains("acquisition_id") && !j.at("acquisition_id").is_null()) {
        fov->acquisition_id =
          as_u32(j.at("acquisition_id"), ctx + ".acquisition_id");
        fov->has_acquisition_id = true;
    }
    fov->array_settings = alloc_zeroed<ZarrArraySettings>(1);
    load_array(
      require(j, "array", ctx), fov->array_settings, ctx + ".array", false);
}

void
load_well(const json& j, ZarrHCSWell* well, const std::string& ctx)
{
    well->row_name =
      dup_cstr(as_string(require(j, "row_name", ctx), ctx + ".row_name"));
    well->column_name =
      dup_cstr(as_string(require(j, "column_name", ctx), ctx + ".column_name"));

    const auto& images = require(j, "images", ctx);
    if (!images.is_array()) {
        fail(ctx + ".images", "expected a list");
    }
    well->image_count = images.size();
    well->images = alloc_zeroed<ZarrHCSFieldOfView>(images.size());
    for (size_t i = 0; i < images.size(); ++i) {
        load_fov(images[i],
                 &well->images[i],
                 ctx + ".images[" + std::to_string(i) + "]");
    }
}

std::vector<std::string>
as_string_list(const json& j, const std::string& ctx)
{
    if (!j.is_array()) {
        fail(ctx, "expected a list");
    }
    std::vector<std::string> out;
    out.reserve(j.size());
    for (const auto& e : j) {
        out.push_back(as_string(e, ctx));
    }
    return out;
}

const char**
dup_string_list(const std::vector<std::string>& src)
{
    auto* arr = alloc_zeroed<const char*>(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        arr[i] = dup_cstr(src[i]);
    }
    return arr;
}

void
load_plate(const json& j, ZarrHCSPlate* plate, const std::string& ctx)
{
    plate->path = dup_cstr(as_string(require(j, "path", ctx), ctx + ".path"));
    if (j.contains("name") && !j.at("name").is_null()) {
        plate->name = dup_cstr(as_string(j.at("name"), ctx + ".name"));
    }

    const auto rows =
      as_string_list(require(j, "row_names", ctx), ctx + ".row_names");
    plate->row_count = rows.size();
    plate->row_names = dup_string_list(rows);

    const auto cols =
      as_string_list(require(j, "column_names", ctx), ctx + ".column_names");
    plate->column_count = cols.size();
    plate->column_names = dup_string_list(cols);

    const auto& wells = require(j, "wells", ctx);
    if (!wells.is_array()) {
        fail(ctx + ".wells", "expected a list");
    }
    plate->well_count = wells.size();
    plate->wells = alloc_zeroed<ZarrHCSWell>(wells.size());
    for (size_t i = 0; i < wells.size(); ++i) {
        load_well(wells[i],
                  &plate->wells[i],
                  ctx + ".wells[" + std::to_string(i) + "]");
    }

    if (j.contains("acquisitions") && !j.at("acquisitions").is_null()) {
        const auto& acqs = j.at("acquisitions");
        if (!acqs.is_array()) {
            fail(ctx + ".acquisitions", "expected a list");
        }
        plate->acquisition_count = acqs.size();
        plate->acquisitions = alloc_zeroed<ZarrHCSAcquisition>(acqs.size());
        for (size_t i = 0; i < acqs.size(); ++i) {
            load_acquisition(acqs[i],
                             &plate->acquisitions[i],
                             ctx + ".acquisitions[" + std::to_string(i) + "]");
        }
    }
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
dump_array(const ZarrArraySettings& a, bool include_output_key)
{
    json j;
    if (include_output_key && a.output_key) {
        j["output_key"] = a.output_key;
    }
    j["data_type"] = from_enum(kDataTypes, a.data_type, "data type");
    j["multiscale"] = a.multiscale;
    if (a.multiscale) {
        j["downsampling_method"] = from_enum(
          kDownsamplingMethods, a.downsampling_method, "downsampling method");
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

json
dump_plate(const ZarrHCSPlate& p)
{
    json j;
    j["path"] = p.path ? p.path : "";
    if (p.name) {
        j["name"] = p.name;
    }
    j["row_names"] = json::array();
    for (size_t i = 0; i < p.row_count; ++i) {
        j["row_names"].push_back(p.row_names[i] ? p.row_names[i] : "");
    }
    j["column_names"] = json::array();
    for (size_t i = 0; i < p.column_count; ++i) {
        j["column_names"].push_back(p.column_names[i] ? p.column_names[i] : "");
    }
    if (p.acquisition_count) {
        auto acqs = json::array();
        for (size_t i = 0; i < p.acquisition_count; ++i) {
            const auto& a = p.acquisitions[i];
            json aj;
            aj["id"] = a.id;
            if (a.name) {
                aj["name"] = a.name;
            }
            if (a.description) {
                aj["description"] = a.description;
            }
            if (a.has_start_time) {
                aj["start_time"] = a.start_time;
            }
            if (a.has_end_time) {
                aj["end_time"] = a.end_time;
            }
            acqs.push_back(aj);
        }
        j["acquisitions"] = acqs;
    }
    j["wells"] = json::array();
    for (size_t i = 0; i < p.well_count; ++i) {
        const auto& w = p.wells[i];
        json wj;
        wj["row_name"] = w.row_name ? w.row_name : "";
        wj["column_name"] = w.column_name ? w.column_name : "";
        wj["images"] = json::array();
        for (size_t k = 0; k < w.image_count; ++k) {
            const auto& fov = w.images[k];
            json fj;
            fj["path"] = fov.path ? fov.path : "";
            if (fov.has_acquisition_id) {
                fj["acquisition_id"] = fov.acquisition_id;
            }
            if (fov.array_settings) {
                fj["array"] = dump_array(*fov.array_settings, false);
            }
            wj["images"].push_back(fj);
        }
        j["wells"].push_back(wj);
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
json_to_settings(const json& doc, ZarrStreamSettings* out)
{
    // zero first so a throw anywhere leaves a struct that is safe to pass to
    // destroy_loaded_settings (free(nullptr) is a no-op; zeroed counts skip)
    std::memset(out, 0, sizeof(*out));

    if (!doc.is_object()) {
        fail("", "config root must be a mapping");
    }
    if (doc.contains("version")) {
        const auto v = as_u64(doc.at("version"), "version");
        if (v != kSchemaVersion) {
            fail("version",
                 "unsupported schema version " + std::to_string(v) +
                   " (expected " + std::to_string(kSchemaVersion) + ")");
        }
    }

    out->store_path =
      dup_cstr(as_string(require(doc, "store_path", ""), "store_path"));
    out->overwrite = doc.contains("overwrite")
                       ? as_bool(doc.at("overwrite"), "overwrite")
                       : false;
    out->max_threads = doc.contains("max_threads")
                         ? static_cast<unsigned int>(
                             as_u32(doc.at("max_threads"), "max_threads"))
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
            load_array(arrays[i],
                       &out->arrays[i],
                       "arrays[" + std::to_string(i) + "]",
                       true);
        }
    }

    if (doc.contains("plates") && !doc.at("plates").is_null()) {
        const auto& plates = doc.at("plates");
        if (!plates.is_array()) {
            fail("plates", "expected a list");
        }
        auto* hcs = alloc_zeroed<ZarrHCSSettings>(1);
        out->hcs_settings = hcs;
        hcs->plate_count = plates.size();
        hcs->plates = alloc_zeroed<ZarrHCSPlate>(plates.size());
        for (size_t i = 0; i < plates.size(); ++i) {
            load_plate(
              plates[i], &hcs->plates[i], "plates[" + std::to_string(i) + "]");
        }
    }

    if (!out->arrays && !out->hcs_settings) {
        fail("", "config must define 'arrays' and/or 'plates'");
    }
}

json
settings_to_json(const ZarrStreamSettings* s)
{
    json doc;
    doc["version"] = kSchemaVersion;
    doc["store_path"] = s->store_path ? s->store_path : "";
    doc["overwrite"] = s->overwrite;
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
            doc["arrays"].push_back(dump_array(s->arrays[i], true));
        }
    }

    if (s->hcs_settings && s->hcs_settings->plate_count) {
        doc["plates"] = json::array();
        for (size_t i = 0; i < s->hcs_settings->plate_count; ++i) {
            doc["plates"].push_back(dump_plate(s->hcs_settings->plates[i]));
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

    if (s->hcs_settings) {
        for (size_t p = 0; p < s->hcs_settings->plate_count; ++p) {
            auto& plate = s->hcs_settings->plates[p];
            free_cstr(plate.path);
            free_cstr(plate.name);
            for (size_t i = 0; i < plate.row_count; ++i) {
                free_cstr(plate.row_names[i]);
            }
            std::free(const_cast<const char**>(plate.row_names));
            for (size_t i = 0; i < plate.column_count; ++i) {
                free_cstr(plate.column_names[i]);
            }
            std::free(const_cast<const char**>(plate.column_names));
            for (size_t i = 0; i < plate.acquisition_count; ++i) {
                free_cstr(plate.acquisitions[i].name);
                free_cstr(plate.acquisitions[i].description);
            }
            std::free(plate.acquisitions);
            for (size_t i = 0; i < plate.well_count; ++i) {
                auto& well = plate.wells[i];
                free_cstr(well.row_name);
                free_cstr(well.column_name);
                for (size_t k = 0; k < well.image_count; ++k) {
                    free_cstr(well.images[k].path);
                    free_array_contents(well.images[k].array_settings);
                    std::free(well.images[k].array_settings);
                }
                std::free(well.images);
            }
            std::free(plate.wells);
        }
        std::free(s->hcs_settings->plates);
        std::free(s->hcs_settings);
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
