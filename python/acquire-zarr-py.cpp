#include <cstdlib>
#include <iostream>
#include <memory>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "acquire.zarr.h"

#ifdef _DEBUG
#include <crtdbg.h>
#endif

namespace py = pybind11;

namespace {
auto ZarrStreamDeleter = [](ZarrStream_s* stream) {
    if (stream) {
        ZarrStream_destroy(stream);
    }
};

struct ArrayLifetimeProps
{
    std::string output_key;
    std::vector<std::string> names;
    std::vector<std::string> units;
    std::vector<ZarrDimensionType> types;
    std::vector<double> scales;
    std::vector<uint32_t> array_sizes;
    std::vector<uint32_t> chunk_sizes;
    std::vector<uint32_t> shard_sizes;
    std::vector<size_t> storage_dimension_order;

    ZarrDataType data_type{};

    ZarrCompressionSettings compression{};
    bool has_compression{ false };

    std::optional<ZarrDownsamplingMethod> downsampling_method;
    bool is_ngff{ false };
    uint32_t max_levels{ 0 };

    ZarrArraySettings* array_settings()
    {
        if (output_key.empty()) {
            array_settings_.output_key = nullptr;
        } else {
            array_settings_.output_key = output_key.c_str();
        }

        if (has_compression) {
            array_settings_.compression_settings = &compression;
        } else {
            array_settings_.compression_settings = nullptr;
        }

        const size_t n_dims = names.size();

        if (auto code = ZarrArraySettings_create_dimension_array(
              &array_settings_, n_dims);
            code != ZarrStatusCode_Success) {
            const std::string err =
              "Failed to allocate memory for dimension array: " +
              std::string(Zarr_get_status_message(code));
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }

        for (auto i = 0; i < n_dims; ++i) {
            array_settings_.dimensions[i].name = names[i].c_str();
            array_settings_.dimensions[i].unit = units[i].c_str();
            array_settings_.dimensions[i].type = types[i];
            array_settings_.dimensions[i].array_size_px = array_sizes[i];
            array_settings_.dimensions[i].chunk_size_px = chunk_sizes[i];
            array_settings_.dimensions[i].shard_size_chunks = shard_sizes[i];
            array_settings_.dimensions[i].scale = scales[i];
        }

        array_settings_.data_type = data_type;
        array_settings_.downsampling_method =
          downsampling_method.value_or(ZarrDownsamplingMethod_None);
        array_settings_.is_ngff = is_ngff;
        array_settings_.max_levels = max_levels;

        if (!storage_dimension_order.empty()) {
            array_settings_.storage_dimension_order =
              storage_dimension_order.data();
        } else {
            array_settings_.storage_dimension_order = nullptr;
        }

        return &array_settings_;
    }

  private:
    ZarrArraySettings array_settings_{};
};

const char*
data_type_to_str(ZarrDataType t)
{
    switch (t) {
        case ZarrDataType_uint8:
            return "UINT8";
        case ZarrDataType_uint16:
            return "UINT16";
        case ZarrDataType_uint32:
            return "UINT32";
        case ZarrDataType_uint64:
            return "UINT64";
        case ZarrDataType_int8:
            return "INT8";
        case ZarrDataType_int16:
            return "INT16";
        case ZarrDataType_int32:
            return "INT32";
        case ZarrDataType_int64:
            return "INT64";
        case ZarrDataType_float32:
            return "FLOAT32";
        case ZarrDataType_float64:
            return "FLOAT64";
        default:
            return "UNKNOWN";
    }
}

const char*
compressor_to_str(ZarrCompressor c)
{
    switch (c) {
        case ZarrCompressor_None:
            return "NONE";
        case ZarrCompressor_Blosc1:
            return "BLOSC1";
        case ZarrCompressor_Zstd:
            return "ZSTD";
        default:
            return "UNKNOWN";
    }
}

const char*
compression_codec_to_str(ZarrCompressionCodec c)
{
    switch (c) {
        case ZarrCompressionCodec_None:
            return "NONE";
        case ZarrCompressionCodec_BloscLZ4:
            return "BLOSC_LZ4";
        case ZarrCompressionCodec_BloscZstd:
            return "BLOSC_ZSTD";
        case ZarrCompressionCodec_Zstd:
            return "ZSTD";
        default:
            return "UNKNOWN";
    }
}

const char*
dimension_type_to_str(ZarrDimensionType t)
{
    switch (t) {
        case ZarrDimensionType_Space:
            return "SPACE";
        case ZarrDimensionType_Channel:
            return "CHANNEL";
        case ZarrDimensionType_Time:
            return "TIME";
        case ZarrDimensionType_Other:
            return "OTHER";
        default:
            return "UNKNOWN";
    }
}

const char*
log_level_to_str(ZarrLogLevel level)
{
    switch (level) {
        case ZarrLogLevel_Debug:
            return "DEBUG";
        case ZarrLogLevel_Info:
            return "INFO";
        case ZarrLogLevel_Warning:
            return "WARNING";
        case ZarrLogLevel_Error:
            return "ERROR";
        case ZarrLogLevel_None:
            return "NONE";
        default:
            return "UNKNOWN";
    }
}

ZarrDataType
numpy_dtype_to_zarr_datatype(const py::dtype& dtype)
{
    if (dtype.is(py::dtype::of<uint8_t>())) {
        return ZarrDataType_uint8;
    } else if (dtype.is(py::dtype::of<uint16_t>())) {
        return ZarrDataType_uint16;
    } else if (dtype.is(py::dtype::of<uint32_t>())) {
        return ZarrDataType_uint32;
    } else if (dtype.is(py::dtype::of<uint64_t>())) {
        return ZarrDataType_uint64;
    } else if (dtype.is(py::dtype::of<int8_t>())) {
        return ZarrDataType_int8;
    } else if (dtype.is(py::dtype::of<int16_t>())) {
        return ZarrDataType_int16;
    } else if (dtype.is(py::dtype::of<int32_t>())) {
        return ZarrDataType_int32;
    } else if (dtype.is(py::dtype::of<int64_t>())) {
        return ZarrDataType_int64;
    } else if (dtype.is(py::dtype::of<float>())) {
        return ZarrDataType_float32;
    } else if (dtype.is(py::dtype::of<double>())) {
        return ZarrDataType_float64;
    } else {
        std::string err = "Unsupported NumPy dtype: " +
                          py::str(py::handle(dtype)).cast<std::string>();
        PyErr_SetString(PyExc_ValueError, err.c_str());
        throw py::error_already_set();
    }
}
} // namespace

class PyZarrS3Settings
{
  public:
    PyZarrS3Settings() = default;
    ~PyZarrS3Settings() = default;

    void set_endpoint(const std::string& endpoint) { endpoint_ = endpoint; }
    const std::string& endpoint() const { return endpoint_; }

    void set_bucket_name(const std::string& bucket) { bucket_name_ = bucket; }
    const std::string& bucket_name() const { return bucket_name_; }

    void set_region(const std::string& region) { region_ = region; }
    const std::optional<std::string>& region() const { return region_; }

    std::string repr() const
    {
        const auto region =
          region_.has_value() ? ("'" + region_.value() + "'") : "None";

        return "S3Settings(endpoint='" + endpoint_ + "', bucket_name='" +
               bucket_name_ + "', region=" + region + ")";
    }

    ZarrS3Settings* settings()
    {
        s3_settings_.endpoint = endpoint_.c_str();
        s3_settings_.bucket_name = bucket_name_.c_str();
        if (region_) {
            s3_settings_.region = region_->c_str();
        } else {
            s3_settings_.region = nullptr;
        }
        return &s3_settings_;
    }

  private:
    std::string endpoint_;
    std::string bucket_name_;
    std::optional<std::string> region_;

    ZarrS3Settings s3_settings_{};
};

class PyZarrCompressionSettings
{
  public:
    PyZarrCompressionSettings() = default;
    ~PyZarrCompressionSettings() = default;

    ZarrCompressor compressor() const { return compressor_; }
    void set_compressor(ZarrCompressor compressor) { compressor_ = compressor; }

    ZarrCompressionCodec codec() const { return codec_; }
    void set_codec(ZarrCompressionCodec codec) { codec_ = codec; }

    uint8_t level() const { return level_; }
    void set_level(uint8_t level) { level_ = level; }

    uint8_t shuffle() const { return shuffle_; }
    void set_shuffle(uint8_t shuffle) { shuffle_ = shuffle; }

    std::string repr() const
    {
        return "CompressionSettings(compressor=Compressor." +
               std::string(compressor_to_str(compressor_)) +
               ", codec=CompressionCodec." +
               std::string(compression_codec_to_str(codec_)) +
               ", level=" + std::to_string(level_) +
               ", shuffle=" + std::to_string(shuffle_) + ")";
    }

  private:
    ZarrCompressor compressor_{ ZarrCompressor_None };
    ZarrCompressionCodec codec_{ ZarrCompressionCodec_None };
    uint8_t level_{ 1 };
    uint8_t shuffle_{ 0 };
};

class PyZarrDimensionProperties
{
  public:
    PyZarrDimensionProperties() = default;
    ~PyZarrDimensionProperties() = default;

    std::string name() const { return name_; }
    void set_name(const std::string& name) { name_ = name; }

    ZarrDimensionType type() const { return type_; }
    void set_type(ZarrDimensionType type) { type_ = type; }

    std::optional<std::string> unit() const { return unit_; }
    void set_unit(const std::optional<std::string>& unit) { unit_ = unit; }

    double scale() const { return scale_; }
    void set_scale(double scale) { scale_ = scale; }

    uint32_t array_size_px() const { return array_size_px_; }
    void set_array_size_px(uint32_t size) { array_size_px_ = size; }

    uint32_t chunk_size_px() const { return chunk_size_px_; }
    void set_chunk_size_px(uint32_t size) { chunk_size_px_ = size; }

    uint32_t shard_size_chunks() const { return shard_size_chunks_; }
    void set_shard_size_chunks(uint32_t size) { shard_size_chunks_ = size; }

    std::string repr() const
    {
        std::string unit = "None";
        if (unit_) {
            unit = "'" + *unit_ + "'";
        }
        return "Dimension(name='" + name_ + "', kind=DimensionType." +
               std::string(dimension_type_to_str(type_)) + ", unit=" + unit +
               ", scale=" + std::to_string(scale_) +
               ", array_size_px=" + std::to_string(array_size_px_) +
               ", chunk_size_px=" + std::to_string(chunk_size_px_) +
               ", shard_size_chunks=" + std::to_string(shard_size_chunks_) +
               ")";
    }

  private:
    std::string name_;
    ZarrDimensionType type_{ ZarrDimensionType_Space };

    std::optional<std::string> unit_;
    double scale_{ 1.0 };

    uint32_t array_size_px_{ 0 };
    uint32_t chunk_size_px_{ 0 };
    uint32_t shard_size_chunks_{ 0 };
};

PYBIND11_MAKE_OPAQUE(std::vector<PyZarrDimensionProperties>);

class PyZarrArraySettings
{
  public:
    PyZarrArraySettings() = default;
    ~PyZarrArraySettings() = default;

    const std::string& output_key() const { return output_key_; }
    void set_output_key(const std::string& key) { output_key_ = key; }

    const std::optional<PyZarrCompressionSettings>& compression() const
    {
        return compression_settings_;
    }

    void set_compression(
      const std::optional<PyZarrCompressionSettings>& settings)
    {
        compression_settings_ = settings;
    }

    // a value for downsampling_method coerces is_ngff to True, but only for
    // as long as it is set: clearing it restores the requested value
    bool is_ngff() const
    {
        return is_ngff_ || downsampling_method_.has_value();
    }

    void set_is_ngff(bool is_ngff) { is_ngff_ = is_ngff; }

    const std::vector<PyZarrDimensionProperties>& dimensions() const
    {
        return dims_;
    }

    std::vector<PyZarrDimensionProperties>& dimensions() { return dims_; }

    void set_dimensions(const std::vector<PyZarrDimensionProperties>& dims)
    {
        dims_ = dims;
    }

    ZarrDataType data_type() const { return data_type_; }
    void set_data_type(ZarrDataType type) { data_type_ = type; }

    std::optional<ZarrDownsamplingMethod> downsampling_method() const
    {
        return downsampling_method_;
    }

    void set_downsampling_method(std::optional<ZarrDownsamplingMethod> method)
    {
        downsampling_method_ = method;
    }

    uint32_t max_levels() const { return max_levels_; }
    void set_max_levels(uint32_t levels) { max_levels_ = levels; }

    const std::vector<std::string>& storage_dimension_order() const
    {
        return storage_dimension_order_;
    }

    void set_storage_dimension_order(const std::vector<std::string>& order)
    {
        // Validate that dimension 0 is not transposed away
        if (!order.empty() && !dims_.empty()) {
            const std::string& first_dim_name = dims_[0].name();
            if (order[0] != first_dim_name) {
                throw py::type_error(
                  "Transposing dimension 0 ('" + first_dim_name +
                  "') away from position 0 is not currently supported. "
                  "The first dimension must remain first in "
                  "storage_dimension_order.");
            }

            // Validate that the last two acquisition dimensions remain in the
            // last two positions (they can swap with each other, but cannot
            // move elsewhere). This is required because frames arrive as 2D
            // arrays with the shape of the last two acquisition dimensions.
            const auto n = dims_.size();
            if (order.size() == n && n >= 2) {
                const auto& last_acq_name = dims_[n - 1].name();
                const auto& second_last_acq_name = dims_[n - 2].name();

                const bool last_two_preserved =
                  (order[n - 1] == last_acq_name ||
                   order[n - 1] == second_last_acq_name) &&
                  (order[n - 2] == last_acq_name ||
                   order[n - 2] == second_last_acq_name);

                if (!last_two_preserved) {
                    throw py::type_error(
                      "The last two dimensions in acquisition order ('" +
                      second_last_acq_name + "', '" + last_acq_name +
                      "') must remain in the last two positions in "
                      "storage_dimension_order. "
                      "They may swap with each other, but cannot be reordered "
                      "with "
                      "other dimensions.");
                }
            }
        }
        storage_dimension_order_ = order;
    }

    ArrayLifetimeProps to_lifetime_props() const
    {
        ArrayLifetimeProps lt_props{};
        lt_props.output_key = output_key_;
        lt_props.data_type = data_type_;
        lt_props.downsampling_method = downsampling_method_;
        lt_props.is_ngff = is_ngff();
        lt_props.max_levels = max_levels_;

        // compression settings
        if (compression_settings_.has_value()) {
            lt_props.compression = {
                .compressor = compression_settings_->compressor(),
                .codec = compression_settings_->codec(),
                .level = compression_settings_->level(),
                .shuffle = compression_settings_->shuffle(),
            };
            lt_props.has_compression = true;
        } else {
            lt_props.compression = {};
            lt_props.has_compression = false;
        }

        const size_t n_dims = dims_.size();
        lt_props.names.resize(n_dims);
        lt_props.units.resize(n_dims);
        lt_props.array_sizes.resize(n_dims);
        lt_props.chunk_sizes.resize(n_dims);
        lt_props.shard_sizes.resize(n_dims);
        lt_props.types.resize(n_dims);
        lt_props.scales.resize(n_dims);

        for (auto i = 0; i < n_dims; ++i) {
            const auto& dim = dims_[i];
            lt_props.names[i] = dim.name();
            lt_props.units[i] = dim.unit().has_value() ? *dim.unit() : "";
            lt_props.array_sizes[i] = dim.array_size_px();
            lt_props.chunk_sizes[i] = dim.chunk_size_px();
            lt_props.shard_sizes[i] = dim.shard_size_chunks();
            lt_props.types[i] = dim.type();
            lt_props.scales[i] = dim.scale();
        }

        // Convert dimension order names to indices
        if (!storage_dimension_order_.empty()) {
            lt_props.storage_dimension_order.reserve(
              storage_dimension_order_.size());
            for (const auto& name : storage_dimension_order_) {
                // Find this name in dims_ and get its index
                bool found = false;
                for (size_t i = 0; i < n_dims; ++i) {
                    if (dims_[i].name() == name) {
                        lt_props.storage_dimension_order.push_back(i);
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw py::value_error(
                      "Dimension name '" + name +
                      "' in storage_dimension_order not found in dimensions");
                }
            }
        }

        return lt_props;
    }

  private:
    std::string output_key_;
    std::optional<PyZarrCompressionSettings> compression_settings_;
    std::vector<PyZarrDimensionProperties> dims_;

    ZarrDataType data_type_{ ZarrDataType_uint8 };
    bool is_ngff_{ false };
    std::optional<ZarrDownsamplingMethod> downsampling_method_{ std::nullopt };
    uint32_t max_levels_{ 0 };

    std::vector<std::string> storage_dimension_order_;
};

PYBIND11_MAKE_OPAQUE(std::vector<PyZarrArraySettings>);

class PyZarrStreamSettings
{
  public:
    PyZarrStreamSettings() = default;
    ~PyZarrStreamSettings() = default;

    const std::string& store_path() const { return store_path_; }
    void set_store_path(const std::string& path) { store_path_ = path; }

    const std::optional<PyZarrS3Settings>& s3() const
    {
        return py_s3_settings_;
    }

    void set_s3(const std::optional<PyZarrS3Settings>& settings)
    {
        py_s3_settings_ = settings;
    }

    unsigned int max_threads() const { return max_threads_; }

    void set_max_threads(unsigned int max_threads)
    {
        max_threads_ = max_threads;
    }

    bool overwrite() const { return overwrite_; }
    void set_overwrite(bool overwrite) { overwrite_ = overwrite; }

    ZarrIntermediateGroups intermediate_groups() const
    {
        return intermediate_groups_;
    }
    void set_intermediate_groups(ZarrIntermediateGroups mode)
    {
        intermediate_groups_ = mode;
    }

    const std::vector<PyZarrArraySettings>& arrays() const { return arrays_; }
    std::vector<PyZarrArraySettings>& arrays() { return arrays_; }

    void set_arrays(const std::vector<PyZarrArraySettings>& arrays)
    {
        arrays_ = arrays;
    }

    ZarrStreamSettings* to_settings() const
    {
        memset(&settings_, 0, sizeof(settings_));

        settings_.store_path = store_path_.c_str();
        settings_.max_threads = max_threads_;
        settings_.overwrite = static_cast<int>(overwrite_);
        settings_.intermediate_groups = intermediate_groups_;

        if (py_s3_settings_) {
            s3_settings_ = *py_s3_settings_->settings();
            settings_.s3_settings = &s3_settings_;
        }

        // construct array lifetime props and set up arrays
        const size_t n_arrays = arrays_.size();

        array_lifetimes_.clear();
        for (size_t i = 0; i < n_arrays; ++i) {
            array_lifetimes_.push_back(arrays_[i].to_lifetime_props());
        }

        array_settings_.clear();
        for (size_t i = 0; i < n_arrays; ++i) {
            array_settings_.emplace_back(*array_lifetimes_[i].array_settings());
        }

        if (n_arrays > 0) {
            settings_.arrays = array_settings_.data();
            settings_.array_count = n_arrays;
        }

        return &settings_;
    }

    size_t get_maximum_memory_usage() const
    {
        const ZarrStreamSettings* settings = to_settings();

        size_t usage;
        if (ZarrStreamSettings_estimate_max_memory_usage(settings, &usage) !=
            ZarrStatusCode_Success) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Failed to estimate maximum memory usage.");
            throw py::error_already_set();
        }

        return usage;
    }

    std::vector<std::string> get_array_keys() const
    {
        auto* settings = to_settings();
        size_t n_keys = ZarrStreamSettings_get_array_count(settings);

        if (n_keys == 0) {
            return {};
        }

        std::vector<std::string> keys(n_keys);
        char* c_key = nullptr;

        for (size_t i = 0; i < n_keys; ++i) {
            if (auto code =
                  ZarrStreamSettings_get_array_key(settings, i, &c_key);
                code != ZarrStatusCode_Success) {
                const std::string err =
                  "Failed to get array key: " +
                  std::string(Zarr_get_status_message(code));
                PyErr_SetString(PyExc_RuntimeError, err.c_str());
                throw py::error_already_set();
            }
            keys[i] = c_key;
            free(c_key);
        }

        return keys;
    }

  private:
    std::string store_path_;
    mutable std::optional<PyZarrS3Settings> py_s3_settings_{ std::nullopt };
    unsigned int max_threads_{ 0 };
    bool overwrite_{ false };
    ZarrIntermediateGroups intermediate_groups_{
        ZarrIntermediateGroups_IfMissing
    };

    std::vector<PyZarrArraySettings> arrays_;

    mutable ZarrS3Settings s3_settings_{};

    mutable std::vector<ArrayLifetimeProps> array_lifetimes_;

    mutable std::vector<ZarrArraySettings> array_settings_;

    mutable ZarrStreamSettings settings_{};
};

// ---------------------------------------------------------------------------
// Config-file support: read a loader-owned ZarrStreamSettings into Python
// objects (the dump direction reuses to_settings() + the C dump API).
// ---------------------------------------------------------------------------
namespace {
PyZarrDimensionProperties
read_dimension(const ZarrDimensionProperties& d)
{
    PyZarrDimensionProperties dim;
    dim.set_name(d.name ? d.name : "");
    dim.set_type(d.type);
    dim.set_array_size_px(d.array_size_px);
    dim.set_chunk_size_px(d.chunk_size_px);
    dim.set_shard_size_chunks(d.shard_size_chunks);
    if (d.unit) {
        dim.set_unit(std::string(d.unit));
    }
    dim.set_scale(d.scale);
    return dim;
}

PyZarrArraySettings
read_array(const ZarrArraySettings& a)
{
    PyZarrArraySettings arr;
    if (a.output_key) {
        arr.set_output_key(a.output_key);
    }
    arr.set_data_type(a.data_type);
    if (a.downsampling_method > ZarrDownsamplingMethod_None) {
        arr.set_downsampling_method(a.downsampling_method);
        arr.set_max_levels(a.max_levels);
    }
    arr.set_is_ngff(a.is_ngff);
    if (a.compression_settings) {
        PyZarrCompressionSettings c;
        c.set_compressor(a.compression_settings->compressor);
        c.set_codec(a.compression_settings->codec);
        c.set_level(a.compression_settings->level);
        c.set_shuffle(a.compression_settings->shuffle);
        arr.set_compression(c);
    }

    std::vector<PyZarrDimensionProperties> dims;
    dims.reserve(a.dimension_count);
    for (size_t i = 0; i < a.dimension_count; ++i) {
        dims.push_back(read_dimension(a.dimensions[i]));
    }
    arr.set_dimensions(dims);

    // storage order is index-based in the config/C struct, name-based in Python
    if (a.storage_dimension_order) {
        std::vector<std::string> order;
        order.reserve(a.dimension_count);
        for (size_t i = 0; i < a.dimension_count; ++i) {
            const size_t idx = a.storage_dimension_order[i];
            order.push_back(idx < dims.size() ? dims[idx].name() : std::string());
        }
        arr.set_storage_dimension_order(order);
    }
    return arr;
}

PyZarrStreamSettings
read_stream_settings(const ZarrStreamSettings& s)
{
    PyZarrStreamSettings out;
    out.set_store_path(s.store_path ? s.store_path : "");
    out.set_max_threads(s.max_threads);
    out.set_overwrite(s.overwrite);
    out.set_intermediate_groups(s.intermediate_groups);

    if (s.s3_settings) {
        PyZarrS3Settings s3;
        s3.set_endpoint(s.s3_settings->endpoint ? s.s3_settings->endpoint : "");
        s3.set_bucket_name(
          s.s3_settings->bucket_name ? s.s3_settings->bucket_name : "");
        if (s.s3_settings->region) {
            s3.set_region(s.s3_settings->region);
        }
        out.set_s3(s3);
    }

    if (s.arrays) {
        std::vector<PyZarrArraySettings> arrays;
        arrays.reserve(s.array_count);
        for (size_t i = 0; i < s.array_count; ++i) {
            arrays.push_back(read_array(s.arrays[i]));
        }
        out.set_arrays(arrays);
    }

    return out;
}

PyZarrStreamSettings
finish_load(ZarrStreamSettings& c, ZarrStatusCode status, const char* what)
{
    if (status != ZarrStatusCode_Success) {
        ZarrStreamSettings_destroy_loaded(&c);
        throw py::value_error(std::string("Failed to load stream settings from ") +
                              what + " (see log for details)");
    }
    PyZarrStreamSettings out = read_stream_settings(c);
    ZarrStreamSettings_destroy_loaded(&c);
    return out;
}

std::string
dump_settings(const PyZarrStreamSettings& self, ZarrConfigFormat format)
{
    char* text = nullptr;
    if (ZarrStreamSettings_dump_to_string(self.to_settings(), &text, format) !=
        ZarrStatusCode_Success) {
        throw py::value_error("Failed to serialize stream settings");
    }
    std::string out(text);
    free(text);
    return out;
}
} // namespace

class PyZarrStream
{
  public:
    explicit PyZarrStream(const PyZarrStreamSettings& settings)
    {
        open_(settings);
    }

    void append(py::array image_data, const std::optional<std::string>& key)
    {
        if (!is_active()) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Stream not open for appending.");
            throw py::error_already_set();
        }
        // if the array is already contiguous, we can just write it out
        if (image_data.flags() & py::array::c_style) {
            write_contiguous_data(image_data, key);
            return;
        }

        // just make a copy of smaller (2-dim or less) arrays
        if (image_data.ndim() <= 2) {
            py::module np = py::module::import("numpy");
            py::array contiguous_data =
              np.attr("ascontiguousarray")(image_data);
            write_contiguous_data(contiguous_data, key);
            return;
        }

        // iterate through frames
        iterate_and_append(image_data, 0, std::vector<py::ssize_t>(), key);
    }

    void skip(size_t bytes_in, const std::optional<std::string>& key) const
    {
        size_t bytes_out;
        const char* key_str = key.has_value() ? key->c_str() : nullptr;
        const auto status = ZarrStream_append(
          stream_.get(), nullptr, bytes_in, &bytes_out, key_str);
        if (status != ZarrStatusCode_Success || bytes_in != bytes_out) {
            const std::string err =
              "Failed to skip: " + std::string(Zarr_get_status_message(status));
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
    }

    // iterate over the indices of the array until we get down to 2 dimensions,
    // then write the frame
    void iterate_and_append(const py::array& array,
                            size_t dim,
                            std::vector<py::ssize_t> indices,
                            std::optional<std::string> key)
    {
        if (dim == array.ndim() - 2) {
            // we are down to a 2D frame - we can write it
            py::array frame = extract_frame(array, indices);
            write_contiguous_data(frame, key);
        } else {
            // construct indices for this dimension
            for (py::ssize_t i = 0; i < array.shape()[dim]; ++i) {
                indices.push_back(i);
                iterate_and_append(array, dim + 1, indices, key);
                indices.pop_back();
            }
        }
    }

    // extract a 2D frame given the indices for all but the last 2 dimensions
    py::array extract_frame(const py::array& array,
                            const std::vector<py::ssize_t>& indices)
    {
        // Use Python's slicing to extract the frame
        py::tuple args(array.ndim());

        // fill the tuple with the indices for higher dimensions...
        for (size_t i = 0; i < indices.size(); ++i) {
            args[i] = py::int_(indices[i]);
        }

        // ... and slices for the last two
        py::module builtins = py::module::import("builtins");
        py::object slice_fn = builtins.attr("slice");
        py::object none = py::none(); // equivalent to : in Python

        args[array.ndim() - 2] = slice_fn(none, none, none);
        args[array.ndim() - 1] = slice_fn(none, none, none);

        // here's the frame
        py::object frame = array.attr("__getitem__")(args);
        return frame.cast<py::array>();
    }

    void write_contiguous_data(py::array frame,
                               const std::optional<std::string>& key) const
    {
        // double-check the frame is C-contiguous
        py::array contiguous_data;
        if (!(frame.flags() & py::array::c_style)) {
            py::module np = py::module::import("numpy");
            contiguous_data = np.attr("ascontiguousarray")(frame);
        } else {
            contiguous_data = frame;
        }

        const auto buf = contiguous_data.request();
        const auto* ptr = static_cast<uint8_t*>(buf.ptr);

        py::gil_scoped_release release;

        const char* key_str = key.has_value() ? key->c_str() : nullptr;
        const size_t bytes_in = buf.itemsize * buf.size;

        size_t bytes_out;
        const auto status =
          ZarrStream_append(stream_.get(), ptr, bytes_in, &bytes_out, key_str);

        py::gil_scoped_acquire acquire;

        const std::string array_key =
          key_str ? "'" + std::string(key_str) + "'" : "(NULL)";
        std::string err;
        switch (status) {
            case ZarrStatusCode_Success:
                if (bytes_out != bytes_in) {
                    err = "Expected to write " + std::to_string(bytes_in) +
                          " bytes to array " + array_key + ", wrote " +
                          std::to_string(bytes_out) + ".";
                }
                break;
            case ZarrStatusCode_KeyNotFound:
                err = "Array key " + array_key + " not found";
                break;
            case ZarrStatusCode_WriteOutOfBounds:
                err = "Attempted out of bounds write to array " + array_key;
                break;
            case ZarrStatusCode_PartialWrite:
                err = "Partial write to array " + array_key + ": wrote " +
                      std::to_string(bytes_out) +
                      " bytes of contiguous region of " +
                      std::to_string(bytes_in) + " bytes";
                break;
            default:
                err = "Failed to append data to Zarr stream: " +
                      std::string(Zarr_get_status_message(status));
        }

        if (!err.empty()) {
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
    }

    bool write_custom_metadata(py::object metadata,
                               const std::optional<std::string>& node_key,
                               const std::optional<std::string>& metadata_key)
    {
        if (!is_active()) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Cannot write metadata unless streaming.");
            throw py::error_already_set();
        }

        std::string metadata_str;
        if (py::isinstance<py::dict>(metadata)) {
            py::module_ json = py::module_::import("json");
            metadata_str = json.attr("dumps")(metadata).cast<std::string>();
        } else if (py::isinstance<py::str>(metadata)) {
            metadata_str = metadata.cast<std::string>();
        } else {
            PyErr_SetString(PyExc_TypeError, "metadata must be a str or dict");
            throw py::error_already_set();
        }

        const char* node_cstr =
          node_key.has_value() ? node_key->c_str() : nullptr;
        const char* meta_cstr =
          metadata_key.has_value() ? metadata_key->c_str() : nullptr;

        auto status = ZarrStream_write_custom_metadata(
          stream_.get(), node_cstr, meta_cstr, metadata_str.c_str());

        if (status != ZarrStatusCode_Success) {
            std::string err = "Failed to write custom metadata: " +
                              std::string(Zarr_get_status_message(status));
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }

        return true;
    }

    bool is_active() const { return static_cast<bool>(stream_); }

    void close()
    {
        if (!is_active()) {
            return;
        }

        ZarrStatusCode status = ZarrStatusCode_Success;
        try {
            // release ownership so the deleter does not also destroy the
            // stream; ZarrStream_close finalizes, frees, and reports a failed
            // flush (e.g. an I/O error on a network filesystem). It blocks on
            // the queue drain + fsyncs, so drop the GIL while it runs.
            ZarrStream* raw = stream_.release();
            {
                py::gil_scoped_release release;
                status = ZarrStream_close(raw);
            }
        } catch (const std::exception& exc) {
            std::string err =
              "Failed to close Zarr stream: " + std::string(exc.what());
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }

        if (status != ZarrStatusCode_Success) {
            std::string err = "Failed to close Zarr stream: " +
                              std::string(Zarr_get_status_message(status));
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
    }

    size_t get_current_memory_usage() const
    {
        if (!is_active()) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Stream not open for memory usage query.");
            throw py::error_already_set();
        }

        size_t usage = 0;
        auto status =
          ZarrStream_get_current_memory_usage(stream_.get(), &usage);

        if (status != ZarrStatusCode_Success) {
            std::string err = "Failed to get current memory usage: " +
                              std::string(Zarr_get_status_message(status));
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }

        return usage;
    }

  private:
    using ZarrStreamPtr =
      std::unique_ptr<ZarrStream, decltype(ZarrStreamDeleter)>;

    ZarrStreamPtr stream_;

    std::string store_path_;
    std::string s3_endpoint_;
    std::string s3_bucket_name_;
    std::string s3_region_;

    // TODO (aliddell): we can make this public to allow reopening the stream
    // once we have support for that in the C API
    void open_(const PyZarrStreamSettings& settings)
    {
        if (is_active()) {
            return;
        }

        auto* stream_settings = settings.to_settings();
        stream_ =
          ZarrStreamPtr(ZarrStream_create(stream_settings), ZarrStreamDeleter);
        if (!stream_) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to create Zarr stream");
            throw py::error_already_set();
        }
    }
};

PYBIND11_MODULE(acquire_zarr, m)
{
    py::options options;
    options.disable_user_defined_docstrings();
    options.disable_function_signatures();

    using namespace pybind11::literals;

    m.doc() = R"pbdoc(
        Acquire Zarr Writer Python API
        -----------------------
        .. currentmodule:: acquire_zarr
        .. autosummary::
           :toctree: _generate
           append
    )pbdoc";

    py::bind_vector<std::vector<PyZarrDimensionProperties>>(m,
                                                            "VectorDimension");
    py::bind_vector<std::vector<PyZarrArraySettings>>(m, "VectorArraySettings");

    py::enum_<ZarrVersion>(m, "ZarrVersion").value("V3", ZarrVersion_3);

    py::enum_<ZarrDataType>(m, "DataType")
      .value(data_type_to_str(ZarrDataType_uint8), ZarrDataType_uint8)
      .value(data_type_to_str(ZarrDataType_uint16), ZarrDataType_uint16)
      .value(data_type_to_str(ZarrDataType_uint32), ZarrDataType_uint32)
      .value(data_type_to_str(ZarrDataType_uint64), ZarrDataType_uint64)
      .value(data_type_to_str(ZarrDataType_int8), ZarrDataType_int8)
      .value(data_type_to_str(ZarrDataType_int16), ZarrDataType_int16)
      .value(data_type_to_str(ZarrDataType_int32), ZarrDataType_int32)
      .value(data_type_to_str(ZarrDataType_int64), ZarrDataType_int64)
      .value(data_type_to_str(ZarrDataType_float32), ZarrDataType_float32)
      .value(data_type_to_str(ZarrDataType_float64), ZarrDataType_float64);

    py::enum_<ZarrCompressor>(m, "Compressor")
      .value(compressor_to_str(ZarrCompressor_None), ZarrCompressor_None)
      .value(compressor_to_str(ZarrCompressor_Blosc1), ZarrCompressor_Blosc1)
      .value(compressor_to_str(ZarrCompressor_Zstd), ZarrCompressor_Zstd);

    py::enum_<ZarrCompressionCodec>(m, "CompressionCodec")
      .value(compression_codec_to_str(ZarrCompressionCodec_None),
             ZarrCompressionCodec_None)
      .value(compression_codec_to_str(ZarrCompressionCodec_BloscLZ4),
             ZarrCompressionCodec_BloscLZ4)
      .value(compression_codec_to_str(ZarrCompressionCodec_BloscZstd),
             ZarrCompressionCodec_BloscZstd)
      .value(compression_codec_to_str(ZarrCompressionCodec_Zstd),
             ZarrCompressionCodec_Zstd);

    py::enum_<ZarrDimensionType>(m, "DimensionType")
      .value(dimension_type_to_str(ZarrDimensionType_Space),
             ZarrDimensionType_Space)
      .value(dimension_type_to_str(ZarrDimensionType_Channel),
             ZarrDimensionType_Channel)
      .value(dimension_type_to_str(ZarrDimensionType_Time),
             ZarrDimensionType_Time)
      .value(dimension_type_to_str(ZarrDimensionType_Other),
             ZarrDimensionType_Other);

    py::enum_<ZarrDownsamplingMethod>(m, "DownsamplingMethod")
      .value("DECIMATE", ZarrDownsamplingMethod_Decimate)
      .value("MEAN", ZarrDownsamplingMethod_Mean)
      .value("MIN", ZarrDownsamplingMethod_Min)
      .value("MAX", ZarrDownsamplingMethod_Max);

    py::enum_<ZarrIntermediateGroups>(m, "IntermediateGroups")
      .value("IF_MISSING", ZarrIntermediateGroups_IfMissing)
      .value("ALWAYS", ZarrIntermediateGroups_Always)
      .value("NEVER", ZarrIntermediateGroups_Never);

    py::enum_<ZarrLogLevel>(m, "LogLevel")
      .value(log_level_to_str(ZarrLogLevel_Debug), ZarrLogLevel_Debug)
      .value(log_level_to_str(ZarrLogLevel_Info), ZarrLogLevel_Info)
      .value(log_level_to_str(ZarrLogLevel_Warning), ZarrLogLevel_Warning)
      .value(log_level_to_str(ZarrLogLevel_Error), ZarrLogLevel_Error)
      .value(log_level_to_str(ZarrLogLevel_None), ZarrLogLevel_None);

    py::class_<PyZarrS3Settings>(m, "S3Settings", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> endpoint,
                       std::optional<std::string> bucket_name,
                       std::optional<std::string> region) {
               PyZarrS3Settings settings;

               if (endpoint) {
                   settings.set_endpoint(*endpoint);
               }
               if (bucket_name) {
                   settings.set_bucket_name(*bucket_name);
               }
               if (region) {
                   settings.set_region(*region);
               }

               return settings;
           }),
           py::kw_only(),
           py::arg("endpoint") = std::nullopt,
           py::arg("bucket_name") = std::nullopt,
           py::arg("region") = std::nullopt)
      .def("__repr__", [](const PyZarrS3Settings& self) { return self.repr(); })
      .def_property("endpoint",
                    &PyZarrS3Settings::endpoint,
                    &PyZarrS3Settings::set_endpoint)
      .def_property("bucket_name",
                    &PyZarrS3Settings::bucket_name,
                    &PyZarrS3Settings::set_bucket_name)
      .def_property(
        "region", &PyZarrS3Settings::region, &PyZarrS3Settings::set_region);

    py::class_<PyZarrCompressionSettings>(
      m, "CompressionSettings", py::dynamic_attr())
      .def(py::init([](std::optional<ZarrCompressor> compressor,
                       std::optional<ZarrCompressionCodec> codec,
                       std::optional<int> level,
                       std::optional<int> shuffle) {
               PyZarrCompressionSettings settings;

               if (compressor) {
                   settings.set_compressor(*compressor);
               }
               if (codec) {
                   settings.set_codec(*codec);
               }
               if (level) {
                   settings.set_level(*level);
               }
               if (shuffle) {
                   settings.set_shuffle(*shuffle);
               }
               return settings;
           }),
           py::kw_only(),
           py::arg("compressor") = std::nullopt,
           py::arg("codec") = std::nullopt,
           py::arg("level") = std::nullopt,
           py::arg("shuffle") = std::nullopt)
      .def("__repr__",
           [](const PyZarrCompressionSettings& self) { return self.repr(); })
      .def_property("compressor",
                    &PyZarrCompressionSettings::compressor,
                    &PyZarrCompressionSettings::set_compressor)
      .def_property("codec",
                    &PyZarrCompressionSettings::codec,
                    &PyZarrCompressionSettings::set_codec)
      .def_property("level",
                    &PyZarrCompressionSettings::level,
                    &PyZarrCompressionSettings::set_level)
      .def_property("shuffle",
                    &PyZarrCompressionSettings::shuffle,
                    &PyZarrCompressionSettings::set_shuffle);

    py::class_<PyZarrDimensionProperties>(m, "Dimension", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> name,
                       std::optional<ZarrDimensionType> kind,
                       std::optional<std::string> unit,
                       std::optional<double> scale,
                       std::optional<uint32_t> array_size_px,
                       std::optional<uint32_t> chunk_size_px,
                       std::optional<uint32_t> shard_size_chunks) {
               PyZarrDimensionProperties props;

               if (name) {
                   props.set_name(*name);
               }
               if (kind) {
                   props.set_type(*kind);
               }
               if (unit) {
                   props.set_unit(*unit);
               }
               if (scale) {
                   props.set_scale(*scale);
               }
               if (array_size_px) {
                   props.set_array_size_px(*array_size_px);
               }
               if (chunk_size_px) {
                   props.set_chunk_size_px(*chunk_size_px);
               }
               if (shard_size_chunks) {
                   props.set_shard_size_chunks(*shard_size_chunks);
               }

               return props;
           }),
           py::kw_only(),
           py::arg("name") = std::nullopt,
           py::arg("kind") = std::nullopt,
           py::arg("unit") = std::nullopt,
           py::arg("scale") = std::nullopt,
           py::arg("array_size_px") = std::nullopt,
           py::arg("chunk_size_px") = std::nullopt,
           py::arg("shard_size_chunks") = std::nullopt)
      .def("__repr__",
           [](const PyZarrDimensionProperties& self) { return self.repr(); })
      .def_property("name",
                    &PyZarrDimensionProperties::name,
                    &PyZarrDimensionProperties::set_name)
      .def_property("kind",
                    &PyZarrDimensionProperties::type,
                    &PyZarrDimensionProperties::set_type)
      .def_property("unit",
                    &PyZarrDimensionProperties::unit,
                    &PyZarrDimensionProperties::set_unit)
      .def_property("scale",
                    &PyZarrDimensionProperties::scale,
                    &PyZarrDimensionProperties::set_scale)
      .def_property("array_size_px",
                    &PyZarrDimensionProperties::array_size_px,
                    &PyZarrDimensionProperties::set_array_size_px)
      .def_property("chunk_size_px",
                    &PyZarrDimensionProperties::chunk_size_px,
                    &PyZarrDimensionProperties::set_chunk_size_px)
      .def_property("shard_size_chunks",
                    &PyZarrDimensionProperties::shard_size_chunks,
                    &PyZarrDimensionProperties::set_shard_size_chunks);

    py::class_<PyZarrArraySettings>(m, "ArraySettings", py::dynamic_attr())
      .def(
        py::init([](std::optional<std::string> output_key,
                    std::optional<PyZarrCompressionSettings> compression,
                    std::optional<py::list> dimensions,
                    std::optional<py::object> data_type,
                    std::optional<ZarrDownsamplingMethod> downsampling_method,
                    uint32_t max_levels,
                    std::optional<py::list> storage_dimension_order,
                    std::optional<bool> is_ngff) {
            PyZarrArraySettings settings;

            if (output_key) {
                settings.set_output_key(*output_key);
            }
            if (compression) {
                settings.set_compression(*compression);
            }
            if (dimensions) {
                auto& dims = *dimensions;
                std::vector<PyZarrDimensionProperties> dims_vec(dims.size());

                for (auto i = 0; i < dims.size(); ++i) {
                    dims_vec[i] = dims[i].cast<PyZarrDimensionProperties>();
                }
                settings.set_dimensions(dims_vec);
            }
            if (data_type) {
                if (py::isinstance<py::dtype>(*data_type)) {
                    auto dtype = data_type->cast<py::dtype>();
                    settings.set_data_type(numpy_dtype_to_zarr_datatype(dtype));
                } else {
                    // try to convert to dtype first
                    try {
                        py::module np = py::module::import("numpy");
                        py::dtype dtype = np.attr("dtype")((*data_type));
                        settings.set_data_type(
                          numpy_dtype_to_zarr_datatype(dtype));
                    } catch (const std::exception& exc) {
                        // fall back to assuming it's a ZarrDataType
                        settings.set_data_type(data_type->cast<ZarrDataType>());
                    }
                }
            }
            if (downsampling_method) {
                settings.set_downsampling_method(*downsampling_method);
            }
            settings.set_max_levels(max_levels);
            if (is_ngff) {
                settings.set_is_ngff(*is_ngff);
            }
            if (storage_dimension_order) {
                auto& order_list = *storage_dimension_order;
                std::vector<std::string> order_vec(order_list.size());
                for (auto i = 0; i < order_list.size(); ++i) {
                    order_vec[i] = order_list[i].cast<std::string>();
                }
                settings.set_storage_dimension_order(order_vec);
            }

            return settings;
        }),
        py::kw_only(),
        py::arg("output_key") = std::nullopt,
        py::arg("compression") = std::nullopt,
        py::arg("dimensions") = std::nullopt,
        py::arg("data_type") = std::nullopt,
        py::arg("downsampling_method") = std::nullopt,
        py::arg("max_levels") = 0,
        py::arg("storage_dimension_order") = std::nullopt,
        py::arg("is_ngff") = std::nullopt)
      .def("__repr__",
           [](const PyZarrArraySettings& self) {
               std::string repr =
                 "ArraySettings(output_key='" + self.output_key() + "'";

               if (self.compression().has_value()) {
                   repr += ", compression=" + self.compression()->repr();
               }
               repr += ", dimensions=[";
               for (const auto& dim : self.dimensions()) {
                   repr += dim.repr() + ", ";
               }
               repr += "], data_type=DataType." +
                       std::string(data_type_to_str(self.data_type()));

               if (self.downsampling_method()) {
                   const auto method = *self.downsampling_method();
                   std::string method_str;
                   switch (method) {
                       case ZarrDownsamplingMethod_Decimate:
                           method_str = "DownsamplingMethod.DECIMATE";
                           break;
                       case ZarrDownsamplingMethod_Mean:
                           method_str = "DownsamplingMethod.MEAN";
                           break;
                       case ZarrDownsamplingMethod_Min:
                           method_str = "DownsamplingMethod.MIN";
                           break;
                       case ZarrDownsamplingMethod_Max:
                           method_str = "DownsamplingMethod.MAX";
                           break;
                       case ZarrDownsamplingMethod_None:
                       default:
                           method_str = "None";
                   }
                   repr += ", downsampling_method=" + method_str;
               }

               repr += std::string(", is_ngff=") +
                       (self.is_ngff() ? "True" : "False");

               repr += ")";
               return repr;
           })
      .def_property("output_key",
                    &PyZarrArraySettings::output_key,
                    &PyZarrArraySettings::set_output_key)
      .def_property(
        "compression",
        [](const PyZarrArraySettings& self) -> py::object {
            if (self.compression()) {
                return py::cast(*self.compression());
            }
            return py::none();
        },
        [](PyZarrArraySettings& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_compression(std::nullopt);
            } else {
                self.set_compression(obj.cast<PyZarrCompressionSettings>());
            }
        })
      .def_property(
        "dimensions",
        [](PyZarrArraySettings& self) -> py::object {
            return py::cast(self.dimensions(),
                            py::return_value_policy::reference);
        },
        [](PyZarrArraySettings& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<PyZarrDimensionProperties> dims;
                for (auto item : obj.cast<py::list>()) {
                    dims.push_back(item.cast<PyZarrDimensionProperties>());
                }
                self.set_dimensions(dims);
            } else {
                // raise a TypeError if not a list
                PyErr_SetString(PyExc_TypeError,
                                "Expected a list of DimensionProperties.");
                throw py::error_already_set();
            }
        })
      .def_property(
        "data_type",
        &PyZarrArraySettings::data_type,
        [](PyZarrArraySettings& self, py::object& obj) {
            if (py::isinstance<py::dtype>(obj)) {
                auto dtype = obj.cast<py::dtype>();
                self.set_data_type(numpy_dtype_to_zarr_datatype(dtype));
            } else {
                // try to create a dtype from the NumPy type class
                try {
                    py::module np = py::module::import("numpy");
                    py::dtype dtype = np.attr("dtype")(obj);
                    self.set_data_type(numpy_dtype_to_zarr_datatype(dtype));
                } catch (...) {
                    // cast to ZarrDataType
                    self.set_data_type(obj.cast<ZarrDataType>());
                }
            }
        })
      .def_property(
        "downsampling_method",
        [](const PyZarrArraySettings& self) -> py::object {
            if (self.downsampling_method()) {
                return py::cast(*self.downsampling_method());
            }
            return py::none();
        },
        [](PyZarrArraySettings& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_downsampling_method(std::nullopt);
            } else {
                self.set_downsampling_method(
                  obj.cast<ZarrDownsamplingMethod>());
            }
        })
      .def_property("max_levels",
                    &PyZarrArraySettings::max_levels,
                    &PyZarrArraySettings::set_max_levels)
      .def_property("storage_dimension_order",
                    &PyZarrArraySettings::storage_dimension_order,
                    &PyZarrArraySettings::set_storage_dimension_order)
      .def_property("is_ngff",
                    &PyZarrArraySettings::is_ngff,
                    &PyZarrArraySettings::set_is_ngff);

    py::class_<PyZarrStreamSettings>(m, "StreamSettings", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> store_path,
                       std::optional<PyZarrS3Settings> s3,
                       std::optional<ZarrVersion> version,
                       std::optional<unsigned> max_threads,
                       std::optional<bool> overwrite,
                       std::optional<py::list> arrays,
                       std::optional<ZarrIntermediateGroups> groups) {
               PyZarrStreamSettings settings;
               if (store_path) {
                   settings.set_store_path(*store_path);
               }
               if (s3) {
                   settings.set_s3(*s3);
               }
               if (version) {
                   if (*version != ZarrVersion_3) {
                       PyErr_SetString(PyExc_ValueError,
                                       "Only ZarrVersion.V3 is supported.");
                       throw py::error_already_set();
                   }
               }
               if (max_threads) {
                   settings.set_max_threads(*max_threads);
               }
               if (overwrite) {
                   settings.set_overwrite(*overwrite);
               }
               if (groups) {
                   settings.set_intermediate_groups(*groups);
               }
               if (arrays) {
                   auto& arrs = *arrays;
                   std::vector<PyZarrArraySettings> arrs_vec(arrs.size());

                   for (auto i = 0; i < arrs.size(); ++i) {
                       arrs_vec[i] = arrs[i].cast<PyZarrArraySettings>();
                   }
                   settings.set_arrays(arrs_vec);
               }

               return settings;
           }),
           py::kw_only(),
           py::arg("store_path") = std::nullopt,
           py::arg("s3") = std::nullopt,
           py::arg("version") = std::nullopt,
           py::arg("max_threads") = std::nullopt,
           py::arg("overwrite") = std::nullopt,
           py::arg("arrays") = std::nullopt,
           py::arg("intermediate_groups") = std::nullopt)
      .def("__repr__",
           [](const PyZarrStreamSettings& self) {
               std::string repr =
                 "StreamSettings(store_path='" + self.store_path() + "'";

               if (self.s3().has_value()) {
                   repr += ", s3=" + self.s3()->repr();
               }
               repr +=
                 ", max_threads=" + std::to_string(self.max_threads()) + "," +
                 (self.overwrite() ? " overwrite=True" : " overwrite=False") +
                 ")";
               return repr;
           })
      .def("get_maximum_memory_usage",
           &PyZarrStreamSettings::get_maximum_memory_usage,
           "Estimate the maximum memory usage for the stream settings.")
      .def_property("store_path",
                    &PyZarrStreamSettings::store_path,
                    &PyZarrStreamSettings::set_store_path)
      .def_property(
        "s3",
        [](const PyZarrStreamSettings& self) -> py::object {
            if (self.s3()) {
                return py::cast(*self.s3());
            }
            return py::none();
        },
        [](PyZarrStreamSettings& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_s3(std::nullopt);
            } else {
                self.set_s3(obj.cast<PyZarrS3Settings>());
            }
        })
      .def_property(
        "version",
        [](const PyZarrStreamSettings& self) { return ZarrVersion_3; },
        [](PyZarrStreamSettings&, const py::object& version) {
            if (!version.is_none()) {
                if (const auto ver = version.cast<ZarrVersion>();
                    ver != ZarrVersion_3) {
                    PyErr_SetString(PyExc_ValueError,
                                    "Only ZarrVersion.V3 is supported.");
                    throw py::error_already_set();
                }
            }
        })
      .def_property("max_threads",
                    &PyZarrStreamSettings::max_threads,
                    &PyZarrStreamSettings::set_max_threads)
      .def_property("overwrite",
                    &PyZarrStreamSettings::overwrite,
                    &PyZarrStreamSettings::set_overwrite)
      .def_property("intermediate_groups",
                    &PyZarrStreamSettings::intermediate_groups,
                    &PyZarrStreamSettings::set_intermediate_groups)
      .def_property(
        "arrays",
        [](PyZarrStreamSettings& self) -> py::object {
            return py::cast(self.arrays(), py::return_value_policy::reference);
        },
        [](PyZarrStreamSettings& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<PyZarrArraySettings> arrs;
                for (auto item : obj.cast<py::list>()) {
                    arrs.push_back(item.cast<PyZarrArraySettings>());
                }
                self.set_arrays(arrs);
            } else {
                // raise a TypeError if not a list
                PyErr_SetString(PyExc_TypeError,
                                "Expected a list of ArraySettings.");
                throw py::error_already_set();
            }
        })
      .def(
        "get_array_keys",
        [](PyZarrStreamSettings& self) -> py::list {
            auto keys = self.get_array_keys();
            py::list key_list;
            for (const auto& key : keys) {
                key_list.append(key);
            }
            return key_list;
        },
        "Get a list of all array output keys defined in the stream settings.")
      .def_static(
        "from_file",
        [](const std::string& path) {
            ZarrStreamSettings c{};
            auto status = ZarrStreamSettings_load_from_file(&c, path.c_str());
            return finish_load(c, status, "config file");
        },
        py::arg("path"),
        "Load stream settings from a YAML or JSON config file.")
      .def_static(
        "from_string",
        [](const std::string& text) {
            ZarrStreamSettings c{};
            auto status = ZarrStreamSettings_load_from_string(&c, text.c_str());
            return finish_load(c, status, "config string");
        },
        py::arg("text"),
        "Load stream settings from a YAML or JSON config string.")
      .def(
        "to_file",
        [](const PyZarrStreamSettings& self, const std::string& path) {
            if (ZarrStreamSettings_dump_to_file(self.to_settings(),
                                                path.c_str()) !=
                ZarrStatusCode_Success) {
                throw py::value_error(
                  "Failed to write stream settings to " + path);
            }
        },
        py::arg("path"),
        "Dump stream settings to a config file (format chosen by extension: "
        "`.json` -> JSON, otherwise YAML).")
      .def(
        "to_yaml",
        [](const PyZarrStreamSettings& self) {
            return dump_settings(self, ZarrConfigFormat_Yaml);
        },
        "Serialize stream settings to a YAML string.")
      .def(
        "to_json",
        [](const PyZarrStreamSettings& self) {
            return dump_settings(self, ZarrConfigFormat_Json);
        },
        "Serialize stream settings to a JSON string.")
      .def_static(
        "from_dict",
        [](const py::object& data) {
            const std::string text =
              py::module::import("json").attr("dumps")(data).cast<std::string>();
            ZarrStreamSettings c{};
            auto status = ZarrStreamSettings_load_from_string(&c, text.c_str());
            return finish_load(c, status, "config dict");
        },
        py::arg("data"),
        "Build stream settings from a config dict (same schema as the files).")
      .def(
        "to_dict",
        [](const PyZarrStreamSettings& self) {
            const std::string text = dump_settings(self, ZarrConfigFormat_Json);
            return py::module::import("json").attr("loads")(text);
        },
        "Serialize stream settings to a config dict.");

    py::class_<PyZarrStream>(m, "ZarrStream")
      .def(py::init<PyZarrStreamSettings>())
      .def("close", &PyZarrStream::close)
      .def("append",
           &PyZarrStream::append,
           py::arg("data"),
           py::arg("key") = std::nullopt)
      .def("skip",
           &PyZarrStream::skip,
           py::arg("n_bytes"),
           py::arg("key") = std::nullopt)
      .def("write_custom_metadata",
           &PyZarrStream::write_custom_metadata,
           py::arg("metadata"),
           py::arg("node_key") = std::nullopt,
           py::arg("metadata_key") = std::nullopt)
      .def("is_active", &PyZarrStream::is_active)
      .def("get_current_memory_usage",
           &PyZarrStream::get_current_memory_usage,
           "Get the current memory usage of the stream in bytes.");

    m.def(
      "set_log_level",
      [](ZarrLogLevel level) {
          auto status = Zarr_set_log_level(level);
          if (status != ZarrStatusCode_Success) {
              std::string err = "Failed to set log level: " +
                                std::string(Zarr_get_status_message(status));
              PyErr_SetString(PyExc_RuntimeError, err.c_str());
              throw py::error_already_set();
          }
      },
      "Set the log level for the Zarr API",
      py::arg("level"));

    m.def(
      "get_log_level",
      []() { return Zarr_get_log_level(); },
      "Get the current log level for the Zarr API");

    auto init_status = Zarr_set_log_level(ZarrLogLevel_Info);
    if (init_status != ZarrStatusCode_Success) {
        // Log the error but don't throw, as that would prevent module import
        std::cerr << "Warning: Failed to set initial log level: "
                  << Zarr_get_status_message(init_status) << std::endl;
    }
}