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

    ZarrCompressionSettings compression;
    bool has_compression{ false };
    ZarrDataType data_type;
    std::optional<ZarrDownsamplingMethod> downsampling_method;
    uint32_t max_levels{ 0 };

    struct OMEChannelStorage
    {
        std::optional<std::string> label;
        std::optional<std::string> color;
        ZarrOMEWindow window{};
        bool active{ false };
        std::optional<std::string> family;
        std::optional<double> coefficient;
        bool inverted{ false };
    };

    bool has_omero{ false };
    std::optional<uint32_t> omero_id;
    std::optional<std::string> omero_name;
    std::vector<OMEChannelStorage> omero_channels;
    bool omero_has_rdefs{ false };
    std::optional<std::string> omero_model;
    uint32_t omero_default_t{ 0 };
    uint32_t omero_default_z{ 0 };

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
        array_settings_.multiscale = downsampling_method.has_value();
        array_settings_.downsampling_method =
          downsampling_method.value_or(ZarrDownsamplingMethod_Mean);
        array_settings_.max_levels = max_levels;

        if (!storage_dimension_order.empty()) {
            array_settings_.storage_dimension_order =
              storage_dimension_order.data();
        } else {
            array_settings_.storage_dimension_order = nullptr;
        }

        if (has_omero) {
            omero_.has_id = omero_id.has_value();
            omero_.id = omero_id.value_or(0);
            omero_.name = omero_name ? omero_name->c_str() : nullptr;

            channels_c_.assign(omero_channels.size(), ZarrOMEChannel{});
            for (size_t i = 0; i < omero_channels.size(); ++i) {
                const auto& src = omero_channels[i];
                auto& dst = channels_c_[i];
                dst.label = src.label ? src.label->c_str() : nullptr;
                dst.color = src.color ? src.color->c_str() : nullptr;
                dst.window = src.window;
                dst.active = src.active;
                dst.family = src.family ? src.family->c_str() : nullptr;
                dst.has_coefficient = src.coefficient.has_value();
                dst.coefficient = src.coefficient.value_or(0.0);
                dst.inverted = src.inverted;
            }
            omero_.channels =
              channels_c_.empty() ? nullptr : channels_c_.data();
            omero_.channel_count = channels_c_.size();

            omero_.has_rdefs = omero_has_rdefs;
            if (omero_has_rdefs) {
                omero_.rdefs.model =
                  omero_model ? omero_model->c_str() : nullptr;
                omero_.rdefs.default_t = omero_default_t;
                omero_.rdefs.default_z = omero_default_z;
            } else {
                omero_.rdefs = ZarrOMERenderingDefs{};
            }

            array_settings_.omero = &omero_;
        } else {
            array_settings_.omero = nullptr;
        }

        return &array_settings_;
    }

  private:
    ZarrArraySettings array_settings_{};
    ZarrOMERenderingSettings omero_{};
    std::vector<ZarrOMEChannel> channels_c_;
};

struct FieldOfViewLifetimeProps
{
    std::string path;
    std::optional<uint32_t> acquisition_id;
    ArrayLifetimeProps array;

    ZarrHCSFieldOfView* field_of_view()
    {
        field_of_view_.path = path.c_str();

        if (acquisition_id.has_value()) {
            field_of_view_.acquisition_id = *acquisition_id;
            field_of_view_.has_acquisition_id = true;
        } else {
            field_of_view_.has_acquisition_id = false;
        }

        field_of_view_.array_settings = array.array_settings();

        return &field_of_view_;
    }

  private:
    ZarrHCSFieldOfView field_of_view_{};
};

struct WellLifetimeProps
{
    std::string row_name;
    std::string column_name;
    std::vector<FieldOfViewLifetimeProps> images;

    ZarrHCSWell* well()
    {
        well_.row_name = row_name.c_str();
        well_.column_name = column_name.c_str();

        const size_t n_images = images.size();
        if (const auto code = ZarrHCSWell_create_image_array(&well_, n_images);
            code != ZarrStatusCode_Success) {
            const std::string err =
              "Failed to allocate memory for images array: " +
              std::string(Zarr_get_status_message(code));
            PyErr_SetString(PyExc_ValueError, err.c_str());
            throw py::error_already_set();
        }

        for (size_t i = 0; i < n_images; ++i) {
            well_.images[i] = *images[i].field_of_view();
        }

        return &well_;
    }

  private:
    ZarrHCSWell well_{};
};

struct AcquisitionLifetimeProps
{
    uint32_t id;
    std::string name;
    bool has_name;
    std::string description;
    bool has_description;
    std::optional<uint64_t> start_time;
    std::optional<uint64_t> end_time;

    ZarrHCSAcquisition* acquisition()
    {
        acquisition_.id = id;
        acquisition_.name = has_name ? name.c_str() : nullptr;
        acquisition_.description =
          has_description ? description.c_str() : nullptr;

        if (start_time.has_value()) {
            acquisition_.start_time = *start_time;
            acquisition_.has_start_time = true;
        } else {
            acquisition_.has_start_time = false;
        }

        if (end_time.has_value()) {
            acquisition_.end_time = *end_time;
            acquisition_.has_end_time = true;
        } else {
            acquisition_.has_end_time = false;
        }

        return &acquisition_;
    }

  private:
    ZarrHCSAcquisition acquisition_{};
};

struct PlateLifetimeProps
{
    std::string path;
    std::string name;

    std::vector<std::string> row_names;
    std::vector<std::string> column_names;

    std::vector<WellLifetimeProps> wells;
    std::vector<AcquisitionLifetimeProps> acquisitions;

    ZarrHCSPlate* plate()
    {
        plate_.path = path.c_str();
        plate_.name = name.c_str();

        const size_t n_rows = row_names.size();
        if (const auto code =
              ZarrHCSPlate_create_row_name_array(&plate_, n_rows);
            code != ZarrStatusCode_Success) {
            const std::string err = "Failed to create row names array: " +
                                    std::string(Zarr_get_status_message(code));
            py::set_error(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
        for (size_t i = 0; i < n_rows; ++i) {
            plate_.row_names[i] = row_names[i].c_str();
        }

        const size_t n_columns = column_names.size();
        if (const auto code =
              ZarrHCSPlate_create_column_name_array(&plate_, n_columns);
            code != ZarrStatusCode_Success) {
            const std::string err = "Failed to create column names array: " +
                                    std::string(Zarr_get_status_message(code));
            py::set_error(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
        for (size_t i = 0; i < n_columns; ++i) {
            plate_.column_names[i] = column_names[i].c_str();
        }

        const size_t n_wells = wells.size();
        if (const auto code = ZarrHCSPlate_create_well_array(&plate_, n_wells);
            code != ZarrStatusCode_Success) {
            const std::string err = "Failed to create wells array: " +
                                    std::string(Zarr_get_status_message(code));
            py::set_error(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
        for (size_t i = 0; i < n_wells; ++i) {
            plate_.wells[i] = *wells[i].well();
        }

        const size_t n_acqs = acquisitions.size(); // optional
        if (n_acqs > 0) {
            if (const auto code =
                  ZarrHCSPlate_create_acquisition_array(&plate_, n_acqs);
                code != ZarrStatusCode_Success) {
                const std::string err =
                  "Failed to create acquisitions array: " +
                  std::string(Zarr_get_status_message(code));
                py::set_error(PyExc_RuntimeError, err.c_str());
                throw py::error_already_set();
            }

            for (size_t i = 0; i < n_acqs; ++i) {
                plate_.acquisitions[i] = *acquisitions[i].acquisition();
            }
        }

        return &plate_;
    }

  private:
    ZarrHCSPlate plate_{};
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

class PyZarrOMEWindow
{
  public:
    PyZarrOMEWindow() = default;
    ~PyZarrOMEWindow() = default;

    double min() const { return min_; }
    void set_min(double v) { min_ = v; }

    double max() const { return max_; }
    void set_max(double v) { max_ = v; }

    double start() const { return start_; }
    void set_start(double v) { start_ = v; }

    double end() const { return end_; }
    void set_end(double v) { end_ = v; }

    std::string repr() const
    {
        return "OMEWindow(min=" + std::to_string(min_) +
               ", max=" + std::to_string(max_) +
               ", start=" + std::to_string(start_) +
               ", end=" + std::to_string(end_) + ")";
    }

  private:
    double min_{ 0.0 };
    double max_{ 0.0 };
    double start_{ 0.0 };
    double end_{ 0.0 };
};

class PyZarrOMEChannel
{
  public:
    PyZarrOMEChannel() = default;
    ~PyZarrOMEChannel() = default;

    const std::optional<std::string>& label() const { return label_; }
    void set_label(const std::optional<std::string>& v) { label_ = v; }

    const std::optional<std::string>& color() const { return color_; }
    void set_color(const std::optional<std::string>& v) { color_ = v; }

    const PyZarrOMEWindow& window() const { return window_; }
    PyZarrOMEWindow& window() { return window_; }
    void set_window(const PyZarrOMEWindow& v) { window_ = v; }

    bool active() const { return active_; }
    void set_active(bool v) { active_ = v; }

    const std::optional<std::string>& family() const { return family_; }
    void set_family(const std::optional<std::string>& v) { family_ = v; }

    std::optional<double> coefficient() const { return coefficient_; }
    void set_coefficient(std::optional<double> v) { coefficient_ = v; }

    bool inverted() const { return inverted_; }
    void set_inverted(bool v) { inverted_ = v; }

    std::string repr() const
    {
        return "OMEChannel(label=" + (label_ ? ("'" + *label_ + "'") : "None") +
               ", color=" + (color_ ? ("'" + *color_ + "'") : "None") +
               ", active=" + (active_ ? "True" : "False") + ")";
    }

  private:
    std::optional<std::string> label_;
    std::optional<std::string> color_;
    PyZarrOMEWindow window_;
    bool active_{ true };
    std::optional<std::string> family_;
    std::optional<double> coefficient_;
    bool inverted_{ false };
};

class PyZarrOMERenderingDefs
{
  public:
    PyZarrOMERenderingDefs() = default;
    ~PyZarrOMERenderingDefs() = default;

    const std::optional<std::string>& model() const { return model_; }
    void set_model(const std::optional<std::string>& v) { model_ = v; }

    uint32_t default_t() const { return default_t_; }
    void set_default_t(uint32_t v) { default_t_ = v; }

    uint32_t default_z() const { return default_z_; }
    void set_default_z(uint32_t v) { default_z_ = v; }

    std::string repr() const
    {
        return "OMERenderingDefs(model=" +
               (model_ ? ("'" + *model_ + "'") : "None") +
               ", default_t=" + std::to_string(default_t_) +
               ", default_z=" + std::to_string(default_z_) + ")";
    }

  private:
    std::optional<std::string> model_;
    uint32_t default_t_{ 0 };
    uint32_t default_z_{ 0 };
};

class PyZarrOMERenderingSettings
{
  public:
    PyZarrOMERenderingSettings() = default;
    ~PyZarrOMERenderingSettings() = default;

    const std::optional<uint32_t>& id() const { return id_; }
    void set_id(const std::optional<uint32_t>& v) { id_ = v; }

    const std::optional<std::string>& name() const { return name_; }
    void set_name(const std::optional<std::string>& v) { name_ = v; }

    const std::vector<PyZarrOMEChannel>& channels() const { return channels_; }
    std::vector<PyZarrOMEChannel>& channels() { return channels_; }
    void set_channels(const std::vector<PyZarrOMEChannel>& v) { channels_ = v; }

    const std::shared_ptr<PyZarrOMERenderingDefs>& rdefs() const
    {
        return rdefs_;
    }
    void set_rdefs(std::shared_ptr<PyZarrOMERenderingDefs> v)
    {
        rdefs_ = std::move(v);
    }

    std::string repr() const
    {
        return "OMERenderingSettings(channels=[" +
               std::to_string(channels_.size()) +
               " channel(s)], id=" + (id_ ? std::to_string(*id_) : "None") +
               ", name=" + (name_ ? ("'" + *name_ + "'") : "None") + ")";
    }

  private:
    std::optional<uint32_t> id_;
    std::optional<std::string> name_;
    std::vector<PyZarrOMEChannel> channels_;
    std::shared_ptr<PyZarrOMERenderingDefs> rdefs_;
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
PYBIND11_MAKE_OPAQUE(std::vector<PyZarrOMEChannel>);

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

    const std::shared_ptr<PyZarrOMERenderingSettings>& omero() const
    {
        return omero_settings_;
    }

    void set_omero(std::shared_ptr<PyZarrOMERenderingSettings> settings)
    {
        omero_settings_ = std::move(settings);
    }

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

        // omero rendering metadata
        if (omero_settings_) {
            const auto& omero = *omero_settings_;
            lt_props.has_omero = true;
            lt_props.omero_id = omero.id();
            lt_props.omero_name = omero.name();

            lt_props.omero_channels.clear();
            lt_props.omero_channels.reserve(omero.channels().size());
            for (const auto& c : omero.channels()) {
                ArrayLifetimeProps::OMEChannelStorage storage;
                storage.label = c.label();
                storage.color = c.color();
                storage.window = {
                    .min = c.window().min(),
                    .max = c.window().max(),
                    .start = c.window().start(),
                    .end = c.window().end(),
                };
                storage.active = c.active();
                storage.family = c.family();
                storage.coefficient = c.coefficient();
                storage.inverted = c.inverted();
                lt_props.omero_channels.push_back(std::move(storage));
            }

            if (omero.rdefs()) {
                lt_props.omero_has_rdefs = true;
                lt_props.omero_model = omero.rdefs()->model();
                lt_props.omero_default_t = omero.rdefs()->default_t();
                lt_props.omero_default_z = omero.rdefs()->default_z();
            } else {
                lt_props.omero_has_rdefs = false;
            }
        } else {
            lt_props.has_omero = false;
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
    std::shared_ptr<PyZarrOMERenderingSettings> omero_settings_;
    std::vector<PyZarrDimensionProperties> dims_;
    ZarrDataType data_type_{ ZarrDataType_uint8 };
    std::optional<ZarrDownsamplingMethod> downsampling_method_{ std::nullopt };
    uint32_t max_levels_{ 0 };
    std::vector<std::string> storage_dimension_order_;
};

class PyZarrFieldOfView
{
  public:
    PyZarrFieldOfView() = default;
    ~PyZarrFieldOfView() = default;

    const std::string& path() const { return path_; }
    void set_path(const std::string& path) { path_ = path; }

    std::optional<uint32_t> acquisition_id() const { return acquisition_id_; }

    void set_acquisition_id(const std::optional<uint32_t>& id)
    {
        acquisition_id_ = id;
    }

    const PyZarrArraySettings& array_settings() const
    {
        return array_settings_;
    }

    void set_array_settings(const PyZarrArraySettings& settings)
    {
        array_settings_ = settings;
    }

    FieldOfViewLifetimeProps to_lifetime_props() const
    {
        FieldOfViewLifetimeProps lt_props{};
        lt_props.path = path_;
        lt_props.acquisition_id = acquisition_id_;
        lt_props.array = array_settings_.to_lifetime_props();

        return lt_props;
    }

  private:
    std::string path_;
    std::optional<uint32_t> acquisition_id_;
    PyZarrArraySettings array_settings_;
};

class PyZarrWell
{
  public:
    PyZarrWell() = default;
    ~PyZarrWell() = default;

    const std::string& row_name() const { return row_name_; }
    void set_row_name(const std::string& name) { row_name_ = name; }

    const std::string& column_name() const { return column_name_; }
    void set_column_name(const std::string& name) { column_name_ = name; }

    const std::vector<PyZarrFieldOfView>& images() const { return images_; }
    std::vector<PyZarrFieldOfView>& images() { return images_; }

    void set_images(const std::vector<PyZarrFieldOfView>& dims)
    {
        images_ = dims;
    }

    WellLifetimeProps to_lifetime_props() const
    {
        WellLifetimeProps lt_props{};

        lt_props.row_name = row_name_;
        lt_props.column_name = column_name_;

        const size_t n_images = images_.size();
        lt_props.images.resize(n_images);

        for (auto i = 0; i < n_images; ++i) {
            lt_props.images[i] = images_[i].to_lifetime_props();
        }

        return lt_props;
    }

  private:
    std::string row_name_;
    std::string column_name_;
    std::vector<PyZarrFieldOfView> images_;
};

class PyZarrAcquisition
{
  public:
    PyZarrAcquisition() = default;
    ~PyZarrAcquisition() = default;

    uint32_t id() const { return id_; }
    void set_id(uint32_t id) { id_ = id; }

    std::optional<std::string> name() const { return name_; }
    void set_name(const std::optional<std::string>& name) { name_ = name; }

    std::optional<std::string> description() const { return description_; }

    void set_description(const std::optional<std::string>& description)
    {
        description_ = description;
    }

    std::optional<uint64_t> start_time() const { return start_time_; }

    void set_start_time(const std::optional<uint64_t>& time)
    {
        start_time_ = time;
    }

    std::optional<uint64_t> end_time() const { return end_time_; }
    void set_end_time(const std::optional<uint64_t>& time) { end_time_ = time; }

    AcquisitionLifetimeProps to_lifetime_props() const
    {
        AcquisitionLifetimeProps lt_props{};

        lt_props.id = id_;

        if (name_.has_value()) {
            lt_props.name = *name_;
        }
        lt_props.has_name = name_.has_value();

        if (description_.has_value()) {
            lt_props.description = *description_;
        }
        lt_props.has_description = description_.has_value();

        lt_props.start_time = start_time_;
        lt_props.end_time = end_time_;

        return lt_props;
    }

  private:
    uint32_t id_;
    std::optional<std::string> name_;
    std::optional<std::string> description_;
    std::optional<uint64_t> start_time_;
    std::optional<uint64_t> end_time_;
};

class PyZarrPlate
{
  public:
    PyZarrPlate() = default;
    ~PyZarrPlate() = default;

    const std::string& path() const { return path_; }
    void set_path(const std::string& path) { path_ = path; }

    const std::string& name() const { return name_; }
    void set_name(const std::string& name) { name_ = name; }

    const std::vector<std::string>& row_names() const { return row_names_; }
    std::vector<std::string>& row_names() { return row_names_; }

    void set_row_names(const std::vector<std::string>& names)
    {
        row_names_ = names;
    }

    const std::vector<std::string>& column_names() const
    {
        return column_names_;
    }

    std::vector<std::string>& column_names() { return column_names_; }

    void set_column_names(const std::vector<std::string>& names)
    {
        column_names_ = names;
    }

    const std::vector<PyZarrWell>& wells() const { return wells_; }
    std::vector<PyZarrWell>& wells() { return wells_; }
    void set_wells(const std::vector<PyZarrWell>& wells) { wells_ = wells; }

    const std::vector<PyZarrAcquisition>& acquisitions() const
    {
        return acquisitions_;
    }

    std::vector<PyZarrAcquisition>& acquisitions() { return acquisitions_; }

    void set_acquisitions(const std::vector<PyZarrAcquisition>& acqs)
    {
        acquisitions_ = acqs;
    }

    PlateLifetimeProps to_lifetime_props() const
    {
        PlateLifetimeProps lt_props{};

        lt_props.name = name_;
        lt_props.path = path_;

        const size_t n_rows = row_names_.size();
        const size_t n_columns = column_names_.size();
        const size_t n_wells = wells_.size();
        const size_t n_acqs = acquisitions_.size();

        lt_props.row_names.resize(n_rows);
        lt_props.row_names.resize(n_rows);
        for (size_t i = 0; i < n_rows; ++i) {
            lt_props.row_names[i] = row_names_[i];
        }

        lt_props.column_names.resize(n_columns);
        for (size_t i = 0; i < n_columns; ++i) {
            lt_props.column_names[i] = column_names_[i];
        }

        lt_props.wells.resize(n_wells);
        for (size_t i = 0; i < n_wells; ++i) {
            lt_props.wells[i] = wells_[i].to_lifetime_props();
        }

        lt_props.acquisitions.resize(n_acqs);
        for (size_t i = 0; i < n_acqs; ++i) {
            lt_props.acquisitions[i] = acquisitions_[i].to_lifetime_props();
        }

        return lt_props;
    }

  private:
    std::string path_;
    std::string name_;
    std::vector<std::string> row_names_;
    std::vector<std::string> column_names_;
    std::vector<PyZarrWell> wells_;
    std::vector<PyZarrAcquisition> acquisitions_;
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

    ZarrOMEVersion ome_version() const { return ome_version_; }
    void set_ome_version(ZarrOMEVersion version) { ome_version_ = version; }

    const std::vector<PyZarrArraySettings>& arrays() const { return arrays_; }
    std::vector<PyZarrArraySettings>& arrays() { return arrays_; }

    void set_arrays(const std::vector<PyZarrArraySettings>& arrays)
    {
        arrays_ = arrays;
    }

    const std::vector<PyZarrPlate>& plates() const { return plates_; }
    std::vector<PyZarrPlate>& plates() { return plates_; }

    void set_plates(const std::vector<PyZarrPlate>& plates)
    {
        plates_ = plates;
    }

    ZarrStreamSettings* to_settings() const
    {
        memset(&settings_, 0, sizeof(settings_));

        settings_.store_path = store_path_.c_str();
        settings_.max_threads = max_threads_;
        settings_.overwrite = static_cast<int>(overwrite_);
        settings_.ome_version = ome_version_;

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

        // construct plate lifetime props and set up HCS settings
        const size_t n_plates = plates_.size();

        plate_lifetimes_.clear();
        for (size_t i = 0; i < n_plates; ++i) {
            plate_lifetimes_.push_back(plates_[i].to_lifetime_props());
        }

        plate_settings_.clear();
        for (size_t i = 0; i < n_plates; ++i) {
            plate_settings_.emplace_back(*plate_lifetimes_[i].plate());
        }

        if (n_plates > 0) {
            hcs_settings_.plates = plate_settings_.data();
            hcs_settings_.plate_count = n_plates;
            settings_.hcs_settings = &hcs_settings_;
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
    ZarrOMEVersion ome_version_{ ZarrOMEVersion_0_5 };

    std::vector<PyZarrArraySettings> arrays_;
    std::vector<PyZarrPlate> plates_;

    mutable ZarrS3Settings s3_settings_;

    mutable std::vector<ArrayLifetimeProps> array_lifetimes_;
    mutable std::vector<PlateLifetimeProps> plate_lifetimes_;

    mutable std::vector<ZarrArraySettings> array_settings_;
    mutable std::vector<ZarrHCSPlate> plate_settings_;
    mutable ZarrHCSSettings hcs_settings_;

    mutable ZarrStreamSettings settings_;
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
    // multiscale is implicit in Python: it is on iff a downsampling method is
    // set
    if (a.multiscale) {
        arr.set_downsampling_method(a.downsampling_method);
        arr.set_max_levels(a.max_levels);
    }
    if (a.compression_settings) {
        PyZarrCompressionSettings c;
        c.set_compressor(a.compression_settings->compressor);
        c.set_codec(a.compression_settings->codec);
        c.set_level(a.compression_settings->level);
        c.set_shuffle(a.compression_settings->shuffle);
        arr.set_compression(c);
    }

    if (a.omero) {
        auto omero = std::make_shared<PyZarrOMERenderingSettings>();
        if (a.omero->has_id) {
            omero->set_id(a.omero->id);
        }
        if (a.omero->name) {
            omero->set_name(std::string(a.omero->name));
        }

        std::vector<PyZarrOMEChannel> channels;
        channels.reserve(a.omero->channel_count);
        for (size_t i = 0; i < a.omero->channel_count; ++i) {
            const auto& c = a.omero->channels[i];
            PyZarrOMEChannel channel;
            if (c.label) {
                channel.set_label(std::string(c.label));
            }
            if (c.color) {
                channel.set_color(std::string(c.color));
            }
            PyZarrOMEWindow window;
            window.set_min(c.window.min);
            window.set_max(c.window.max);
            window.set_start(c.window.start);
            window.set_end(c.window.end);
            channel.set_window(window);
            channel.set_active(c.active);
            if (c.family) {
                channel.set_family(std::string(c.family));
            }
            if (c.has_coefficient) {
                channel.set_coefficient(c.coefficient);
            }
            channel.set_inverted(c.inverted);
            channels.push_back(channel);
        }
        omero->set_channels(channels);

        if (a.omero->has_rdefs) {
            auto rdefs = std::make_shared<PyZarrOMERenderingDefs>();
            if (a.omero->rdefs.model) {
                rdefs->set_model(std::string(a.omero->rdefs.model));
            }
            rdefs->set_default_t(a.omero->rdefs.default_t);
            rdefs->set_default_z(a.omero->rdefs.default_z);
            omero->set_rdefs(rdefs);
        }

        arr.set_omero(omero);
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
            order.push_back(idx < dims.size() ? dims[idx].name()
                                              : std::string());
        }
        arr.set_storage_dimension_order(order);
    }
    return arr;
}

PyZarrAcquisition
read_acquisition(const ZarrHCSAcquisition& a)
{
    PyZarrAcquisition acq;
    acq.set_id(a.id);
    if (a.name) {
        acq.set_name(std::string(a.name));
    }
    if (a.description) {
        acq.set_description(std::string(a.description));
    }
    if (a.has_start_time) {
        acq.set_start_time(a.start_time);
    }
    if (a.has_end_time) {
        acq.set_end_time(a.end_time);
    }
    return acq;
}

PyZarrFieldOfView
read_fov(const ZarrHCSFieldOfView& f)
{
    PyZarrFieldOfView fov;
    fov.set_path(f.path ? f.path : "");
    if (f.has_acquisition_id) {
        fov.set_acquisition_id(f.acquisition_id);
    }
    if (f.array_settings) {
        fov.set_array_settings(read_array(*f.array_settings));
    }
    return fov;
}

PyZarrWell
read_well(const ZarrHCSWell& w)
{
    PyZarrWell well;
    well.set_row_name(w.row_name ? w.row_name : "");
    well.set_column_name(w.column_name ? w.column_name : "");
    std::vector<PyZarrFieldOfView> images;
    images.reserve(w.image_count);
    for (size_t i = 0; i < w.image_count; ++i) {
        images.push_back(read_fov(w.images[i]));
    }
    well.set_images(images);
    return well;
}

PyZarrPlate
read_plate(const ZarrHCSPlate& p)
{
    PyZarrPlate plate;
    plate.set_path(p.path ? p.path : "");
    if (p.name) {
        plate.set_name(p.name);
    }

    std::vector<std::string> rows;
    rows.reserve(p.row_count);
    for (size_t i = 0; i < p.row_count; ++i) {
        rows.emplace_back(p.row_names[i] ? p.row_names[i] : "");
    }
    plate.set_row_names(rows);

    std::vector<std::string> cols;
    cols.reserve(p.column_count);
    for (size_t i = 0; i < p.column_count; ++i) {
        cols.emplace_back(p.column_names[i] ? p.column_names[i] : "");
    }
    plate.set_column_names(cols);

    std::vector<PyZarrAcquisition> acqs;
    acqs.reserve(p.acquisition_count);
    for (size_t i = 0; i < p.acquisition_count; ++i) {
        acqs.push_back(read_acquisition(p.acquisitions[i]));
    }
    plate.set_acquisitions(acqs);

    std::vector<PyZarrWell> wells;
    wells.reserve(p.well_count);
    for (size_t i = 0; i < p.well_count; ++i) {
        wells.push_back(read_well(p.wells[i]));
    }
    plate.set_wells(wells);
    return plate;
}

PyZarrStreamSettings
read_stream_settings(const ZarrStreamSettings& s)
{
    PyZarrStreamSettings out;
    out.set_store_path(s.store_path ? s.store_path : "");
    out.set_max_threads(s.max_threads);
    out.set_overwrite(s.overwrite);
    out.set_ome_version(s.ome_version);

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

    if (s.hcs_settings) {
        std::vector<PyZarrPlate> plates;
        plates.reserve(s.hcs_settings->plate_count);
        for (size_t i = 0; i < s.hcs_settings->plate_count; ++i) {
            plates.push_back(read_plate(s.hcs_settings->plates[i]));
        }
        out.set_plates(plates);
    }
    return out;
}

PyZarrStreamSettings
finish_load(ZarrStreamSettings& c, ZarrStatusCode status, const char* what)
{
    if (status != ZarrStatusCode_Success) {
        ZarrStreamSettings_destroy_loaded(&c);
        throw py::value_error(
          std::string("Failed to load stream settings from ") + what +
          " (see log for details)");
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
                               const std::optional<std::string>& array_key,
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

        const char* array_cstr =
          array_key.has_value() ? array_key->c_str() : nullptr;
        const char* meta_cstr =
          metadata_key.has_value() ? metadata_key->c_str() : nullptr;

        auto status = ZarrStream_write_custom_metadata(
          stream_.get(), array_cstr, meta_cstr, metadata_str.c_str());

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
    py::bind_vector<std::vector<PyZarrOMEChannel>>(m, "VectorOMEChannel");

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

    py::enum_<ZarrOMEVersion>(m, "OMEVersion")
      .value("V0_5", ZarrOMEVersion_0_5)
      .value("V0_6", ZarrOMEVersion_0_6);

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

    py::class_<PyZarrOMEWindow>(m, "OMEWindow", py::dynamic_attr())
      .def(py::init([](double min, double max, double start, double end) {
               PyZarrOMEWindow w;
               w.set_min(min);
               w.set_max(max);
               w.set_start(start);
               w.set_end(end);
               return w;
           }),
           py::kw_only(),
           py::arg("min") = 0.0,
           py::arg("max") = 0.0,
           py::arg("start") = 0.0,
           py::arg("end") = 0.0)
      .def("__repr__", [](const PyZarrOMEWindow& self) { return self.repr(); })
      .def_property("min", &PyZarrOMEWindow::min, &PyZarrOMEWindow::set_min)
      .def_property("max", &PyZarrOMEWindow::max, &PyZarrOMEWindow::set_max)
      .def_property(
        "start", &PyZarrOMEWindow::start, &PyZarrOMEWindow::set_start)
      .def_property("end", &PyZarrOMEWindow::end, &PyZarrOMEWindow::set_end);

    py::class_<PyZarrOMEChannel>(m, "OMEChannel", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> label,
                       std::optional<std::string> color,
                       std::optional<PyZarrOMEWindow> window,
                       bool active,
                       std::optional<std::string> family,
                       std::optional<double> coefficient,
                       bool inverted) {
               PyZarrOMEChannel c;
               c.set_label(label);
               c.set_color(color);
               if (window) {
                   c.set_window(*window);
               }
               c.set_active(active);
               c.set_family(family);
               c.set_coefficient(coefficient);
               c.set_inverted(inverted);
               return c;
           }),
           py::kw_only(),
           py::arg("label") = std::nullopt,
           py::arg("color") = std::nullopt,
           py::arg("window") = std::nullopt,
           py::arg("active") = true,
           py::arg("family") = std::nullopt,
           py::arg("coefficient") = std::nullopt,
           py::arg("inverted") = false)
      .def("__repr__", [](const PyZarrOMEChannel& self) { return self.repr(); })
      .def_property(
        "label", &PyZarrOMEChannel::label, &PyZarrOMEChannel::set_label)
      .def_property(
        "color", &PyZarrOMEChannel::color, &PyZarrOMEChannel::set_color)
      .def_property("window",
                    py::cpp_function(
                      [](PyZarrOMEChannel& self) -> PyZarrOMEWindow& {
                          return self.window();
                      },
                      py::return_value_policy::reference_internal),
                    &PyZarrOMEChannel::set_window)
      .def_property(
        "active", &PyZarrOMEChannel::active, &PyZarrOMEChannel::set_active)
      .def_property(
        "family", &PyZarrOMEChannel::family, &PyZarrOMEChannel::set_family)
      .def_property("coefficient",
                    &PyZarrOMEChannel::coefficient,
                    &PyZarrOMEChannel::set_coefficient)
      .def_property("inverted",
                    &PyZarrOMEChannel::inverted,
                    &PyZarrOMEChannel::set_inverted);

    py::class_<PyZarrOMERenderingDefs, std::shared_ptr<PyZarrOMERenderingDefs>>(
      m, "OMERenderingDefs", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> model,
                       uint32_t default_t,
                       uint32_t default_z) {
               PyZarrOMERenderingDefs r;
               r.set_model(model);
               r.set_default_t(default_t);
               r.set_default_z(default_z);
               return r;
           }),
           py::kw_only(),
           py::arg("model") = std::nullopt,
           py::arg("default_t") = 0,
           py::arg("default_z") = 0)
      .def("__repr__",
           [](const PyZarrOMERenderingDefs& self) { return self.repr(); })
      .def_property("model",
                    &PyZarrOMERenderingDefs::model,
                    &PyZarrOMERenderingDefs::set_model)
      .def_property("default_t",
                    &PyZarrOMERenderingDefs::default_t,
                    &PyZarrOMERenderingDefs::set_default_t)
      .def_property("default_z",
                    &PyZarrOMERenderingDefs::default_z,
                    &PyZarrOMERenderingDefs::set_default_z);

    py::class_<PyZarrOMERenderingSettings,
               std::shared_ptr<PyZarrOMERenderingSettings>>(
      m, "OMERenderingSettings", py::dynamic_attr())
      .def(py::init([](std::optional<py::list> channels,
                       std::optional<uint32_t> id,
                       std::optional<std::string> name,
                       std::shared_ptr<PyZarrOMERenderingDefs> rdefs) {
               PyZarrOMERenderingSettings s;
               s.set_id(id);
               s.set_name(name);
               if (channels) {
                   std::vector<PyZarrOMEChannel> chans;
                   for (auto item : *channels) {
                       chans.push_back(item.cast<PyZarrOMEChannel>());
                   }
                   s.set_channels(chans);
               }
               if (rdefs) {
                   s.set_rdefs(std::move(rdefs));
               }
               return s;
           }),
           py::kw_only(),
           py::arg("channels") = std::nullopt,
           py::arg("id") = std::nullopt,
           py::arg("name") = std::nullopt,
           py::arg("rdefs") = std::shared_ptr<PyZarrOMERenderingDefs>())
      .def("__repr__",
           [](const PyZarrOMERenderingSettings& self) { return self.repr(); })
      .def_property("id",
                    &PyZarrOMERenderingSettings::id,
                    &PyZarrOMERenderingSettings::set_id)
      .def_property("name",
                    &PyZarrOMERenderingSettings::name,
                    &PyZarrOMERenderingSettings::set_name)
      .def_property(
        "channels",
        // Live, mutable view: `omero.channels[0].label = ...` and
        // `omero.channels.append(...)` write through to the stored vector.
        // The enclosing OMERenderingSettings is shared_ptr-held, so its Python
        // wrapper (and this reference) stays valid across `array.omero`.
        py::cpp_function(
          [](PyZarrOMERenderingSettings& self)
            -> std::vector<PyZarrOMEChannel>& { return self.channels(); },
          py::return_value_policy::reference_internal),
        py::cpp_function([](PyZarrOMERenderingSettings& self, py::object obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<PyZarrOMEChannel> chans;
                for (auto item : obj.cast<py::list>()) {
                    chans.push_back(item.cast<PyZarrOMEChannel>());
                }
                self.set_channels(chans);
            } else {
                PyErr_SetString(PyExc_TypeError,
                                "Expected a list of OMEChannel.");
                throw py::error_already_set();
            }
        }))
      .def_property("rdefs",
                    &PyZarrOMERenderingSettings::rdefs,
                    &PyZarrOMERenderingSettings::set_rdefs);

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
                    std::shared_ptr<PyZarrOMERenderingSettings> omero,
                    std::optional<py::list> dimensions,
                    std::optional<py::object> data_type,
                    std::optional<ZarrDownsamplingMethod> downsampling_method,
                    uint32_t max_levels,
                    std::optional<py::list> storage_dimension_order) {
            PyZarrArraySettings settings;

            if (output_key) {
                settings.set_output_key(*output_key);
            }
            if (compression) {
                settings.set_compression(*compression);
            }
            if (omero) {
                settings.set_omero(omero);
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
        py::arg("omero") = std::shared_ptr<PyZarrOMERenderingSettings>(),
        py::arg("dimensions") = std::nullopt,
        py::arg("data_type") = std::nullopt,
        py::arg("downsampling_method") = std::nullopt,
        py::arg("max_levels") = 0,
        py::arg("storage_dimension_order") = std::nullopt)
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
                       default:
                           method_str = "None";
                   }
                   repr += ", downsampling_method=" + method_str;
               }

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
        "omero", &PyZarrArraySettings::omero, &PyZarrArraySettings::set_omero)
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
                    &PyZarrArraySettings::set_storage_dimension_order);

    py::class_<PyZarrFieldOfView>(m, "FieldOfView", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> path,
                       std::optional<uint32_t> acquisition_id,
                       std::optional<PyZarrArraySettings> array_settings) {
               PyZarrFieldOfView fov;

               if (path) {
                   fov.set_path(*path);
               }
               if (acquisition_id) {
                   fov.set_acquisition_id(*acquisition_id);
               }
               if (array_settings) {
                   fov.set_array_settings(*array_settings);
               }

               return fov;
           }),
           py::kw_only(),
           py::arg("path") = std::nullopt,
           py::arg("acquisition_id") = std::nullopt,
           py::arg("array_settings") = std::nullopt)
      .def("__repr__",
           [](const PyZarrFieldOfView& self) {
               std::string repr = "FieldOfView(path='" + self.path() + "'";

               if (self.acquisition_id()) {
                   repr += ", acquisition_id=" +
                           std::to_string(*self.acquisition_id());
               }

               repr += ")";
               return repr;
           })
      .def_property(
        "path", &PyZarrFieldOfView::path, &PyZarrFieldOfView::set_path)
      .def_property(
        "acquisition_id",
        [](const PyZarrFieldOfView& self) -> py::object {
            if (self.acquisition_id()) {
                return py::cast(*self.acquisition_id());
            }
            return py::none();
        },
        [](PyZarrFieldOfView& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_acquisition_id(std::nullopt);
            } else {
                self.set_acquisition_id(obj.cast<uint32_t>());
            }
        })
      .def_property("array_settings",
                    &PyZarrFieldOfView::array_settings,
                    &PyZarrFieldOfView::set_array_settings);

    py::class_<PyZarrWell>(m, "Well", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> row_name,
                       std::optional<std::string> column_name,
                       std::optional<py::list> images) {
               PyZarrWell well;

               if (row_name) {
                   well.set_row_name(*row_name);
               }
               if (column_name) {
                   well.set_column_name(*column_name);
               }
               if (images) {
                   auto& imgs = *images;
                   std::vector<PyZarrFieldOfView> imgs_vec(imgs.size());

                   for (auto i = 0; i < imgs.size(); ++i) {
                       imgs_vec[i] = imgs[i].cast<PyZarrFieldOfView>();
                   }
                   well.set_images(imgs_vec);
               }

               return well;
           }),
           py::kw_only(),
           py::arg("row_name") = std::nullopt,
           py::arg("column_name") = std::nullopt,
           py::arg("images") = std::nullopt)
      .def("__repr__",
           [](const PyZarrWell& self) {
               return "Well(row_name='" + self.row_name() + "', column_name='" +
                      self.column_name() +
                      "', images=" + std::to_string(self.images().size()) + ")";
           })
      .def_property(
        "row_name", &PyZarrWell::row_name, &PyZarrWell::set_row_name)
      .def_property(
        "column_name", &PyZarrWell::column_name, &PyZarrWell::set_column_name)
      .def_property(
        "images",
        [](PyZarrWell& self) -> py::object {
            return py::cast(self.images(), py::return_value_policy::reference);
        },
        [](PyZarrWell& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<PyZarrFieldOfView> imgs;
                for (auto item : obj.cast<py::list>()) {
                    imgs.push_back(item.cast<PyZarrFieldOfView>());
                }
                self.set_images(imgs);
            } else {
                PyErr_SetString(PyExc_TypeError,
                                "Expected a list of FieldOfView objects.");
                throw py::error_already_set();
            }
        });

    py::class_<PyZarrAcquisition>(m, "Acquisition", py::dynamic_attr())
      .def(py::init([](std::optional<uint32_t> id,
                       std::optional<std::string> name,
                       std::optional<std::string> description,
                       std::optional<uint64_t> start_time,
                       std::optional<uint64_t> end_time) {
               PyZarrAcquisition acq;

               if (id) {
                   acq.set_id(*id);
               }
               if (name) {
                   acq.set_name(*name);
               }
               if (description) {
                   acq.set_description(*description);
               }
               if (start_time) {
                   acq.set_start_time(*start_time);
               }
               if (end_time) {
                   acq.set_end_time(*end_time);
               }

               return acq;
           }),
           py::kw_only(),
           py::arg("id") = std::nullopt,
           py::arg("name") = std::nullopt,
           py::arg("description") = std::nullopt,
           py::arg("start_time") = std::nullopt,
           py::arg("end_time") = std::nullopt)
      .def("__repr__",
           [](const PyZarrAcquisition& self) {
               std::string repr = "Acquisition(id=" + std::to_string(self.id());

               if (self.name()) {
                   repr += ", name='" + *self.name() + "'";
               }
               if (self.description()) {
                   repr += ", description='" + *self.description() + "'";
               }

               repr += ")";
               return repr;
           })
      .def_property("id", &PyZarrAcquisition::id, &PyZarrAcquisition::set_id)
      .def_property(
        "name",
        [](const PyZarrAcquisition& self) -> py::object {
            if (self.name()) {
                return py::cast(*self.name());
            }
            return py::none();
        },
        [](PyZarrAcquisition& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_name(std::nullopt);
            } else {
                self.set_name(obj.cast<std::string>());
            }
        })
      .def_property(
        "description",
        [](const PyZarrAcquisition& self) -> py::object {
            if (self.description()) {
                return py::cast(*self.description());
            }
            return py::none();
        },
        [](PyZarrAcquisition& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_description(std::nullopt);
            } else {
                self.set_description(obj.cast<std::string>());
            }
        })
      .def_property(
        "start_time",
        [](const PyZarrAcquisition& self) -> py::object {
            if (self.start_time()) {
                return py::cast(*self.start_time());
            }
            return py::none();
        },
        [](PyZarrAcquisition& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_start_time(std::nullopt);
            } else {
                self.set_start_time(obj.cast<uint32_t>());
            }
        })
      .def_property(
        "end_time",
        [](const PyZarrAcquisition& self) -> py::object {
            if (self.end_time()) {
                return py::cast(*self.end_time());
            }
            return py::none();
        },
        [](PyZarrAcquisition& self, py::object& obj) {
            if (obj.is_none()) {
                self.set_end_time(std::nullopt);
            } else {
                self.set_end_time(obj.cast<uint32_t>());
            }
        });

    py::class_<PyZarrPlate>(m, "Plate", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> path,
                       std::optional<std::string> name,
                       std::optional<py::list> row_names,
                       std::optional<py::list> column_names,
                       std::optional<py::list> wells,
                       std::optional<py::list> acquisitions) {
               PyZarrPlate plate;

               if (path) {
                   plate.set_path(*path);
               }
               if (name) {
                   plate.set_name(*name);
               }
               if (row_names) {
                   std::vector<std::string> rows;
                   for (auto item : row_names->cast<py::list>()) {
                       rows.push_back(item.cast<std::string>());
                   }
                   plate.set_row_names(rows);
               }
               if (column_names) {
                   std::vector<std::string> cols;
                   for (auto item : column_names->cast<py::list>()) {
                       cols.push_back(item.cast<std::string>());
                   }
                   plate.set_column_names(cols);
               }
               if (wells) {
                   std::vector<PyZarrWell> wells_vec;
                   for (auto item : wells->cast<py::list>()) {
                       wells_vec.push_back(item.cast<PyZarrWell>());
                   }
                   plate.set_wells(wells_vec);
               }
               if (acquisitions) {
                   std::vector<PyZarrAcquisition> acqs_vec;
                   for (auto item : acquisitions->cast<py::list>()) {
                       acqs_vec.push_back(item.cast<PyZarrAcquisition>());
                   }
                   plate.set_acquisitions(acqs_vec);
               }

               return plate;
           }),
           py::kw_only(),
           py::arg("path") = std::nullopt,
           py::arg("name") = std::nullopt,
           py::arg("row_names") = std::nullopt,
           py::arg("column_names") = std::nullopt,
           py::arg("wells") = std::nullopt,
           py::arg("acquisitions") = std::nullopt)
      .def("__repr__",
           [](const PyZarrPlate& self) {
               return "Plate(path='" + self.path() + "', name='" + self.name() +
                      "', wells=" + std::to_string(self.wells().size()) +
                      ", acquisitions=" +
                      std::to_string(self.acquisitions().size()) + ")";
           })
      .def_property("path", &PyZarrPlate::path, &PyZarrPlate::set_path)
      .def_property("name", &PyZarrPlate::name, &PyZarrPlate::set_name)
      .def_property(
        "row_names",
        [](PyZarrPlate& self) -> py::object {
            return py::cast(self.row_names(),
                            py::return_value_policy::reference);
        },
        [](PyZarrPlate& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<std::string> rows;
                for (auto item : obj.cast<py::list>()) {
                    rows.push_back(item.cast<std::string>());
                }
                self.set_row_names(rows);
            } else {
                PyErr_SetString(PyExc_TypeError, "Expected a list of strings.");
                throw py::error_already_set();
            }
        })
      .def_property(
        "column_names",
        [](PyZarrPlate& self) -> py::object {
            return py::cast(self.column_names(),
                            py::return_value_policy::reference);
        },
        [](PyZarrPlate& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<std::string> cols;
                for (auto item : obj.cast<py::list>()) {
                    cols.push_back(item.cast<std::string>());
                }
                self.set_column_names(cols);
            } else {
                PyErr_SetString(PyExc_TypeError, "Expected a list of strings.");
                throw py::error_already_set();
            }
        })
      .def_property(
        "wells",
        [](PyZarrPlate& self) -> py::object {
            return py::cast(self.wells(), py::return_value_policy::reference);
        },
        [](PyZarrPlate& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<PyZarrWell> wells_vec;
                for (auto item : obj.cast<py::list>()) {
                    wells_vec.push_back(item.cast<PyZarrWell>());
                }
                self.set_wells(wells_vec);
            } else {
                PyErr_SetString(PyExc_TypeError,
                                "Expected a list of Well objects.");
                throw py::error_already_set();
            }
        })
      .def_property(
        "acquisitions",
        [](PyZarrPlate& self) -> py::object {
            return py::cast(self.acquisitions(),
                            py::return_value_policy::reference);
        },
        [](PyZarrPlate& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<PyZarrAcquisition> acqs_vec;
                for (auto item : obj.cast<py::list>()) {
                    acqs_vec.push_back(item.cast<PyZarrAcquisition>());
                }
                self.set_acquisitions(acqs_vec);
            } else {
                PyErr_SetString(PyExc_TypeError,
                                "Expected a list of Acquisition objects.");
                throw py::error_already_set();
            }
        });

    py::class_<PyZarrStreamSettings>(m, "StreamSettings", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> store_path,
                       std::optional<PyZarrS3Settings> s3,
                       std::optional<ZarrVersion> version,
                       std::optional<unsigned> max_threads,
                       std::optional<bool> overwrite,
                       std::optional<ZarrOMEVersion> ome_version,
                       std::optional<py::list> arrays,
                       std::optional<py::list> hcs_plates) {
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
               if (ome_version) {
                   settings.set_ome_version(*ome_version);
               }
               if (arrays) {
                   auto& arrs = *arrays;
                   std::vector<PyZarrArraySettings> arrs_vec(arrs.size());

                   for (auto i = 0; i < arrs.size(); ++i) {
                       arrs_vec[i] = arrs[i].cast<PyZarrArraySettings>();
                   }
                   settings.set_arrays(arrs_vec);
               }
               if (hcs_plates) {
                   auto& plates = *hcs_plates;
                   std::vector<PyZarrPlate> plates_vec(plates.size());

                   for (auto i = 0; i < plates.size(); ++i) {
                       plates_vec[i] = plates[i].cast<PyZarrPlate>();
                   }
                   settings.set_plates(plates_vec);
               }

               return settings;
           }),
           py::kw_only(),
           py::arg("store_path") = std::nullopt,
           py::arg("s3") = std::nullopt,
           py::arg("version") = std::nullopt,
           py::arg("max_threads") = std::nullopt,
           py::arg("overwrite") = std::nullopt,
           py::arg("ome_version") = std::nullopt,
           py::arg("arrays") = std::nullopt,
           py::arg("hcs_plates") = std::nullopt)
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
      .def_property("ome_version",
                    &PyZarrStreamSettings::ome_version,
                    &PyZarrStreamSettings::set_ome_version)
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
      .def_property(
        "hcs_plates",
        [](PyZarrStreamSettings& self) -> py::object {
            return py::cast(self.plates(), py::return_value_policy::reference);
        },
        [](PyZarrStreamSettings& self, py::object& obj) {
            if (py::isinstance<py::list>(obj)) {
                std::vector<PyZarrPlate> plates;
                for (auto item : obj.cast<py::list>()) {
                    plates.push_back(item.cast<PyZarrPlate>());
                }
                self.set_plates(plates);
            } else {
                // raise a TypeError if not a list
                PyErr_SetString(PyExc_TypeError, "Expected a list of Plates.");
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
            if (ZarrStreamSettings_dump_to_file(
                  self.to_settings(), path.c_str()) != ZarrStatusCode_Success) {
                throw py::value_error("Failed to write stream settings to " +
                                      path);
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
            const std::string text = py::module::import("json")
                                       .attr("dumps")(data)
                                       .cast<std::string>();
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
           py::arg("array_key") = std::nullopt,
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