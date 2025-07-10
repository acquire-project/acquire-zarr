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

  private:
    std::string endpoint_;
    std::string bucket_name_;
    std::optional<std::string> region_;
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

  private:
    std::string output_key_;
    std::optional<PyZarrCompressionSettings> compression_settings_;
    std::vector<PyZarrDimensionProperties> dims_;
    ZarrDataType data_type_{ ZarrDataType_uint8 };
    std::optional<ZarrDownsamplingMethod> downsampling_method_{ std::nullopt };
};

PYBIND11_MAKE_OPAQUE(std::vector<PyZarrArraySettings>);

class PyZarrStreamSettings
{
  public:
    PyZarrStreamSettings() = default;
    ~PyZarrStreamSettings() = default;

    const std::string& store_path() const { return store_path_; }
    void set_store_path(const std::string& path) { store_path_ = path; }

    const std::optional<PyZarrS3Settings>& s3() const { return s3_settings_; }
    void set_s3(const std::optional<PyZarrS3Settings>& settings)
    {
        s3_settings_ = settings;
    }

    ZarrVersion version() const { return version_; }
    void set_version(ZarrVersion version) { version_ = version; }

    unsigned int max_threads() const { return max_threads_; }
    void set_max_threads(unsigned int max_threads)
    {
        max_threads_ = max_threads;
    }

    bool overwrite() const { return overwrite_; }
    void set_overwrite(bool overwrite) { overwrite_ = overwrite; }

    const std::vector<PyZarrArraySettings>& arrays() const { return arrays_; }
    std::vector<PyZarrArraySettings>& arrays() { return arrays_; }
    void set_arrays(const std::vector<PyZarrArraySettings>& arrays)
    {
        arrays_ = arrays;
    }

  private:
    std::string store_path_;
    std::optional<PyZarrS3Settings> s3_settings_{ std::nullopt };
    ZarrVersion version_{ ZarrVersion_2 };
    unsigned int max_threads_{ std::thread::hardware_concurrency() };
    bool overwrite_{ false };
    std::vector<PyZarrArraySettings> arrays_;
};

class PyZarrStream
{
  public:
    explicit PyZarrStream(const PyZarrStreamSettings& settings)
    {
        open_(settings);
    }

    void append(py::array image_data, std::optional<std::string> key)
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

    void write_contiguous_data(py::array frame, std::optional<std::string> key)
    {
        // double check the frame is C-contiguous
        py::array contiguous_data;
        if (!(frame.flags() & py::array::c_style)) {
            py::module np = py::module::import("numpy");
            contiguous_data = np.attr("ascontiguousarray")(frame);
        } else {
            contiguous_data = frame;
        }

        auto buf = contiguous_data.request();
        auto* ptr = (uint8_t*)buf.ptr;

        py::gil_scoped_release release;

        const char* key_str = key.has_value() ? key->c_str() : nullptr;
        size_t bytes_out, bytes_in = buf.itemsize * buf.size;
        auto status =
          ZarrStream_append(stream_.get(), ptr, bytes_in, &bytes_out, key_str);

        py::gil_scoped_acquire acquire;

        if (status != ZarrStatusCode_Success) {
            std::string err = "Failed to append data to Zarr stream: " +
                              std::string(Zarr_get_status_message(status));
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        } else if (bytes_out != bytes_in) {
            std::string err = "Expected to write " + std::to_string(bytes_in) +
                              " bytes, wrote " + std::to_string(bytes_out) +
                              ".";
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
    }

    bool write_custom_metadata(py::str custom_metadata, bool overwrite)
    {
        if (!is_active()) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Cannot write metadata unless streaming.");
            throw py::error_already_set();
        }

        auto status = ZarrStream_write_custom_metadata(
          stream_.get(),
          custom_metadata.cast<std::string>().c_str(),
          overwrite);

        if (status == ZarrStatusCode_WillNotOverwrite) {
            return false; // Metadata already exists and overwrite is false
        } else if (status != ZarrStatusCode_Success) {
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

        try {
            stream_.reset(); // calls ZarrStream_destroy
        } catch (const std::exception& exc) {
            std::string err =
              "Failed to close Zarr stream: " + std::string(exc.what());
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
    }

  private:
    using ZarrStreamPtr =
      std::unique_ptr<ZarrStream, decltype(ZarrStreamDeleter)>;
    struct ArrayLifetimeProps
    {
        std::string output_key;
        ZarrCompressionSettings compression;
        std::vector<ZarrDimensionProperties> dimension_props;
        std::vector<std::string> dimension_names;
        std::vector<std::string> dimension_units;
    };

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

        size_t n_arrays = settings.arrays().size();
        if (n_arrays == 0) {
            PyErr_SetString(PyExc_ValueError,
                            "At least one array must be specified.");
            throw py::error_already_set();
        }

        ZarrS3Settings s3_settings;

        ZarrStreamSettings stream_settings{
            .store_path = nullptr,
            .s3_settings = nullptr,
            .version = settings.version(),
            .max_threads = settings.max_threads(),
            .overwrite = settings.overwrite(),
            .arrays = new ZarrArraySettings[n_arrays],
            .array_count = n_arrays,
        };

        store_path_ = settings.store_path();
        stream_settings.store_path = store_path_.c_str();

        if (settings.s3().has_value()) {
            const auto& s3 = settings.s3().value();
            s3_endpoint_ = s3.endpoint();
            s3_settings.endpoint = s3_endpoint_.c_str();

            s3_bucket_name_ = s3.bucket_name();
            s3_settings.bucket_name = s3_bucket_name_.c_str();

            if (s3.region().has_value()) {
                s3_region_ = s3.region().value();
                s3_settings.region = s3_region_.c_str();
            } else {
                s3_settings.region = nullptr;
            }

            stream_settings.s3_settings = &s3_settings;
        }

        std::vector<ArrayLifetimeProps> array_props_array(n_arrays);
        for (auto i = 0; i < n_arrays; ++i) {
            const auto& array_settings = settings.arrays()[i];
            auto& array_lt_props = array_props_array[i];
            auto& stream_array = stream_settings.arrays[i];

            array_lt_props.output_key = array_settings.output_key();
            stream_array.output_key = array_lt_props.output_key.c_str();

            stream_array.compression_settings = nullptr;
            if (array_settings.compression().has_value()) {
                const auto compression = *array_settings.compression();

                // construct compression settings to live long enough
                array_lt_props.compression = {
                    .compressor = compression.compressor(),
                    .codec = compression.codec(),
                    .level = compression.level(),
                    .shuffle = compression.shuffle(),
                };

                stream_array.compression_settings = &array_lt_props.compression;
            }

            const auto& dims = array_settings.dimensions();
            array_lt_props.dimension_names.resize(dims.size());
            auto& dim_names = array_lt_props.dimension_names;
            array_lt_props.dimension_units.resize(dims.size());
            auto& dim_units = array_lt_props.dimension_units;

            auto& dimension_props = array_lt_props.dimension_props;
            for (auto j = 0; j < dims.size(); ++j) {
                const auto& dim = dims[j];
                dim_names[j] = dim.name();
                dim_units[j] = dim.unit().has_value() ? *dim.unit() : "";

                ZarrDimensionProperties properties{
                    .name = dim_names[j].c_str(),
                    .type = dim.type(),
                    .array_size_px = dim.array_size_px(),
                    .chunk_size_px = dim.chunk_size_px(),
                    .shard_size_chunks = dim.shard_size_chunks(),
                    .unit = dim_units[j].c_str(),
                    .scale = dim.scale(),
                };
                dimension_props.push_back(properties);
            }

            stream_array.dimensions = dimension_props.data();
            stream_array.dimension_count = dims.size();

            stream_array.data_type = array_settings.data_type();

            auto downsampling_method = array_settings.downsampling_method();
            stream_array.multiscale = downsampling_method.has_value();
            if (stream_array.multiscale) {
                stream_array.downsampling_method = *downsampling_method;
            }
        }

        stream_ =
          ZarrStreamPtr(ZarrStream_create(&stream_settings), ZarrStreamDeleter);
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

    py::enum_<ZarrVersion>(m, "ZarrVersion")
      .value("V2", ZarrVersion_2)
      .value("V3", ZarrVersion_3);

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
      .value(compressor_to_str(ZarrCompressor_Blosc1), ZarrCompressor_Blosc1);

    py::enum_<ZarrCompressionCodec>(m, "CompressionCodec")
      .value(compression_codec_to_str(ZarrCompressionCodec_None),
             ZarrCompressionCodec_None)
      .value(compression_codec_to_str(ZarrCompressionCodec_BloscLZ4),
             ZarrCompressionCodec_BloscLZ4)
      .value(compression_codec_to_str(ZarrCompressionCodec_BloscZstd),
             ZarrCompressionCodec_BloscZstd);

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
                    std::optional<ZarrDataType> data_type,
                    std::optional<ZarrDownsamplingMethod> downsampling_method) {
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
                settings.set_data_type(*data_type);
            }
            if (downsampling_method) {
                settings.set_downsampling_method(*downsampling_method);
            }

            return settings;
        }),
        py::kw_only(),
        py::arg("output_key") = std::nullopt,
        py::arg("compression") = std::nullopt,
        py::arg("dimensions") = std::nullopt,
        py::arg("data_type") = std::nullopt,
        py::arg("downsampling_method") = std::nullopt)
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
      .def_property("data_type",
                    &PyZarrArraySettings::data_type,
                    &PyZarrArraySettings::set_data_type)
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
        });

    py::class_<PyZarrStreamSettings>(m, "StreamSettings", py::dynamic_attr())
      .def(py::init([](std::optional<std::string> store_path,
                       std::optional<PyZarrS3Settings> s3,
                       std::optional<ZarrVersion> version,
                       std::optional<unsigned> max_threads,
                       std::optional<bool> overwrite,
                       std::optional<py::list> arrays) {
               PyZarrStreamSettings settings;
               if (store_path) {
                   settings.set_store_path(*store_path);
               }
               if (s3) {
                   settings.set_s3(*s3);
               }
               if (version) {
                   settings.set_version(*version);
               }
               if (max_threads) {
                   settings.set_max_threads(*max_threads);
               }
               if (overwrite) {
                   settings.set_overwrite(*overwrite);
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
           py::arg("arrays") = std::nullopt)
      .def("__repr__",
           [](const PyZarrStreamSettings& self) {
               std::string repr =
                 "StreamSettings(store_path='" + self.store_path() + "'";

               if (self.s3().has_value()) {
                   repr += ", s3=" + self.s3()->repr();
               }
               repr +=
                 ", version=ZarrVersion." +
                 std::string(self.version() == ZarrVersion_2 ? "V2" : "V3") +
                 ", max_threads=" + std::to_string(self.max_threads()) + "," +
                 (self.overwrite() ? " overwrite=True" : " overwrite=False") +
                 ")";
               return repr;
           })
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
      .def_property("version",
                    &PyZarrStreamSettings::version,
                    &PyZarrStreamSettings::set_version)
      .def_property("max_threads",
                    &PyZarrStreamSettings::max_threads,
                    &PyZarrStreamSettings::set_max_threads)
      .def_property("overwrite",
                    &PyZarrStreamSettings::overwrite,
                    &PyZarrStreamSettings::set_overwrite)
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
        });

    py::class_<PyZarrStream>(m, "ZarrStream")
      .def(py::init<PyZarrStreamSettings>())
      .def("close", &PyZarrStream::close)
      .def("append", &PyZarrStream::append,
           py::arg("data"),
           py::arg("key") = std::nullopt)
      .def("write_custom_metadata",
           &PyZarrStream::write_custom_metadata,
           py::arg("custom_metadata"),
           py::arg("overwrite"))
      .def("is_active", &PyZarrStream::is_active);

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
