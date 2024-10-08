#include <memory>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

#include "acquire.zarr.h"

namespace py = pybind11;

namespace {
auto ZarrStreamDeleter = [](ZarrStream_s* stream) {
    if (stream) {
        ZarrStream_destroy(stream);
    }
};
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

    void set_access_key_id(const std::string& access_key_id)
    {
        access_key_id_ = access_key_id;
    }
    const std::string& access_key_id() const { return access_key_id_; }

    void set_secret_access_key(const std::string& secret_access_key)
    {
        secret_access_key_ = secret_access_key;
    }
    const std::string& secret_access_key() const { return secret_access_key_; }

  private:
    std::string endpoint_;
    std::string bucket_name_;
    std::string access_key_id_;
    std::string secret_access_key_;
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

  private:
    ZarrCompressor compressor_;
    ZarrCompressionCodec codec_;
    uint8_t level_;
    uint8_t shuffle_;
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

    uint32_t array_size_px() const { return array_size_px_; }
    void set_array_size_px(uint32_t size) { array_size_px_ = size; }

    uint32_t chunk_size_px() const { return chunk_size_px_; }
    void set_chunk_size_px(uint32_t size) { chunk_size_px_ = size; }

    uint32_t shard_size_chunks() const { return shard_size_chunks_; }
    void set_shard_size_chunks(uint32_t size) { shard_size_chunks_ = size; }

  private:
    std::string name_;
    ZarrDimensionType type_;
    uint32_t array_size_px_;
    uint32_t chunk_size_px_;
    uint32_t shard_size_chunks_;
};

class PyZarrStreamSettings
{
  public:
    PyZarrStreamSettings() = default;
    ~PyZarrStreamSettings() = default;

    std::string store_path() const { return store_path_; }
    void set_store_path(const std::string& path) { store_path_ = path; }

    std::optional<std::string> custom_metadata() const
    {
        return custom_metadata_;
    }
    void set_custom_metadata(const std::optional<std::string>& metadata)
    {
        custom_metadata_ = metadata;
    }

    std::optional<PyZarrS3Settings> s3() const { return s3_settings_; }
    void set_s3(const std::optional<PyZarrS3Settings>& settings)
    {
        s3_settings_ = settings;
    }

    std::optional<PyZarrCompressionSettings> compression() const
    {
        return compression_settings_;
    }
    void set_compression(
      const std::optional<PyZarrCompressionSettings>& settings)
    {
        compression_settings_ = settings;
    }

    std::vector<PyZarrDimensionProperties> dimensions() const
    {
        return dimensions_;
    }
    void set_dimensions(const std::vector<PyZarrDimensionProperties>& dims)
    {
        dimensions_ = dims;
    }

    bool multiscale() const { return multiscale_; }
    void set_multiscale(bool multiscale) { multiscale_ = multiscale; }

    ZarrDataType data_type() const { return data_type_; }
    void set_data_type(ZarrDataType type) { data_type_ = type; }

    ZarrVersion version() const { return version_; }
    void set_version(ZarrVersion version) { version_ = version; }

  private:
    std::string store_path_;
    std::optional<std::string> custom_metadata_;
    std::optional<PyZarrS3Settings> s3_settings_;
    std::optional<PyZarrCompressionSettings> compression_settings_;
    std::vector<PyZarrDimensionProperties> dimensions_;
    bool multiscale_ = false;
    ZarrDataType data_type_;
    ZarrVersion version_;
};

class PyZarrStream
{
  public:
    explicit PyZarrStream(const PyZarrStreamSettings& settings)
    {
        ZarrS3Settings s3_settings;
        ZarrCompressionSettings compression_settings;

        ZarrStreamSettings stream_settings{
            .store_path = nullptr,
            .custom_metadata = nullptr,
            .s3_settings = nullptr,
            .compression_settings = nullptr,
            .dimensions = nullptr,
            .dimension_count = 0,
            .multiscale = settings.multiscale(),
            .data_type = settings.data_type(),
            .version = settings.version(),
        };

        auto store_path = settings.store_path();
        stream_settings.store_path = store_path.c_str();

        auto metadata = settings.custom_metadata();
        stream_settings.custom_metadata =
          settings.custom_metadata().has_value()
            ? settings.custom_metadata()->c_str()
            : nullptr;

        if (settings.s3().has_value()) {
            s3_settings.endpoint = settings.s3()->endpoint().c_str();
            s3_settings.bucket_name = settings.s3()->bucket_name().c_str();
            s3_settings.access_key_id = settings.s3()->access_key_id().c_str();
            s3_settings.secret_access_key =
              settings.s3()->secret_access_key().c_str();
            stream_settings.s3_settings = &s3_settings;
        }

        if (settings.compression().has_value()) {
            compression_settings.compressor =
              settings.compression()->compressor();
            compression_settings.codec = settings.compression()->codec();
            compression_settings.level = settings.compression()->level();
            compression_settings.shuffle = settings.compression()->shuffle();
            stream_settings.compression_settings = &compression_settings;
        }

        const auto& dims = settings.dimensions();

        std::vector<ZarrDimensionProperties> dimension_props;
        std::vector<std::string> dimension_names(dims.size());
        for (auto i = 0; i < dims.size(); ++i) {
            const auto& dim = dims[i];
            dimension_names[i] = dim.name();
            ZarrDimensionProperties properties{
                .name = dimension_names[i].c_str(),
                .type = dim.type(),
                .array_size_px = dim.array_size_px(),
                .chunk_size_px = dim.chunk_size_px(),
                .shard_size_chunks = dim.shard_size_chunks(),
            };
            dimension_props.push_back(properties);
        }

        stream_settings.dimensions = dimension_props.data();
        stream_settings.dimension_count = dims.size();

        stream_ =
          ZarrStreamPtr(ZarrStream_create(&stream_settings), ZarrStreamDeleter);
        if (!stream_) {
            PyErr_SetString(PyExc_RuntimeError, "Failed to create Zarr stream");
            throw py::error_already_set();
        }
    }

    ~PyZarrStream()
    {
        if (is_active()) {
            ZarrStream_destroy(stream_.get());
        }
    }

    void append(py::array image_data)
    {
        if (!is_active()) {
            PyErr_SetString(PyExc_RuntimeError,
                            "Cannot append unless streaming.");
            throw py::error_already_set();
        }

        auto buf = image_data.request();
        auto* ptr = (uint8_t*)buf.ptr;

        size_t bytes_out;
        auto status = ZarrStream_append(
          stream_.get(), ptr, buf.itemsize * buf.size, &bytes_out);

        if (status != ZarrStatusCode_Success) {
            std::string err = "Failed to append data to Zarr stream: " +
                              std::string(Zarr_get_status_message(status));
            PyErr_SetString(PyExc_RuntimeError, err.c_str());
            throw py::error_already_set();
        }
    }

    bool is_active() const { return static_cast<bool>(stream_); }

  private:
    using ZarrStreamPtr =
      std::unique_ptr<ZarrStream, decltype(ZarrStreamDeleter)>;

    ZarrStreamPtr stream_;
};

PYBIND11_MODULE(acquire_zarr, m)
{
    m.doc() = R"pbdoc(
        Acquire Zarr Writer Python API
        -----------------------
        .. currentmodule:: acquire_zarr
        .. autosummary::
           :toctree: _generate
           append
    )pbdoc";

    py::enum_<ZarrVersion>(m, "Version")
      .value("V2", ZarrVersion_2)
      .value("V3", ZarrVersion_3)
      .export_values();

    py::enum_<ZarrDataType>(m, "DType")
      .value("DTYPE_UINT8", ZarrDataType_uint8)
      .value("DTYPE_UINT16", ZarrDataType_uint16)
      .value("DTYPE_UINT32", ZarrDataType_uint32)
      .value("DTYPE_UINT64", ZarrDataType_uint64)
      .value("DTYPE_INT8", ZarrDataType_int8)
      .value("DTYPE_INT16", ZarrDataType_int16)
      .value("DTYPE_INT32", ZarrDataType_int32)
      .value("DTYPE_INT64", ZarrDataType_int64)
      .value("DTYPE_FLOAT32", ZarrDataType_float32)
      .value("DTYPE_FLOAT64", ZarrDataType_float64)
      .export_values();

    py::enum_<ZarrCompressor>(m, "Compressor")
      .value("COMPRESSOR_NONE", ZarrCompressor_None)
      .value("COMPRESSOR_BLOSC1", ZarrCompressor_Blosc1)
      .export_values();

    py::enum_<ZarrCompressionCodec>(m, "CompressionCodec")
      .value("COMPRESSION_NONE", ZarrCompressionCodec_None)
      .value("COMPRESSION_BLOSC_LZ4", ZarrCompressionCodec_BloscLZ4)
      .value("COMPRESSION_BLOSC_ZSTD", ZarrCompressionCodec_BloscZstd)
      .export_values();

    py::enum_<ZarrDimensionType>(m, "DimensionType")
      .value("DIMENSION_TYPE_SPACE", ZarrDimensionType_Space)
      .value("DIMENSION_TYPE_CHANNEL", ZarrDimensionType_Channel)
      .value("DIMENSION_TYPE_TIME", ZarrDimensionType_Time)
      .value("DIMENSION_TYPE_OTHER", ZarrDimensionType_Other)
      .export_values();

    py::class_<PyZarrS3Settings>(m, "S3Settings")
      .def(py::init<>())
      .def_property("endpoint",
                    &PyZarrS3Settings::endpoint,
                    &PyZarrS3Settings::set_endpoint)
      .def_property("bucket_name",
                    &PyZarrS3Settings::bucket_name,
                    &PyZarrS3Settings::set_bucket_name)
      .def_property("access_key_id",
                    &PyZarrS3Settings::access_key_id,
                    &PyZarrS3Settings::set_access_key_id)
      .def_property("secret_access_key",
                    &PyZarrS3Settings::secret_access_key,
                    &PyZarrS3Settings::set_secret_access_key);

    py::class_<PyZarrCompressionSettings>(m, "ZarrCompressionSettings")
      .def(py::init<>())
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

    py::class_<PyZarrDimensionProperties>(m, "ZarrDimensionProperties")
      .def(py::init<>())
      .def_property("name",
                    &PyZarrDimensionProperties::name,
                    &PyZarrDimensionProperties::set_name)
      .def_property("type",
                    &PyZarrDimensionProperties::type,
                    &PyZarrDimensionProperties::set_type)
      .def_property("array_size_px",
                    &PyZarrDimensionProperties::array_size_px,
                    &PyZarrDimensionProperties::set_array_size_px)
      .def_property("chunk_size_px",
                    &PyZarrDimensionProperties::chunk_size_px,
                    &PyZarrDimensionProperties::set_chunk_size_px)
      .def_property("shard_size_chunks",
                    &PyZarrDimensionProperties::shard_size_chunks,
                    &PyZarrDimensionProperties::set_shard_size_chunks);

    py::class_<PyZarrStreamSettings>(m, "ZarrStreamSettings")
      .def(py::init<>())
      .def_property("store_path",
                    &PyZarrStreamSettings::store_path,
                    &PyZarrStreamSettings::set_store_path)
      .def_property(
        "custom_metadata",
        [](const PyZarrStreamSettings& self) -> py::object {
            if (self.custom_metadata()) {
                return py::cast(*self.custom_metadata());
            }
            return py::none();
        },
        [](PyZarrStreamSettings& self, py::object obj) {
            if (obj.is_none()) {
                self.set_custom_metadata(std::nullopt);
            } else {
                self.set_custom_metadata(obj.cast<std::string>());
            }
        })
      .def_property(
        "s3",
        [](const PyZarrStreamSettings& self) -> py::object {
            if (self.s3()) {
                return py::cast(*self.s3());
            }
            return py::none();
        },
        [](PyZarrStreamSettings& self, py::object obj) {
            if (obj.is_none()) {
                self.set_s3(std::nullopt);
            } else {
                self.set_s3(obj.cast<PyZarrS3Settings>());
            }
        })
      .def_property(
        "compression",
        [](const PyZarrStreamSettings& self) -> py::object {
            if (self.compression()) {
                return py::cast(*self.compression());
            }
            return py::none();
        },
        [](PyZarrStreamSettings& self, py::object obj) {
            if (obj.is_none()) {
                self.set_compression(std::nullopt);
            } else {
                self.set_compression(obj.cast<PyZarrCompressionSettings>());
            }
        })
      .def_property("dimensions",
                    &PyZarrStreamSettings::dimensions,
                    &PyZarrStreamSettings::set_dimensions)
      .def_property("multiscale",
                    &PyZarrStreamSettings::multiscale,
                    &PyZarrStreamSettings::set_multiscale)
      .def_property("data_type",
                    &PyZarrStreamSettings::data_type,
                    &PyZarrStreamSettings::set_data_type)
      .def_property("version",
                    &PyZarrStreamSettings::version,
                    &PyZarrStreamSettings::set_version);

    py::class_<PyZarrStream>(m, "ZarrStream")
      .def(py::init<PyZarrStreamSettings>())
      .def("append", &PyZarrStream::append)
      .def("is_active", &PyZarrStream::is_active);
}
