#include <iostream>
#include <memory>

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "acquire.zarr.h"

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

    std::string repr() const
    {
        auto secret_access_key = secret_access_key_.size() < 6
                                   ? secret_access_key_
                                   : secret_access_key_.substr(0, 5) + "...";

        return "S3Settings(endpoint='" + endpoint_ + "', bucket_name='" +
               bucket_name_ + "', access_key_id='" + access_key_id_ +
               "', secret_access_key='" + secret_access_key + "')";
    }

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
    uint8_t level_{ 0 };
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

    uint32_t array_size_px() const { return array_size_px_; }
    void set_array_size_px(uint32_t size) { array_size_px_ = size; }

    uint32_t chunk_size_px() const { return chunk_size_px_; }
    void set_chunk_size_px(uint32_t size) { chunk_size_px_ = size; }

    uint32_t shard_size_chunks() const { return shard_size_chunks_; }
    void set_shard_size_chunks(uint32_t size) { shard_size_chunks_ = size; }

    std::string repr() const
    {
        return "Dimension(name='" + name_ + "', kind=DimensionType." +
               std::string(dimension_type_to_str(type_)) +
               ", array_size_px=" + std::to_string(array_size_px_) +
               ", chunk_size_px=" + std::to_string(chunk_size_px_) +
               ", shard_size_chunks=" + std::to_string(shard_size_chunks_) +
               ")";
    }

  private:
    std::string name_;
    ZarrDimensionType type_{ ZarrDimensionType_Space };
    uint32_t array_size_px_{ 0 };
    uint32_t chunk_size_px_{ 0 };
    uint32_t shard_size_chunks_{ 0 };
};

PYBIND11_MAKE_OPAQUE(std::vector<PyZarrDimensionProperties>);

class PyZarrStreamSettings
{
  public:
    PyZarrStreamSettings() = default;
    ~PyZarrStreamSettings() = default;

    std::vector<PyZarrDimensionProperties> dimensions;

    const std::string& store_path() const { return store_path_; }
    void set_store_path(const std::string& path) { store_path_ = path; }

    const std::optional<std::string>& custom_metadata() const
    {
        return custom_metadata_;
    }
    void set_custom_metadata(const std::optional<std::string>& metadata)
    {
        custom_metadata_ = metadata;
    }

    const std::optional<PyZarrS3Settings>& s3() const { return s3_settings_; }
    void set_s3(const std::optional<PyZarrS3Settings>& settings)
    {
        s3_settings_ = settings;
    }

    const std::optional<PyZarrCompressionSettings>& compression() const
    {
        return compression_settings_;
    }
    void set_compression(
      const std::optional<PyZarrCompressionSettings>& settings)
    {
        compression_settings_ = settings;
    }

    bool multiscale() const { return multiscale_; }
    void set_multiscale(bool multiscale) { multiscale_ = multiscale; }

    ZarrDataType data_type() const { return data_type_; }
    void set_data_type(ZarrDataType type) { data_type_ = type; }

    ZarrVersion version() const { return version_; }
    void set_version(ZarrVersion version) { version_ = version; }

  private:
    std::string store_path_;
    std::optional<std::string> custom_metadata_{ std::nullopt };
    std::optional<PyZarrS3Settings> s3_settings_{ std::nullopt };
    std::optional<PyZarrCompressionSettings> compression_settings_{
        std::nullopt
    };
    bool multiscale_ = false;
    ZarrDataType data_type_{ ZarrDataType_uint8 };
    ZarrVersion version_{ ZarrVersion_2 };
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

        store_path_ = settings.store_path();
        stream_settings.store_path = store_path_.c_str();

        if (settings.custom_metadata()) {
            custom_metadata_ = settings.custom_metadata().value();
            stream_settings.custom_metadata = custom_metadata_.c_str();
        }

        if (settings.s3().has_value()) {
            const auto& s3 = settings.s3().value();
            s3_endpoint_ = s3.endpoint();
            s3_settings.endpoint = s3_endpoint_.c_str();

            s3_bucket_name_ = s3.bucket_name();
            s3_settings.bucket_name = s3_bucket_name_.c_str();

            s3_access_key_id_ = s3.access_key_id();
            s3_settings.access_key_id = s3_access_key_id_.c_str();

            s3_secret_access_key_ = s3.secret_access_key();
            s3_settings.secret_access_key = s3_secret_access_key_.c_str();

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

        const auto& dims = settings.dimensions;
        dimension_names_.resize(dims.size());

        std::vector<ZarrDimensionProperties> dimension_props;
        for (auto i = 0; i < dims.size(); ++i) {
            const auto& dim = dims[i];
            dimension_names_[i] = dim.name();
            ZarrDimensionProperties properties{
                .name = dimension_names_[i].c_str(),
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

    std::string store_path_;
    std::string custom_metadata_;

    std::vector<std::string> dimension_names_;

    std::string s3_endpoint_;
    std::string s3_bucket_name_;
    std::string s3_access_key_id_;
    std::string s3_secret_access_key_;
};

PYBIND11_MODULE(acquire_zarr, m)
{
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

    py::enum_<ZarrLogLevel>(m, "LogLevel")
      .value(log_level_to_str(ZarrLogLevel_Debug), ZarrLogLevel_Debug)
      .value(log_level_to_str(ZarrLogLevel_Info), ZarrLogLevel_Info)
      .value(log_level_to_str(ZarrLogLevel_Warning), ZarrLogLevel_Warning)
      .value(log_level_to_str(ZarrLogLevel_Error), ZarrLogLevel_Error)
      .value(log_level_to_str(ZarrLogLevel_None), ZarrLogLevel_None);

    py::class_<PyZarrS3Settings>(m, "S3Settings", py::dynamic_attr())
      .def(py::init([](py::kwargs kwargs) {
          PyZarrS3Settings settings;
          if (kwargs.contains("endpoint"))
              settings.set_endpoint(kwargs["endpoint"].cast<std::string>());
          if (kwargs.contains("bucket_name"))
              settings.set_bucket_name(
                kwargs["bucket_name"].cast<std::string>());
          if (kwargs.contains("access_key_id"))
              settings.set_access_key_id(
                kwargs["access_key_id"].cast<std::string>());
          if (kwargs.contains("secret_access_key"))
              settings.set_secret_access_key(
                kwargs["secret_access_key"].cast<std::string>());
          return settings;
      }))
      .def("__repr__", [](const PyZarrS3Settings& self) { return self.repr(); })
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

    py::class_<PyZarrCompressionSettings>(
      m, "CompressionSettings", py::dynamic_attr())
      .def(py::init([](py::kwargs kwargs) {
          PyZarrCompressionSettings settings;
          if (kwargs.contains("compressor"))
              settings.set_compressor(
                kwargs["compressor"].cast<ZarrCompressor>());
          if (kwargs.contains("codec"))
              settings.set_codec(kwargs["codec"].cast<ZarrCompressionCodec>());
          if (kwargs.contains("level"))
              settings.set_level(kwargs["level"].cast<uint8_t>());
          if (kwargs.contains("shuffle"))
              settings.set_shuffle(kwargs["shuffle"].cast<uint8_t>());
          return settings;
      }))
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
      .def(py::init([](py::kwargs kwargs) {
          PyZarrDimensionProperties props;
          if (kwargs.contains("name"))
              props.set_name(kwargs["name"].cast<std::string>());
          if (kwargs.contains("kind"))
              props.set_type(kwargs["kind"].cast<ZarrDimensionType>());
          if (kwargs.contains("array_size_px"))
              props.set_array_size_px(kwargs["array_size_px"].cast<uint32_t>());
          if (kwargs.contains("chunk_size_px"))
              props.set_chunk_size_px(kwargs["chunk_size_px"].cast<uint32_t>());
          if (kwargs.contains("shard_size_chunks"))
              props.set_shard_size_chunks(
                kwargs["shard_size_chunks"].cast<uint32_t>());
          return props;
      }))
      .def("__repr__",
           [](const PyZarrDimensionProperties& self) { return self.repr(); })
      .def_property("name",
                    &PyZarrDimensionProperties::name,
                    &PyZarrDimensionProperties::set_name)
      .def_property("kind",
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

    py::class_<PyZarrStreamSettings>(m, "StreamSettings", py::dynamic_attr())
      .def(py::init([](py::kwargs kwargs) {
          PyZarrStreamSettings settings;

          if (kwargs.contains("store_path"))
              settings.set_store_path(kwargs["store_path"].cast<std::string>());

          if (kwargs.contains("custom_metadata") &&
              !kwargs["custom_metadata"].is_none()) {
              auto cm = kwargs["custom_metadata"].cast<std::string>();
              settings.set_custom_metadata(cm);
          }

          if (kwargs.contains("s3") && !kwargs["s3"].is_none()) {
              auto s3 = kwargs["s3"].cast<PyZarrS3Settings>();
              settings.set_s3(s3);
          }

          if (kwargs.contains("compression") &&
              !kwargs["compression"].is_none()) {
              auto compression =
                kwargs["compression"].cast<PyZarrCompressionSettings>();
              settings.set_compression(compression);
          }

          if (kwargs.contains("dimensions"))
              settings.dimensions =
                kwargs["dimensions"]
                  .cast<std::vector<PyZarrDimensionProperties>>();

          if (kwargs.contains("multiscale"))
              settings.set_multiscale(kwargs["multiscale"].cast<bool>());

          if (kwargs.contains("data_type"))
              settings.set_data_type(kwargs["data_type"].cast<ZarrDataType>());

          if (kwargs.contains("version"))
              settings.set_version(kwargs["version"].cast<ZarrVersion>());

          return settings;
      }))
      .def("__repr__",
           [](const PyZarrStreamSettings& self) {
               std::string repr =
                 "StreamSettings(store_path='" + self.store_path() + "'";
               if (self.custom_metadata().has_value()) {
                   repr += ", custom_metadata='" +
                           self.custom_metadata().value() + "'";
               }

               if (self.s3().has_value()) {
                   repr += ", s3=" + self.s3()->repr();
               }
               if (self.compression().has_value()) {
                   repr += ", compression=" + self.compression()->repr();
               }
               repr += ", dimensions=[";
               for (const auto& dim : self.dimensions) {
                   repr += dim.repr() + ", ";
               }

               std::string multiscale = self.multiscale() ? "True" : "False";
               repr +=
                 "], multiscale=" + multiscale + ", data_type=DataType." +
                 std::string(data_type_to_str(self.data_type())) +
                 ", version=ZarrVersion." +
                 std::string(self.version() == ZarrVersion_2 ? "V2" : "V3") +
                 ")";
               return repr;
           })
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
      .def_readwrite("dimensions", &PyZarrStreamSettings::dimensions)
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
