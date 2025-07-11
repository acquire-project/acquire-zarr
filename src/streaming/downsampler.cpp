#include "downsampler.hh"
#include "macros.hh"

#include <bit>
#include <regex>

namespace {
ZarrDimension
downsample_dimension(const ZarrDimension& dim)
{
    // the smallest this can be is 1
    const uint32_t array_size_px =
      (dim.array_size_px + (dim.array_size_px % 2)) / 2;

    // the smallest this can be is 1
    const uint32_t chunk_size_px = std::min(dim.chunk_size_px, array_size_px);

    // the smallest this can be is also 1
    const uint32_t n_chunks =
      (array_size_px + chunk_size_px - 1) / chunk_size_px;

    const uint32_t shard_size_chunks =
      std::min(n_chunks, dim.shard_size_chunks);

    std::string unit = dim.unit.has_value() ? *dim.unit : "";

    double scale = dim.scale * 2.0;

    return ZarrDimension(dim.name,
                         dim.type,
                         array_size_px,
                         chunk_size_px,
                         shard_size_chunks,
                         unit,
                         scale);
}

template<typename T>
T
decimate4(const T& a, const T& b, const T& c, const T& d)
{
    return a;
}

template<typename T>
T
mean4(const T& a, const T& b, const T& c, const T& d)
{
    return (a + b + c + d) / 4;
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::T
mean4(const T& a, const T& b, const T& c, const T& d)
{
    T mask = 3;
    T result = a / 4 + b / 4 + c / 4 + d / 4;
    T remainder = ((a & mask) + (b & mask) + (c & mask) + (d & mask)) / 4;

    return result + remainder;
}

template<typename T>
T
min4(const T& a, const T& b, const T& c, const T& d)
{
    T val = a;
    if (b < val) {
        val = b;
    }
    if (c < val) {
        val = c;
    }
    if (d < val) {
        val = d;
    }

    return val;
}

template<typename T>
T
max4(const T& a, const T& b, const T& c, const T& d)
{
    T val = a;
    if (b > val) {
        val = b;
    }
    if (c > val) {
        val = c;
    }
    if (d > val) {
        val = d;
    }

    return val;
}

template<typename T>
T
decimate2(const T& a, const T& b)
{
    return a;
}

template<typename T>
T
mean2(const T& a, const T& b)
{
    return (a + b) / 2;
}

template<typename T>
typename std::enable_if<std::is_integral<T>::value, T>::T
mean2(const T& a, const T& b)
{
    T mask = 3;
    T result = a / 2 + b / 2;
    T remainder = ((a & mask) + (b & mask)) / 2;

    return result + remainder;
}

template<typename T>
T
min2(const T& a, const T& b)
{
    return a < b ? a : b;
}

template<typename T>
T
max2(const T& a, const T& b)
{
    return a > b ? a : b;
}

template<typename T>
[[nodiscard]] ByteVector
scale_image(ConstByteSpan src,
            size_t& width,
            size_t& height,
            ZarrDownsamplingMethod method)
{
    T (*scale_fun)(const T&, const T&, const T&, const T&) = nullptr;
    switch (method) {
        case ZarrDownsamplingMethod_Decimate:
            scale_fun = decimate4<T>;
            break;
        case ZarrDownsamplingMethod_Mean:
            scale_fun = mean4<T>;
            break;
        case ZarrDownsamplingMethod_Min:
            scale_fun = min4<T>;
            break;
        case ZarrDownsamplingMethod_Max:
            scale_fun = max4<T>;
            break;
        default:
            throw std::runtime_error("Invalid downsampling method");
    }

    const auto bytes_of_src = src.size();
    const auto bytes_of_frame = width * height * sizeof(T);

    EXPECT(bytes_of_src >= bytes_of_frame,
           "Expecting at least ",
           bytes_of_frame,
           " bytes, got ",
           bytes_of_src);

    const int downscale = 2;
    constexpr auto bytes_of_type = sizeof(T);
    const uint32_t factor = 4;

    const auto w_pad = width + (width % downscale);
    const auto h_pad = height + (height % downscale);
    const auto size_downscaled = w_pad * h_pad * bytes_of_type / factor;

    ByteVector dst(size_downscaled, 0);
    auto* dst_as_T = reinterpret_cast<T*>(dst.data());
    auto* src_as_T = reinterpret_cast<const T*>(src.data());

    size_t dst_idx = 0;
    for (auto row = 0; row < height; row += downscale) {
        const bool pad_height = (row == height - 1 && height != h_pad);

        for (auto col = 0; col < width; col += downscale) {
            size_t src_idx = row * width + col;
            const bool pad_width = (col == width - 1 && width != w_pad);

            T here = src_as_T[src_idx];
            T right = src_as_T[src_idx + !pad_width];
            T down = src_as_T[src_idx + width * (!pad_height)];
            T diag = src_as_T[src_idx + width * (!pad_height) + (!pad_width)];

            dst_as_T[dst_idx++] = scale_fun(here, right, down, diag);
        }
    }

    width = w_pad / downscale;
    height = h_pad / downscale;

    return dst;
}

template<typename T>
void
average_two_frames(ByteVector& dst,
                   ConstByteSpan src,
                   ZarrDownsamplingMethod method)
{
    T (*average_fun)(const T&, const T&) = nullptr;
    switch (method) {
        case ZarrDownsamplingMethod_Decimate:
            average_fun = decimate2<T>;
            break;
        case ZarrDownsamplingMethod_Mean:
            average_fun = mean2<T>;
            break;
        case ZarrDownsamplingMethod_Min:
            average_fun = min2<T>;
            break;
        case ZarrDownsamplingMethod_Max:
            average_fun = max2<T>;
            break;
        default:
            throw std::runtime_error("Invalid downsampling method");
    }

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
        dst_as_T[i] = average_fun(dst_as_T[i], src_as_T[i]);
    }
}
} // namespace

zarr::Downsampler::Downsampler(std::shared_ptr<ArrayConfig> config,
                               ZarrDownsamplingMethod method)
{
    make_writer_configurations_(config);

    switch (config->dtype) {
        case ZarrDataType_uint8:
            scale_fun_ = scale_image<uint8_t>;
            average2_fun_ = average_two_frames<uint8_t>;
            break;
        case ZarrDataType_uint16:
            scale_fun_ = scale_image<uint16_t>;
            average2_fun_ = average_two_frames<uint16_t>;
            break;
        case ZarrDataType_uint32:
            scale_fun_ = scale_image<uint32_t>;
            average2_fun_ = average_two_frames<uint32_t>;
            break;
        case ZarrDataType_uint64:
            scale_fun_ = scale_image<uint64_t>;
            average2_fun_ = average_two_frames<uint64_t>;
            break;
        case ZarrDataType_int8:
            scale_fun_ = scale_image<int8_t>;
            average2_fun_ = average_two_frames<int8_t>;
            break;
        case ZarrDataType_int16:
            scale_fun_ = scale_image<int16_t>;
            average2_fun_ = average_two_frames<int16_t>;
            break;
        case ZarrDataType_int32:
            scale_fun_ = scale_image<int32_t>;
            average2_fun_ = average_two_frames<int32_t>;
            break;
        case ZarrDataType_int64:
            scale_fun_ = scale_image<int64_t>;
            average2_fun_ = average_two_frames<int64_t>;
            break;
        case ZarrDataType_float32:
            scale_fun_ = scale_image<float>;
            average2_fun_ = average_two_frames<float>;
            break;
        case ZarrDataType_float64:
            scale_fun_ = scale_image<double>;
            average2_fun_ = average_two_frames<double>;
            break;
        default:
            throw std::runtime_error("Invalid data type: " +
                                     std::to_string(config->dtype));
    }

    EXPECT(method < ZarrDownsamplingMethodCount,
           "Invalid downsampling method: ",
           static_cast<int>(method));
    method_ = method;
}

void
zarr::Downsampler::add_frame(LockedBuffer& frame)
{
    const auto& base_dims = writer_configurations_[0]->dimensions;
    size_t frame_width = base_dims->width_dim().array_size_px;
    size_t frame_height = base_dims->height_dim().array_size_px;

    frame.with_lock([&](const auto& data) {
        ByteVector current_frame(data.begin(), data.end());
        ByteVector next_level_frame;

        for (auto level = 1; level < n_levels_(); ++level) {
            const auto& prev_dims =
              writer_configurations_[level - 1]->dimensions;
            const auto prev_width = prev_dims->width_dim().array_size_px;
            const auto prev_height = prev_dims->height_dim().array_size_px;
            const auto prev_planes =
              prev_dims->at(prev_dims->ndims() - 3).array_size_px;

            EXPECT(prev_width == frame_width && prev_height == frame_height,
                   "Frame dimensions do not match expected dimensions: ",
                   prev_width,
                   "x",
                   prev_height,
                   " vs. ",
                   frame_width,
                   "x",
                   frame_height);

            const auto& next_dims = writer_configurations_[level]->dimensions;
            const auto next_width = next_dims->width_dim().array_size_px;
            const auto next_height = next_dims->height_dim().array_size_px;
            const auto next_planes =
              next_dims->at(next_dims->ndims() - 3).array_size_px;

            // only downsample if this level's XY size is smaller than the last
            if (next_width < prev_width || next_height < prev_height) {
                next_level_frame =
                  scale_fun_(current_frame, frame_width, frame_height, method_);
            } else {
                next_level_frame.assign(current_frame.begin(),
                                        current_frame.end());
            }

            EXPECT(next_width == frame_width && next_height == frame_height,
                   "Downsampled dimensions do not match expected dimensions: ",
                   next_width,
                   "x",
                   next_height,
                   " vs. ",
                   frame_width,
                   "x",
                   frame_height);

            // if the Z dimension is spatial, and has an odd number of planes,
            // and this is the last plane, we don't want to queue it up to be
            // averaged with the first frame of the next timepoint
            bool average_this_frame = next_planes < prev_planes;
            if (prev_planes % 2 != 0 &&
                level_frame_count_.at(level - 1) % prev_planes == 0) {
                average_this_frame = false;
            }

            // only average if this level's Z size is smaller than the last
            // and if we are not at the last frame of the previous level
            if (average_this_frame) {
                auto it = partial_scaled_frames_.find(level);
                if (it != partial_scaled_frames_.end()) {
                    // average2_fun_ writes to next_level_frame
                    // swap here so that decimate2 can take it->second
                    next_level_frame.swap(it->second);
                    average2_fun_(next_level_frame, it->second, method_);
                    emplace_downsampled_frame_(level, next_level_frame);

                    // clean up this LOD
                    partial_scaled_frames_.erase(it);

                    // set up for next iteration
                    if (level + 1 < writer_configurations_.size()) {
                        current_frame.assign(next_level_frame.begin(),
                                             next_level_frame.end());
                    }
                } else {
                    partial_scaled_frames_.emplace(level, next_level_frame);
                    break;
                }
            } else {
                // no downsampling in Z, so we can just pass the data to the
                // next level
                emplace_downsampled_frame_(level, next_level_frame);

                if (level + 1 < writer_configurations_.size()) {
                    current_frame.assign(next_level_frame.begin(),
                                         next_level_frame.end());
                }
            }
        }
    });
}

bool
zarr::Downsampler::take_frame(int level, LockedBuffer& frame_data)
{
    auto it = downsampled_frames_.find(level);
    if (it != downsampled_frames_.end()) {
        frame_data.assign(it->second);
        downsampled_frames_.erase(level);
        return true;
    }

    return false;
}

const std::unordered_map<int, std::shared_ptr<zarr::ArrayConfig>>&
zarr::Downsampler::writer_configurations() const
{
    return writer_configurations_;
}

std::string
zarr::Downsampler::downsampling_method() const
{
    switch (method_) {
        case ZarrDownsamplingMethod_Decimate:
            return "decimate";
        case ZarrDownsamplingMethod_Mean:
            return "local_mean";
        case ZarrDownsamplingMethod_Min:
            return "local_min";
        case ZarrDownsamplingMethod_Max:
            return "local_max";
        default:
            throw std::runtime_error("Invalid downsampling method: " +
                                     std::to_string(method_));
    }
}

nlohmann::json
zarr::Downsampler::get_metadata() const
{
    nlohmann::json metadata;
    switch (method_) {
        case ZarrDownsamplingMethod_Mean:
            metadata["description"] =
              "The fields in the metadata describe how to reproduce this "
              "multiscaling in scikit-image. The method and its parameters "
              "are given here.";
            metadata["method"] = "skimage.transform.downscale_local_mean";
            metadata["version"] = "0.25.2";
            metadata["kwargs"] = { { "factors", "(2, 2)" }, { "cval", "0" } };
            break;
        case ZarrDownsamplingMethod_Decimate:
            metadata["description"] =
              "Subsampling by taking every 2nd pixel/voxel (top-left corner of "
              "each 2x2 block). "
              "Equivalent to numpy array slicing with stride 2.";
            metadata["method"] = "np.ndarray.__getitem__";
            metadata["version"] = "2.2.6";
            metadata["args"] = { "(slice(0, None, 2), slice(0, None, 2))" };
            break;
        case ZarrDownsamplingMethod_Min:
            metadata["description"] =
              "Minimum pooling over 2x2 blocks. Equivalent to reshaping into "
              "blocks and taking numpy.min along block dimensions.";
            metadata["method"] = "skimage.measure.block_reduce";
            metadata["version"] = "0.25.2";
            metadata["kwargs"] = { { "func", "np.min" } };
            break;
        case ZarrDownsamplingMethod_Max:
            metadata["description"] =
              "Maximum pooling over 2x2 blocks. Equivalent to reshaping into "
              "blocks and taking numpy.max along block dimensions.";
            metadata["method"] = "skimage.measure.block_reduce";
            metadata["version"] = "0.25.2";
            metadata["kwargs"] = { { "func", "np.max" } };
            break;
        default:
            throw std::runtime_error("Invalid downsampling method: " +
                                     std::to_string(method_));
    }

    return metadata;
}

size_t
zarr::Downsampler::n_levels_() const
{
    return writer_configurations_.size();
}

void
zarr::Downsampler::make_writer_configurations_(
  std::shared_ptr<ArrayConfig> config)
{
    EXPECT(config, "Null pointer: config");
    EXPECT(config->node_key.ends_with("/0"),
           "Invalid node key: '",
           config->node_key,
           "'");
    EXPECT(config->level_of_detail == 0,
           "Invalid level of detail: ",
           config->level_of_detail);

    writer_configurations_.emplace(0, config);
    level_frame_count_.emplace(0, 0);

    const std::shared_ptr<ArrayDimensions>& base_dims = config->dimensions;
    const auto ndims = config->dimensions->ndims();

    const auto array_size_x = base_dims->width_dim().array_size_px;
    const auto chunk_size_x = base_dims->width_dim().chunk_size_px;
    const auto n_chunks_x = (array_size_x + chunk_size_x - 1) / chunk_size_x;
    const auto n_levels_x = n_chunks_x > 1 ? std::bit_width(n_chunks_x - 1) : 0;

    const auto array_size_y = base_dims->height_dim().array_size_px;
    const auto chunk_size_y = base_dims->height_dim().chunk_size_px;
    const auto n_chunks_y = (array_size_y + chunk_size_y - 1) / chunk_size_y;
    const auto n_levels_y = n_chunks_y > 1 ? std::bit_width(n_chunks_y - 1) : 0;

    // assume isotropic downsampling, so the number of levels is the same in
    // both
    const auto n_levels_xy = std::min(n_levels_x, n_levels_y);
    auto n_levels = n_levels_xy;

    if (base_dims->at(ndims - 3).type == ZarrDimensionType_Space) {
        // if the 3rd dimension is spatial, we can downsample it as well
        const auto array_size_z = base_dims->at(ndims - 3).array_size_px;
        const auto chunk_size_z = base_dims->at(ndims - 3).chunk_size_px;
        const auto n_chunks_z =
          (array_size_z + chunk_size_z - 1) / chunk_size_z;
        const auto n_divs_z =
          n_chunks_z > 1 ? std::bit_width(n_chunks_z - 1) : 0;

        n_levels = std::max(n_levels_xy, n_divs_z);
    }

    for (auto level = 1; level <= n_levels; ++level) {
        const auto& prev_config = writer_configurations_.at(level - 1);
        const auto& prev_dims = prev_config->dimensions;

        std::vector<ZarrDimension> down_dims(ndims);

        // we don't downsample these dimensions, so just copy them
        for (auto i = 0; i < ndims - 3; ++i) {
            down_dims[i] = prev_dims->at(i);
        }

        const auto& z_dim = prev_dims->at(ndims - 3);
        if (z_dim.type == ZarrDimensionType_Space &&
            z_dim.array_size_px > z_dim.chunk_size_px) {
            down_dims[ndims - 3] = downsample_dimension(z_dim);
        } else {
            // not spatial or fully downsampled, so we just copy it
            down_dims[ndims - 3] = z_dim;
        }

        const auto& y_dim = prev_dims->height_dim();
        const auto& x_dim = prev_dims->width_dim();

        if (std::min(y_dim.array_size_px, x_dim.array_size_px) >
            std::max(y_dim.chunk_size_px, x_dim.chunk_size_px)) {
            // downsample the final 2 dimensions
            down_dims[ndims - 2] = downsample_dimension(y_dim);
            down_dims[ndims - 1] = downsample_dimension(x_dim);
        } else {
            // not spatial or fully downsampled, so we just copy them
            down_dims[ndims - 2] = y_dim;
            down_dims[ndims - 1] = x_dim;
        }

        auto down_config = std::make_shared<ArrayConfig>(
          prev_config->store_root,
          // the new node key has the same parent as the current, but
          // substitutes the current level of detail with the new one
          std::regex_replace(prev_config->node_key,
                             std::regex("(\\d+)$"),
                             std::to_string(prev_config->level_of_detail + 1)),
          prev_config->bucket_name,
          prev_config->compression_params,
          std::make_shared<ArrayDimensions>(std::move(down_dims),
                                            prev_config->dtype),
          prev_config->dtype,
          prev_config->downsampling_method,
          prev_config->level_of_detail + 1);

        writer_configurations_.emplace(down_config->level_of_detail,
                                       down_config);

        level_frame_count_.emplace(level, 0);
    }
}

void
zarr::Downsampler::emplace_downsampled_frame_(int level,
                                              const ByteVector& frame_data)
{
    downsampled_frames_.emplace(level, frame_data);
    ++level_frame_count_.at(level);
}