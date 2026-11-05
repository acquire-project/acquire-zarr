#include "downsampler.hh"
#include "macros.hh"

#include <array>
#include <bit>
#include <cstdint>
#include <regex>
#include <type_traits>
#include <tuple>
#include <utility>

namespace {
ZarrDimension
downsample_dimension(const ZarrDimension& dim)
{
    // the smallest this can be is 1
    const uint32_t array_size_px =
      (dim.array_size_px + (dim.array_size_px % 2)) / 2;

    // chunk size is preserved regardless of array size: a chunk larger than
    // the array produces a single partial chunk, which is valid in Zarr v3
    const uint32_t chunk_size_px = dim.chunk_size_px;

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

/**
 * The mean of N integers, truncated toward zero as (a + b + ...) / N is, but
 * without overflowing T. 8- and 16-bit types sum in a 32-bit accumulator (N is
 * at most 4, so the sum fits) and 32-bit types in a 64-bit one; the narrower
 * accumulator lets the compiler use wider vector lanes. The 64-bit types sum
 * exactly in a two-word (hi:lo) two's-complement accumulator and divide the
 * magnitude by N, a power of two, with a shift, so there is no signed
 * division. MSVC Release builds computed the earlier quotient-and-remainder
 * form one too low on acquire-project/acquire-zarr#282; the cause was not
 * identified, and disabling vectorization did not help.
 */
template<typename T, size_t N>
T
integer_mean(const std::array<T, N>& values)
{
    static_assert(std::is_integral_v<T>);

    if constexpr (sizeof(T) < sizeof(int64_t)) {
        static_assert(N <= 4);
        using Wide = std::conditional_t<
          sizeof(T) <= 2,
          std::conditional_t<std::is_signed_v<T>, int32_t, uint32_t>,
          std::conditional_t<std::is_signed_v<T>, int64_t, uint64_t>>;
        Wide sum = 0;
        for (const T value : values) {
            sum += value;
        }
        return static_cast<T>(sum / static_cast<Wide>(N));
    } else {
        static_assert(N == 2 || N == 4);
        constexpr unsigned shift = N == 2 ? 1 : 2;

        uint64_t lo = 0;
        int64_t hi = 0;
        for (const T value : values) {
            const auto value_lo = static_cast<uint64_t>(value);
            int64_t value_hi = 0;
            if constexpr (std::is_signed_v<T>) {
                value_hi = value < 0 ? -1 : 0;
            }
            const uint64_t sum_lo = lo + value_lo;
            hi += value_hi + (sum_lo < lo ? 1 : 0);
            lo = sum_lo;
        }

        // shift the magnitude, so that the result truncates toward zero
        const bool negative = hi < 0;
        uint64_t magnitude_lo = lo, magnitude_hi = static_cast<uint64_t>(hi);
        if (negative) {
            magnitude_lo = ~magnitude_lo + 1;
            magnitude_hi = ~magnitude_hi + (magnitude_lo == 0 ? 1 : 0);
        }
        const uint64_t quotient =
          (magnitude_lo >> shift) | (magnitude_hi << (64 - shift));
        return negative ? static_cast<T>(0 - quotient)
                        : static_cast<T>(quotient);
    }
}

template<typename T>
T
mean4(const T& a, const T& b, const T& c, const T& d)
{
    if constexpr (std::is_integral_v<T>) {
        return integer_mean<T, 4>({ a, b, c, d });
    } else {
        return (a + b + c + d) / 4;
    }
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
mean2(const T& a, const T& b)
{
    if constexpr (std::is_integral_v<T>) {
        return integer_mean<T, 2>({ a, b });
    } else {
        return (a + b) / 2;
    }
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

/**
 * Reduce each 2x2 block of @p src to one pixel of @p dst. An odd final row or
 * column is paired with itself. @p reduce is a template parameter, not a
 * function pointer, so it inlines and the inner loop can vectorize.
 */
template<typename T, typename Reduce4>
void
scale_rows(const T* src, T* dst, size_t width, size_t height, Reduce4 reduce)
{
    const size_t width_out = (width + 1) / 2;
    const size_t full_pairs = width / 2;

    for (size_t row = 0; row < height; row += 2) {
        const T* top = src + row * width;
        const T* bottom = row + 1 < height ? top + width : top;
        T* out = dst + (row / 2) * width_out;

        for (size_t i = 0; i < full_pairs; ++i) {
            out[i] = reduce(
              top[2 * i], top[2 * i + 1], bottom[2 * i], bottom[2 * i + 1]);
        }
        if (full_pairs < width_out) {
            const size_t col = width - 1;
            out[full_pairs] =
              reduce(top[col], top[col], bottom[col], bottom[col]);
        }
    }
}

/**
 * Reduce each 2x2 block of a frame with the kernel @p Reduce. The kernel is a
 * template argument, so the call is direct and inlines; the Downsampler picks
 * one specialization per data type and method when it is constructed.
 */
template<typename T, T (*Reduce)(const T&, const T&, const T&, const T&)>
[[nodiscard]] ByteVector
scale_image(ConstByteSpan src, size_t& width, size_t& height)
{
    const auto bytes_of_src = src.size();
    const auto bytes_of_frame = width * height * sizeof(T);

    EXPECT(bytes_of_src >= bytes_of_frame,
           "Expecting at least ",
           bytes_of_frame,
           " bytes, got ",
           bytes_of_src);

    const size_t width_out = (width + 1) / 2;
    const size_t height_out = (height + 1) / 2;

    ByteVector dst(width_out * height_out * sizeof(T), 0);
    scale_rows(reinterpret_cast<const T*>(src.data()),
               reinterpret_cast<T*>(dst.data()),
               width,
               height,
               [](T a, T b, T c, T d) { return Reduce(a, b, c, d); });

    width = width_out;
    height = height_out;

    return dst;
}

void
expect_same_size(const ByteVector& dst, ConstByteSpan src)
{
    EXPECT(dst.size() == src.size(),
           "Expecting ",
           src.size(),
           " bytes in destination, got ",
           dst.size());
}

/**
 * Combine @p src into @p dst pixel by pixel with the kernel @p Reduce, a
 * template argument so that it inlines.
 */
template<typename T, T (*Reduce)(const T&, const T&)>
void
average_two_frames(ByteVector& dst, ConstByteSpan src)
{
    expect_same_size(dst, src);

    T* dst_as_T = reinterpret_cast<T*>(dst.data());
    const T* src_as_T = reinterpret_cast<const T*>(src.data());
    const auto num_pixels = src.size() / sizeof(T);
    for (size_t i = 0; i < num_pixels; ++i) {
        dst_as_T[i] = Reduce(dst_as_T[i], src_as_T[i]);
    }
}

/** Decimation keeps the earlier frame, which is already in @p dst. */
void
keep_earlier_frame(ByteVector& dst, ConstByteSpan src)
{
    expect_same_size(dst, src);
}

using ScaleFn = ByteVector (*)(ConstByteSpan, size_t&, size_t&);
using CombineFn = void (*)(ByteVector&, ConstByteSpan);

template<typename T>
std::pair<ScaleFn, CombineFn>
kernels_for(ZarrDownsamplingMethod method)
{
    switch (method) {
        case ZarrDownsamplingMethod_Decimate:
            return { scale_image<T, decimate4<T>>, keep_earlier_frame };
        case ZarrDownsamplingMethod_Mean:
            return { scale_image<T, mean4<T>>,
                     average_two_frames<T, mean2<T>> };
        case ZarrDownsamplingMethod_Min:
            return { scale_image<T, min4<T>>, average_two_frames<T, min2<T>> };
        case ZarrDownsamplingMethod_Max:
            return { scale_image<T, max4<T>>, average_two_frames<T, max2<T>> };
        default:
            throw std::runtime_error("Invalid downsampling method");
    }
}
} // namespace

zarr::Downsampler::Downsampler(std::shared_ptr<ArrayConfig> config,
                               ZarrDownsamplingMethod method)
{
    make_writer_configurations_(config);

    EXPECT(method > ZarrDownsamplingMethod_None,
           "Cannot construct downsampler with DownsamplingMethod: None");
    EXPECT(method < ZarrDownsamplingMethodCount,
           "Invalid downsampling method: ",
           static_cast<int>(method));

    switch (config->dtype) {
        case ZarrDataType_uint8:
            std::tie(scale_fun_, average2_fun_) = kernels_for<uint8_t>(method);
            break;
        case ZarrDataType_uint16:
            std::tie(scale_fun_, average2_fun_) = kernels_for<uint16_t>(method);
            break;
        case ZarrDataType_uint32:
            std::tie(scale_fun_, average2_fun_) = kernels_for<uint32_t>(method);
            break;
        case ZarrDataType_uint64:
            std::tie(scale_fun_, average2_fun_) = kernels_for<uint64_t>(method);
            break;
        case ZarrDataType_int8:
            std::tie(scale_fun_, average2_fun_) = kernels_for<int8_t>(method);
            break;
        case ZarrDataType_int16:
            std::tie(scale_fun_, average2_fun_) = kernels_for<int16_t>(method);
            break;
        case ZarrDataType_int32:
            std::tie(scale_fun_, average2_fun_) = kernels_for<int32_t>(method);
            break;
        case ZarrDataType_int64:
            std::tie(scale_fun_, average2_fun_) = kernels_for<int64_t>(method);
            break;
        case ZarrDataType_float32:
            std::tie(scale_fun_, average2_fun_) = kernels_for<float>(method);
            break;
        case ZarrDataType_float64:
            std::tie(scale_fun_, average2_fun_) = kernels_for<double>(method);
            break;
        default:
            throw std::runtime_error("Invalid data type: " +
                                     std::to_string(config->dtype));
    }

    method_ = method;
}

void
zarr::Downsampler::add_frame(std::vector<uint8_t>& frame)
{
    const auto& base_dims = writer_configurations_[0]->dimensions;
    size_t frame_width = base_dims->width_dim().array_size_px;
    size_t frame_height = base_dims->height_dim().array_size_px;
    ++level_frame_count_.at(0);

    ByteVector current_frame(frame.begin(), frame.end());
    ByteVector next_level_frame;

    for (auto level = 1; level < n_levels_(); ++level) {
        const auto& prev_dims = writer_configurations_[level - 1]->dimensions;
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
              scale_fun_(current_frame, frame_width, frame_height);
        } else {
            next_level_frame.assign(current_frame.begin(), current_frame.end());
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
                // swap here so that decimation keeps the earlier frame
                next_level_frame.swap(it->second);
                average2_fun_(next_level_frame, it->second);
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
}

bool
zarr::Downsampler::take_frame(int level, std::vector<uint8_t>& frame_data)
{
    if (const auto it = downsampled_frames_.find(level);
        it != downsampled_frames_.end()) {
        frame_data.swap(it->second);
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
    const uint32_t n_levels_x = n_chunks_x > 1 ? std::bit_width(n_chunks_x - 1) : 0;

    const auto array_size_y = base_dims->height_dim().array_size_px;
    const auto chunk_size_y = base_dims->height_dim().chunk_size_px;
    const auto n_chunks_y = (array_size_y + chunk_size_y - 1) / chunk_size_y;
    const uint32_t n_levels_y = n_chunks_y > 1 ? std::bit_width(n_chunks_y - 1) : 0;

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
        const uint32_t n_divs_z =
          n_chunks_z > 1 ? std::bit_width(n_chunks_z - 1) : 0;

        n_levels = std::max(n_levels_xy, n_divs_z);
    }

    if (config->max_levels > 0) {
        n_levels = std::min(n_levels, static_cast<uint32_t>(config->max_levels));
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
          prev_config->level_of_detail + 1,
          true);

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