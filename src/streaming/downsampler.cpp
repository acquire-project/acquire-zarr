#include "downsampler.hh"
#include "macros.hh"
#include "zarrv2.array.writer.hh"
#include "zarrv3.array.writer.hh"

namespace {
template<typename T>
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

    const int downscale = 2;
    constexpr auto bytes_of_type = sizeof(T);
    const uint32_t factor = 4;

    const auto w_pad = width + (width % downscale);
    const auto h_pad = height + (height % downscale);
    const auto size_downscaled = w_pad * h_pad * bytes_of_type / factor;

    ByteVector dst(size_downscaled, static_cast<std::byte>(0));
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

            dst_as_T[dst_idx++] = (T)((here + right + down + diag) / factor);
        }
    }

    width = w_pad / downscale;
    height = h_pad / downscale;

    return dst;
}

template<typename T>
void
average_two_frames(ByteVector& dst, ConstByteSpan src)
{
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
        dst_as_T[i] = static_cast<T>(0.5 * (dst_as_T[i] + src_as_T[i]));
    }
}
} // namespace

zarr::Downsampler::Downsampler(const ArrayWriterConfig& config)
{
    make_writer_configurations_(config);

    switch (config.dtype) {
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
                                     std::to_string(config.dtype));
    }
}

void
zarr::Downsampler::add_frame(ConstByteSpan frame_data)
{
    if (is_3d_downsample_()) {
        downsample_3d_(frame_data);
    } else {
        downsample_2d_(frame_data);
    }
}

bool
zarr::Downsampler::get_downsampled_frame(int level, ByteVector& frame_data)
{
    auto it = downsampled_frames_.find(level);
    if (it != downsampled_frames_.end()) {
        frame_data = it->second;
        downsampled_frames_.erase(level);
        return true;
    }

    return false;
}

const std::unordered_map<int, zarr::ArrayWriterConfig>&
zarr::Downsampler::writer_configurations() const
{
    return writer_configurations_;
}

bool
zarr::Downsampler::is_3d_downsample_() const
{
    // the width and depth dimensions are always spatial -- if the 3rd dimension
    // is also spatial and nontrivial, then we downsample in 3 dimensions
    const auto& dims = writer_configurations_.at(0).dimensions;
    const auto ndims = dims->ndims();

    const auto& third_dim = dims->at(ndims - 3);
    return third_dim.type == ZarrDimensionType_Space &&
           third_dim.array_size_px > 1;
}

size_t
zarr::Downsampler::n_levels_() const
{
    return writer_configurations_.size();
}

void
zarr::Downsampler::make_writer_configurations_(const ArrayWriterConfig& config)
{
    writer_configurations_.insert({ config.level_of_detail, config });

    const auto ndims = config.dimensions->ndims();

    auto cur_config = config;
    bool do_downsample = true;
    while (do_downsample) {
        const auto& dims = cur_config.dimensions;

        // downsample the final 3 dimensions
        std::vector<ZarrDimension> down_dims(ndims);
        for (auto i = 0; i < ndims; ++i) {
            const auto& dim = dims->at(i);
            if (i < ndims - 3 || dim.type != ZarrDimensionType_Space) {
                down_dims[i] = dim;
                continue;
            }

            const uint32_t array_size_px =
              (dim.array_size_px + (dim.array_size_px % 2)) / 2;

            const uint32_t chunk_size_px =
              dim.array_size_px == 0
                ? dim.chunk_size_px
                : std::min(dim.chunk_size_px, array_size_px);

            CHECK(chunk_size_px);
            const uint32_t n_chunks =
              (array_size_px + chunk_size_px - 1) / chunk_size_px;

            const uint32_t shard_size_chunks =
              dim.array_size_px == 0
                ? 1
                : std::min(n_chunks, dim.shard_size_chunks);

            down_dims[i] = { dim.name,
                             dim.type,
                             array_size_px,
                             chunk_size_px,
                             shard_size_chunks };
        }

        auto down_config = cur_config;
        down_config.dimensions = std::make_shared<ArrayDimensions>(
          std::move(down_dims), cur_config.dtype);

        ++down_config.level_of_detail;

        // can we downsample down_config?
        for (auto i = 0; i < ndims; ++i) {
            // downsampling made the chunk size strictly smaller
            const auto& dim = cur_config.dimensions->at(i);
            const auto& downsampled_dim = down_config.dimensions->at(i);

            if (dim.chunk_size_px > downsampled_dim.chunk_size_px) {
                do_downsample = false;
                break;
            }
        }

        writer_configurations_.emplace(down_config.level_of_detail,
                                       down_config);

        cur_config = down_config;
    }
}

void
zarr::Downsampler::downsample_3d_(ConstByteSpan frame_data)
{
    const auto& dims = writer_configurations_[0].dimensions;
    size_t frame_width = dims->width_dim().array_size_px;
    size_t frame_height = dims->height_dim().array_size_px;

    ConstByteSpan data = frame_data;
    ByteVector downsampled;
    for (auto i = 1; i < n_levels_(); ++i) {
        downsampled = scale_fun_(data, frame_width, frame_height);
        auto it = partial_scaled_frames_.find(i);
        if (it != partial_scaled_frames_.end()) {
            // downsampled is the new frame
            average2_fun_(downsampled, it->second);
            downsampled_frames_.emplace(i, downsampled);

            // clean up this LOD
            partial_scaled_frames_.erase(it);

            // set up for next iteration
            if (i + 1 < writer_configurations_.size()) {
                data = downsampled;
            }
        } else {
            partial_scaled_frames_.emplace(i, downsampled);
            break;
        }
    }
}

void
zarr::Downsampler::downsample_2d_(ConstByteSpan frame_data)
{
    const auto& dims = writer_configurations_[0].dimensions;
    size_t frame_width = dims->width_dim().array_size_px;
    size_t frame_height = dims->height_dim().array_size_px;

    ConstByteSpan data = frame_data;
    ByteVector downsampled;
    for (auto i = 1; i < n_levels_(); ++i) {
        downsampled = scale_fun_(data, frame_width, frame_height);
        downsampled_frames_.emplace(i, downsampled);
        data = downsampled;
    }
}