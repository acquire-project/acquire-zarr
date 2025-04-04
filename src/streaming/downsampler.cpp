#include "downsampler.hh"

zarr::Downsampler::Downsampler(
  const ArrayWriterConfig& config,
  std::shared_ptr<ThreadPool> thread_pool,
  std::shared_ptr<S3ConnectionPool> s3_connection_pool)
  : array_writer_config_(config)
  , thread_pool_(thread_pool)
  , s3_connection_pool_(s3_connection_pool)
{
}

bool
zarr::Downsampler::is_3d_downsample_() const
{
    // the width and depth dimensions are always spatial -- if the 3rd dimension
    // is also spatial, then we downsample in 3 dimensions
    const auto& dims = array_writer_config_.dimensions;
    const auto ndims = dims->ndims();

    return dims->at(ndims - 3).type == ZarrDimensionType_Space;
}

std::vector<zarr::ArrayWriterConfig>
zarr::Downsampler::make_downsampled_configs_()
{
    const auto& dims = array_writer_config_.dimensions;
    const auto ndims = dims->ndims();

    // downsample dimensions
    std::vector<ZarrDimension> downsampled_dims(ndims);
    for (auto i = 0; i < ndims; ++i) {
        const auto& dim = dims->at(i);
        // don't downsample channels
        if (dim.type == ZarrDimensionType_Channel) {
            downsampled_dims[i] = dim;
        } else {
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

            downsampled_dims[i] = { dim.name,
                                    dim.type,
                                    array_size_px,
                                    chunk_size_px,
                                    shard_size_chunks };
        }
    }
    downsampled_config.dimensions = std::make_shared<ArrayDimensions>(
      std::move(downsampled_dims), config.dtype);

    downsampled_config.level_of_detail = config.level_of_detail + 1;
    downsampled_config.bucket_name = config.bucket_name;
    downsampled_config.store_path = config.store_path;

    downsampled_config.dtype = config.dtype;

    // copy the Blosc compression parameters
    downsampled_config.compression_params = config.compression_params;

    // can we downsample downsampled_config?
    for (auto i = 0; i < config.dimensions->ndims(); ++i) {
        // downsampling made the chunk size strictly smaller
        const auto& dim = config.dimensions->at(i);
        const auto& downsampled_dim = downsampled_config.dimensions->at(i);

        if (dim.chunk_size_px > downsampled_dim.chunk_size_px) {
            return false;
        }
    }

    return true;
}

void
zarr::Downsampler::make_writers_()
{
    writers_.clear();

    const auto is_s3 = (s3_connection_pool_ != nullptr);
}