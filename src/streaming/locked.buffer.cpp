#include "locked.buffer.hh"
#include "macros.hh"

#include <blosc.h>

void
zarr::LockedBuffer::resize(size_t n)
{
    std::unique_lock lock(mutex_);
    data_.resize(n);
}

void
zarr::LockedBuffer::resize_and_fill(size_t n, uint8_t value)
{
    std::unique_lock lock(mutex_);

    data_.resize(n, value);
    std::fill(data_.begin(), data_.end(), value);
}

size_t
zarr::LockedBuffer::size() const
{
    std::unique_lock lock(mutex_);
    return data_.size();
}

bool
zarr::LockedBuffer::compress(const zarr::BloscCompressionParams& params,
                             size_t type_size)
{
    std::unique_lock lock(mutex_);
    if (data_.empty()) {
        LOG_WARNING("Buffer is empty, not compressing.");
        return false;
    }

    std::vector<uint8_t> compressed_data(data_.size() + BLOSC_MAX_OVERHEAD);
    const auto n_bytes_compressed = blosc_compress_ctx(params.clevel,
                                                       params.shuffle,
                                                       type_size,
                                                       data_.size(),
                                                       data_.data(),
                                                       compressed_data.data(),
                                                       compressed_data.size(),
                                                       params.codec_id.c_str(),
                                                       0,
                                                       1);

    if (n_bytes_compressed <= 0) {
        LOG_ERROR("blosc_compress_ctx failed with code ", n_bytes_compressed);
        return false;
    }

    data_.swap(compressed_data);
    return true;
}