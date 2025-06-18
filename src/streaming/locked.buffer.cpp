#include "locked.buffer.hh"
#include "macros.hh"

#include <blosc.h>

zarr::LockedBuffer::LockedBuffer(std::vector<uint8_t>&& data)
  : data_(std::move(data))
{
}

zarr::LockedBuffer::LockedBuffer(zarr::LockedBuffer&& other) noexcept
  : data_(std::move(other.data_))
{
}

zarr::LockedBuffer&
zarr::LockedBuffer::operator=(zarr::LockedBuffer&& other) noexcept
{
    if (this != &other) {
        std::unique_lock lock1(mutex_, std::defer_lock);
        std::unique_lock lock2(other.mutex_, std::defer_lock);
        std::lock(lock1, lock2); // avoid deadlock

        data_ = std::move(other.data_);
    }

    return *this;
}

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

void
zarr::LockedBuffer::assign(ConstByteSpan data)
{
    std::unique_lock lock(mutex_);
    data_.assign(data.begin(), data.end());
}

void
zarr::LockedBuffer::assign(ByteVector&& data)
{
    std::unique_lock lock(mutex_);
    data_ = std::move(data);
}

void
zarr::LockedBuffer::assign_at(size_t offset, ConstByteSpan data)
{
    std::unique_lock lock(mutex_);
    if (offset + data.size() > data_.size()) {
        data_.resize(offset + data.size());
    }
    std::copy(data.begin(), data.end(), data_.begin() + offset);
}

void
zarr::LockedBuffer::swap(zarr::LockedBuffer& other)
{
    std::unique_lock lock(mutex_);
    other.with_lock([this](ByteVector& other_data) { data_.swap(other_data); });
}

void
zarr::LockedBuffer::clear()
{
    std::unique_lock lock(mutex_);
    data_.clear();
}

std::vector<uint8_t>
zarr::LockedBuffer::take()
{
    std::unique_lock lock(mutex_);
    std::vector<uint8_t> result = std::move(data_);
    data_ = std::vector<uint8_t>{}; // Fresh empty vector
    return result;
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

    compressed_data.resize(n_bytes_compressed);
    data_ = compressed_data;
    return true;
}