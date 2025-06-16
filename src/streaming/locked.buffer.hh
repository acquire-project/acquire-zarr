#pragma once

#include "blosc.compression.params.hh"

#include <mutex>
#include <vector>

namespace zarr {
class LockedBuffer
{
  private:
    mutable std::mutex mutex_;
    std::vector<uint8_t> data_;

  public:
    LockedBuffer() = default;

    template<typename F>
    auto with_lock(F&& fun) -> decltype(fun(data_))
    {
        std::unique_lock lock(mutex_);
        return fun(data_);
    }

    /**
     * @brief Resize the buffer to @p n bytes, but keep existing data.
     * @param n New size of the buffer.
     */
    void resize(size_t n);

    /**
     * @brief Resize the buffer to @p n bytes, filling new bytes with @p value.
     * @param n New size of the buffer.
     * @param value Value to fill new bytes with.
     */
    void resize_and_fill(size_t n, uint8_t value);

    /**
     * @brief Get the current size of the buffer.
     * @return Size of the buffer in bytes.
     */
    size_t size() const;

    /**
     * @brief Compress the buffer in place using Blosc with the given parameters.
     * @param params Compression parameters.
     * @param type_size Size of the data type being compressed (e.g., 1 for uint8, 2 for uint16).
     * @return true if compression was successful, false otherwise.
     */
    [[nodiscard]] bool compress(const zarr::BloscCompressionParams& params,
                                size_t type_size);
};
} // namespace zarr