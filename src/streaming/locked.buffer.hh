#pragma once

#include "blosc.compression.params.hh"
#include "definitions.hh"

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
    LockedBuffer(std::vector<uint8_t>&& data);

    LockedBuffer(const LockedBuffer& other) = delete;
    LockedBuffer(LockedBuffer&& other) noexcept;

    LockedBuffer& operator=(const LockedBuffer&) = delete;
    LockedBuffer& operator=(LockedBuffer&& other) noexcept;

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
     * @brief Assign new data to the buffer, replacing existing data.
     * @param data Data to assign to the buffer.
     */
    void assign(ConstByteSpan data);

    /**
     * @brief Assign new data to the buffer, replacing existing data.
     * @note Moves the data
     * @param data Data to assign to the buffer.
     */
    void assign(ByteVector&& data);

    /**
     * @brief Assign new data to the buffer at offset @p offset, replacing
     * existing data.
     * @param offset
     * @param data
     */
    void assign_at(size_t offset, ConstByteSpan data);

    /**
     * @brief Swap the contents of this buffer with another.
     * @param other The other LockedBuffer to swap with.
     */
    void swap(LockedBuffer& other);

    /**
     * @brief Clear the buffer, removing all data.
     */
    void clear();

    /**
     * @brief Take the contents of the buffer, leaving it empty.
     * @return The contents of the buffer.
     */
    std::vector<uint8_t> take();

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