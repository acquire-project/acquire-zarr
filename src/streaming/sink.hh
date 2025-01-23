#pragma once

#include "definitions.hh"

#include <cstddef> // size_t, std::byte
#include <memory>  // std::unique_ptr
#include <span>    // std::span

namespace zarr {
class Sink
{
  public:
    virtual ~Sink() = default;

    /**
     * @brief Write data to the sink.
     * @param offset The offset in the sink to write to.
     * @param buf The buffer to write to the sink.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] virtual bool write(size_t offset, ConstByteSpan buf) = 0;

    /**
     * @brief Write several data buffers to the sink.
     * @param offset The offset in the sink to write to.
     * @param bufs The buffers to write to the sink.
     * @return True if the write was successful, false otherwise.
     */
    [[nodiscard]] virtual bool write_vectors(size_t offset, const std::vector<ConstByteSpan>& bufs) = 0;

  protected:
    [[nodiscard]] virtual bool flush_() = 0;

    friend bool finalize_sink(std::unique_ptr<Sink>&& sink);
};

bool
finalize_sink(std::unique_ptr<Sink>&& sink);
} // namespace zarr
