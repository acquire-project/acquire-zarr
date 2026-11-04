#pragma once

#include "sink.hh"

namespace zarr {
/**
 * @brief A Sink that accepts every write and stores nothing.
 * @details Selected by a store path that starts with `discard://`. The stream
 * still tiles, compresses and serializes every chunk, shard index and metadata
 * document, so a benchmark measures the library's CPU and memory cost without
 * the cost of a filesystem or a network.
 */
class DiscardSink : public Sink
{
  public:
    /**
     * @param truncate_to_fit As for FileSink: the sink takes only whole-file
     * writes at offset 0, so a misplaced metadata write fails here as it does
     * on disk.
     */
    explicit DiscardSink(bool truncate_to_fit = false);

    bool write(size_t offset, ConstByteSpan data) override;

  protected:
    bool flush_() override;

  private:
    bool truncate_to_fit_;
};
} // namespace zarr
