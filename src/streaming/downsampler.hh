#pragma once

#include "definitions.hh"
#include "array.dimensions.hh"
#include "array.hh"

#include <unordered_map>

namespace zarr {
class Downsampler
{
  public:
    Downsampler(std::shared_ptr<ArrayConfig> config,
                ZarrDownsamplingMethod method);

    /**
     * @brief Add a full-resolution frame to the downsampler.
     * @note Downsampled frames are cached internally and can be retrieved, per
     * level, by calling get_downsampled_frame().
     * @param frame_data The full-resolution frame data.
     */
    void add_frame(ConstByteSpan frame_data);

    /**
     * @brief Get the downsampled frame for the given level, removing it from
     * the internal cache if found. Return false if the frame was not found.
     * @note This method is not idempotent. It will remove the downsampled frame
     * from the internal cache.
     * @param[in] level The level of detail to get.
     * @param[out] frame_data The downsampled frame data.
     * @return True if the downsampled frame was found, false otherwise.
     */
    bool get_downsampled_frame(int level, ByteVector& frame_data);

    const std::unordered_map<int, std::shared_ptr<zarr::ArrayConfig>>&
    writer_configurations() const;

  private:
    using ScaleFunT = std::function<
      ByteVector(ConstByteSpan, size_t&, size_t&, ZarrDownsamplingMethod)>;
    using Average2FunT =
      std::function<void(ByteVector&, ConstByteSpan, ZarrDownsamplingMethod)>;

    ZarrDownsamplingMethod method_;

    ScaleFunT scale_fun_;
    Average2FunT average2_fun_;

    std::unordered_map<int, std::shared_ptr<ArrayConfig>>
      writer_configurations_;
    std::unordered_map<int, ByteVector> downsampled_frames_;
    std::unordered_map<int, ByteVector> partial_scaled_frames_;

    size_t n_levels_() const;

    void make_writer_configurations_(std::shared_ptr<ArrayConfig> config);
};
} // namespace zarr