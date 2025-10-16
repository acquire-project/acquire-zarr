#pragma once

#include "array.hh"
#include "downsampler.hh"
#include "thread.pool.hh"

#include <nlohmann/json.hpp>

#include <optional>

namespace zarr {
class MultiscaleArray : public ArrayBase
{
  public:
    MultiscaleArray(std::shared_ptr<ArrayConfig> config,
                    std::shared_ptr<ThreadPool> thread_pool);

    size_t memory_usage() const noexcept override;

    /**
     * @brief Write a frame to the group.
     * @note This function splits the incoming frame into tiles and writes them
     * to the chunk buffers. If we are writing multiscale frames, the function
     * calls write_multiscale_frames_() to write the scaled frames.
     * @param data The frame data to write.
     * @return The number of bytes written of the full-resolution frame.
     */
    [[nodiscard]] size_t write_frame(std::vector<uint8_t>& data) override;

  protected:
    std::unique_ptr<Downsampler> downsampler_;
    std::vector<std::unique_ptr<Array>> arrays_;

    size_t bytes_per_frame_;

    bool make_metadata_(std::string& metadata_str) override;
    bool close_() override;

    /** @brief Create array writers. */
    [[nodiscard]] virtual bool create_arrays_() = 0;

    /**
     * @brief Construct OME metadata for this group.
     * @return JSON structure with OME metadata for this group.
     */
    nlohmann::json get_ome_metadata_() const;

    /**
     * @brief Create a downsampler for multiscale acquisitions.
     * @return True if not writing multiscale, or if a downsampler was
     *         successfully created. Otherwise, false.
     */
    [[nodiscard]] bool create_downsampler_();

    /** @brief Construct OME multiscales metadata for this group. */
    [[nodiscard]] virtual nlohmann::json make_multiscales_metadata_() const;

    /** @brief Create a configuration for a full-resolution Array. */
    std::shared_ptr<ArrayConfig> make_base_array_config_() const;

    /**
     * @brief Add @p data to downsampler and write downsampled frames to lower-
     * resolution arrays.
     * @param data The frame data to write.
     */
    void write_multiscale_frames_(std::vector<uint8_t>& data);
};
} // namespace zarr