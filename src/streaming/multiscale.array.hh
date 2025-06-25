#pragma once

#include "array.hh"
#include "downsampler.hh"
#include "sink.hh"
#include "thread.pool.hh"

#include <nlohmann/json.hpp>

#include <optional>

namespace zarr {
class MultiscaleArray : public ArrayBase
{
  public:
    MultiscaleArray(std::shared_ptr<ArrayConfig> config,
                    std::shared_ptr<ThreadPool> thread_pool,
                    std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    /**
     * @brief Write a frame to the group.
     * @note This function splits the incoming frame into tiles and writes them
     * to the chunk buffers. If we are writing multiscale frames, the function
     * calls write_multiscale_frames_() to write the scaled frames.
     * @param data The frame data to write.
     * @return The number of bytes written of the full-resolution frame.
     */
    [[nodiscard]] size_t write_frame(LockedBuffer& data) override;

  protected:
    std::unique_ptr<zarr::Downsampler> downsampler_;

    std::vector<std::unique_ptr<Array>> arrays_;

    size_t bytes_per_frame_;

    bool close_() override;

    /** @brief Create array writers. */
    [[nodiscard]] virtual bool create_arrays_() = 0;

    /**
     * @brief Construct OME metadata for this group.
     * @return JSON structure with OME metadata for this group.
     */
    virtual nlohmann::json get_ome_metadata_() const = 0;

    /**
     * @brief Create a downsampler for multiscale acquisitions.
     * @return True if not writing multiscale, or if a downsampler was
     *         successfully created. Otherwise, false.
     */
    [[nodiscard]] bool create_downsampler_();

    /** @brief Construct OME multiscales metadata for this group. */
    [[nodiscard]] virtual nlohmann::json make_multiscales_metadata_() const;

    /** @brief Create a configuration for a full-resolution Array. */
    std::shared_ptr<zarr::ArrayConfig> make_base_array_config_() const;

    /**
     * @brief Add @p data to downsampler and write downsampled frames to lower-
     * resolution arrays.
     * @param data The frame data to write.
     */
    void write_multiscale_frames_(LockedBuffer& data);

  private:
    friend bool finalize_group(std::unique_ptr<MultiscaleArray>&& array);
};

[[nodiscard]]
bool
finalize_group(std::unique_ptr<MultiscaleArray>&& array);
} // namespace zarr