#pragma once

#include "array.hh"
#include "downsampler.hh"
#include "thread.pool.hh"

#include <nlohmann/json.hpp>

namespace zarr {
class MultiscaleArray : public ArrayBase
{
  public:
    /// The OME-NGFF version of the `ome` metadata that this group writes.
    static constexpr const char* ome_version = "0.5";

    MultiscaleArray(std::shared_ptr<ArrayConfig> config,
                    std::shared_ptr<ThreadPool> thread_pool,
                    std::shared_ptr<FileHandlePool> file_handle_pool,
                    std::shared_ptr<S3Client> s3_client);

    size_t memory_usage() const noexcept override;

    [[nodiscard]] WriteResult write_frame(std::vector<uint8_t>& frame,
                                          size_t& bytes_written,
                                          uint64_t frame_id) override;
    size_t max_bytes() const override;

    /**
     * @brief Get the array for a level of detail.
     * @param level The level of detail, where 0 is full resolution.
     * @return The array at `<node>/<level>`, or nullptr if there is no such
     * level.
     */
    ArrayBase* level(size_t level) const;

  protected:
    std::unique_ptr<Downsampler> downsampler_;
    std::vector<std::unique_ptr<Array>> arrays_;
    std::vector<uint64_t> array_frame_ids_;

    size_t bytes_per_frame_;

    bool make_metadata_(nlohmann::json& metadata) override;
    nlohmann::json library_ome_metadata_() const override;
    bool close_() override;

    /** @brief Create array writers. */
    [[nodiscard]] bool create_arrays_();

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
     * @brief Add @p data to downsampler and write downsampled frames to
     * lower-resolution arrays.
     * @param frame The frame data to write.
     * @return WriteResult::Ok if all levels are written successfully. otherwise
     * the WriteResult associated with the failure.
     */
    WriteResult write_multiscale_frames_(std::vector<uint8_t>& frame);
};
} // namespace zarr