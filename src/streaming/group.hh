#pragma once

#include "array.hh"
#include "downsampler.hh"
#include "thread.pool.hh"

#include <nlohmann/json.hpp>

#include <optional>

namespace zarr {
struct GroupConfig
{
    std::shared_ptr<ArrayDimensions> dimensions;
    ZarrDataType dtype;
    bool multiscale;
    std::optional<std::string> bucket_name;
    std::string store_root;
    std::optional<BloscCompressionParams> compression_params;
};

class Group
{
  public:
    Group(const GroupConfig& config, std::shared_ptr<ThreadPool> thread_pool);
    Group(const GroupConfig& config,
          std::shared_ptr<ThreadPool> thread_pool,
          std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    virtual ~Group() = default;

    /**
     * @brief Write a frame to the group.
     * @note This function splits the incoming frame into tiles and writes them
     * to the chunk buffers. If we are writing multiscale frames, the function
     * calls write_multiscale_frames_() to write the scaled frames.
     * @param data The frame data to write.
     * @return The number of bytes written of the full-resolution frame.
     */
    size_t write_frame(ConstByteSpan data);

    /**
     * @brief Construct OME metadata for this group.
     * @return
     */
    virtual nlohmann::json get_ome_metadata() const = 0;

  protected:
    GroupConfig config_;

    std::shared_ptr<ThreadPool> thread_pool_;
    std::shared_ptr<S3ConnectionPool> s3_connection_pool_;

    std::optional<zarr::Downsampler> downsampler_;

    std::vector<std::shared_ptr<Array>> arrays_;

    size_t bytes_per_frame_;

    /** @brief Create array writers. */
    [[nodiscard]] virtual bool create_arrays_() = 0;

    /**
     * @brief Create a downsampler for multiscale acquisitions.
     * @return True if not writing multiscale, or if a downsampler was
     *         successfully created. Otherwise, false.
     */
    [[nodiscard]] bool create_downsampler_();

    /** @brief Construct OME multiscales metadata for this group. */
    [[nodiscard]] virtual nlohmann::json make_multiscales_metadata_() const;

    /** @brief Create a configuration for a full-resolution Array. */
    zarr::ArrayConfig make_array_config_() const;

    /**
     * @brief Add @p data to downsampler and write downsampled frames to lower-
     * resolution arrays.
     * @param data The frame data to write.
     */
    void write_multiscale_frames_(ConstByteSpan data);

    /**
     * @brief
     * @return
     */
    [[nodiscard]] virtual bool write_group_metadata_() = 0;

    friend bool finalize_group(std::unique_ptr<Group>&& group);
};

[[nodiscard]]
bool
finalize_group(std::unique_ptr<Group>&& group);
} // namespace zarr