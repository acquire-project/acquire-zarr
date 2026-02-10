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
                    std::shared_ptr<FileHandlePool> file_handle_pool,
                    std::shared_ptr<S3ConnectionPool> s3_connection_pool);

    size_t memory_usage() const noexcept override;

    [[nodiscard]] WriteResult write_data(LockedBuffer& data,
                                         size_t& bytes_written) override;

  protected:
    std::unique_ptr<Downsampler> downsampler_;
    std::vector<std::unique_ptr<Array>> arrays_;

    size_t bytes_per_frame_;

    std::vector<std::string> metadata_keys_() const override;
    bool make_metadata_() override;
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
    std::shared_ptr<zarr::ArrayConfig> make_base_array_config_() const;

    /**
     * @brief Add @p data to downsampler and write downsampled frames to
     * lower-resolution arrays.
     * @param data The frame data to write.
     * @return WriteResult::Ok if all levels are written successfully. otherwise
     * the WriteResult associated with the failure.
     */
    WriteResult write_multiscale_frames_(LockedBuffer& data) const;
};
} // namespace zarr