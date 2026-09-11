#include "macros.hh"
#include "s3.sink.hh"

#include <utility>

namespace {
/// Holds S3Sink's exclusive-append claim for the caller's scope. The claim is
/// released even if an append throws, so a failed write cannot wedge the flush
/// that waits on it; an unacknowledged claim marks the sink failed, since bytes
/// counted as appended never reached the object.
class AppendClaim
{
  public:
    AppendClaim(std::unique_lock<std::mutex>& lock,
                std::condition_variable& cv,
                bool& draining,
                bool& failed)
      : lock_{ lock }
      , cv_{ cv }
      , draining_{ draining }
      , failed_{ failed }
    {
        draining_ = true;
    }

    ~AppendClaim()
    {
        if (!lock_.owns_lock()) {
            // an append threw; take the lock back to publish the failure
            try {
                lock_.lock();
            } catch (...) {
            }
        }

        if (!acknowledged_) {
            failed_ = true;
        }
        draining_ = false;
        cv_.notify_all();
    }

    AppendClaim(const AppendClaim&) = delete;
    AppendClaim& operator=(const AppendClaim&) = delete;

    /// Report that everything claimed was appended.
    void acknowledge() { acknowledged_ = true; }

  private:
    std::unique_lock<std::mutex>& lock_;
    std::condition_variable& cv_;
    bool& draining_;
    bool& failed_;
    bool acknowledged_{ false };
};
} // namespace

zarr::S3Sink::S3Sink(std::string_view bucket_name,
                     std::string_view object_key,
                     std::shared_ptr<S3Client> client)
  : bucket_name_{ bucket_name }
  , object_key_{ object_key }
  , client_{ std::move(client) }
{
    EXPECT(!bucket_name_.empty(), "Bucket name must not be empty");
    EXPECT(!object_key_.empty(), "Object key must not be empty");
    EXPECT(client_, "Null pointer: client");
}

size_t
zarr::S3Sink::memory_usage() const noexcept
{
    return staging_bytes_.load();
}

void
zarr::S3Sink::coalesce_()
{
    for (auto it = pending_.find(nbytes_appended_ + staged_.size());
         it != pending_.end();
         it = pending_.find(nbytes_appended_ + staged_.size())) {
        staged_.insert(staged_.end(), it->second.begin(), it->second.end());
        pending_.erase(it);
    }
}

bool
zarr::S3Sink::drain_(std::unique_lock<std::mutex>& lock)
{
    if (draining_) {
        // another writer is appending and will pick up what we staged
        return true;
    }

    AppendClaim claim(lock, cv_, draining_, failed_);
    bool retval = true;

    while (retval && !staged_.empty() &&
           (upload_ || staged_.size() >= min_part_size_)) {
        if (!upload_) {
            lock.unlock();
            auto upload = client_->create_upload(bucket_name_, object_key_);
            lock.lock();

            if (!upload) {
                LOG_ERROR("Failed to start upload of object ", object_key_);
                retval = false;
                break;
            }
            upload_ = std::move(upload);
        }

        auto batch = std::move(staged_);
        staged_.clear();
        nbytes_appended_ += batch.size();
        staging_bytes_ -= batch.size();

        lock.unlock();
        retval = upload_->append(batch);
        lock.lock();
    }

    if (retval) {
        claim.acknowledge();
    }

    return retval;
}

bool
zarr::S3Sink::write(size_t offset, ConstByteSpan data)
{
    if (data.data() == nullptr || data.empty()) {
        return true;
    }

    std::unique_lock lock(mutex_);

    if (failed_) {
        return false;
    }

    if (closing_) {
        LOG_ERROR("Cannot write data at offset ",
                  offset,
                  " of object ",
                  object_key_,
                  ", it is being finalized");
        return false;
    }

    if (offset < nbytes_appended_) {
        LOG_ERROR("Cannot write data at offset ",
                  offset,
                  " of object ",
                  object_key_,
                  ", already uploaded through ",
                  nbytes_appended_);
        return false;
    }

    // Shard::write_chunk replays the same offset when a write is retried, so a
    // fragment we are still holding is a no-op rather than an error
    if (const auto it = pending_.find(offset); it != pending_.end()) {
        if (it->second.size() != data.size()) {
            LOG_ERROR("Retried write at offset ",
                      offset,
                      " of object ",
                      object_key_,
                      " changed size from ",
                      it->second.size(),
                      " to ",
                      data.size());
            return false;
        }
    } else {
        pending_.emplace(offset, ByteVector(data.begin(), data.end()));
        staging_bytes_ += data.size();
    }

    coalesce_();

    return drain_(lock);
}

bool
zarr::S3Sink::flush_()
{
    std::unique_lock lock(mutex_);

    // finalization should follow the last write, but don't touch the upload
    // underneath a writer that is still appending
    cv_.wait(lock, [this] { return !draining_; });

    closing_ = true;

    if (failed_) {
        return false;
    }

    if (!pending_.empty()) {
        LOG_ERROR("Object ",
                  object_key_,
                  " is missing the bytes at offset ",
                  nbytes_appended_ + staged_.size(),
                  "; ",
                  pending_.size(),
                  " fragment(s) cannot be uploaded");
        return false;
    }

    // hold the append slot for the rest of finalization: two appends in flight
    // at once are undefined, and this class accepts concurrent writers
    AppendClaim claim(lock, cv_, draining_, failed_);

    if (!upload_) {
        if (staged_.empty()) {
            claim.acknowledge();
            return true;
        }

        // never spilled, so send it in one request rather than opening a
        // multipart upload for it
        auto batch = std::move(staged_);
        staged_.clear();
        nbytes_appended_ += batch.size();
        staging_bytes_ -= batch.size();

        lock.unlock();
        const bool stored =
          client_->put_object(bucket_name_, object_key_, batch);
        lock.lock();

        if (stored) {
            claim.acknowledge();
        }

        return stored;
    }

    while (!staged_.empty()) {
        auto batch = std::move(staged_);
        staged_.clear();
        nbytes_appended_ += batch.size();
        staging_bytes_ -= batch.size();

        lock.unlock();
        const bool appended = upload_->append(batch);
        lock.lock();

        if (!appended) {
            return false;
        }
    }

    auto* upload = upload_.get();
    lock.unlock();
    const bool stored = upload->finish();
    lock.lock();

    if (stored) {
        claim.acknowledge();
    }

    return stored;
}
