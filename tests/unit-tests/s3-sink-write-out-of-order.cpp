#include "s3-test-helper.hh"
#include "s3.sink.hh"
#include "unit.test.macros.hh"

#include <numeric>
#include <thread>
#include <vector>

/// Shard::write_chunk claims each chunk's offset under a lock but writes outside
/// it, and every chunk is its own thread-pool job, so S3Sink receives byte ranges
/// concurrently and out of order. The upload underneath is strictly append-only,
/// so the sink has to stage and reorder. Write the fragments of a large object
/// back-to-front from several threads and require the bytes to come back exact.
int
main()
{
    const auto settings = test::s3_settings_from_env();
    if (!settings) {
        LOG_WARNING("S3 not configured. Skipping test.");
        return 0;
    }

    int retval = 1;
    const std::string object_name = "test-object";

    try {
        auto client = std::make_shared<zarr::S3Client>(*settings);

        CHECK(client->bucket_exists(settings->bucket_name));
        CHECK(client->delete_object(settings->bucket_name, object_name));

        // large enough to spill past the part size while streaming
        const size_t fragment_size = 1 << 20;
        const size_t n_fragments = 8;
        const size_t total = fragment_size * n_fragments;

        std::vector<uint8_t> expected(total);
        std::iota(expected.begin(), expected.end(), uint8_t{ 0 });

        // a fragment that cannot be appended yet is reported as held, so
        // Array::memory_usage() can see it
        {
            auto sink = std::make_unique<zarr::S3Sink>(
              settings->bucket_name, object_name, client);

            EXPECT_EQ(size_t, 0, sink->memory_usage());

            // offset 0 has not arrived, so this one can only be staged
            CHECK(sink->write(
              fragment_size,
              std::span(expected.data() + fragment_size, fragment_size)));
            EXPECT_EQ(size_t, fragment_size, sink->memory_usage());
        }

        {
            auto sink = std::make_unique<zarr::S3Sink>(
              settings->bucket_name, object_name, client);

            // back-to-front, concurrently: fragment 0 lands last
            std::vector<std::thread> writers;
            std::vector<char> ok(n_fragments, 0);
            for (size_t i = n_fragments; i-- > 0;) {
                writers.emplace_back([&, i] {
                    const size_t offset = i * fragment_size;
                    ok[i] = sink->write(
                      offset,
                      std::span(expected.data() + offset, fragment_size));
                });
            }
            for (auto& writer : writers) {
                writer.join();
            }
            for (size_t i = 0; i < n_fragments; ++i) {
                CHECK(ok[i]);
            }

            CHECK(zarr::finalize_sink(std::move(sink)));
        }

        EXPECT_EQ(size_t,
                  total,
                  get_object_size(*client, settings->bucket_name, object_name));

        const auto contents =
          client->get_object(settings->bucket_name, object_name);
        CHECK(contents);
        EXPECT_EQ(size_t, total, contents->size());
        if (*contents != expected) {
            LOG_ERROR("Object contents do not match what was written.");
            return 1;
        }

        // cleanup
        CHECK(client->delete_object(settings->bucket_name, object_name));

        retval = 0;
    } catch (const std::exception& e) {
        LOG_ERROR("Exception: ", e.what());
    }

    return retval;
}
