#include "acquire.zarr.h"
#include "test.macros.hh"

#include <nlohmann/json.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <vector>

namespace fs = std::filesystem;

namespace {
constexpr uint32_t frame_size = 2048;
const std::vector<uint8_t>
frame_data(frame_size* frame_size, 0);

const std::vector<uint32_t> chunk_sizes{ 32, 64, 128, 256 };
const std::vector<uint32_t> chunks_per_shard{ 8, 16, 32, 64 };
const std::vector<uint32_t> layers_per_shard{ 1, 2, 4, 8, 16 };

} // namespace

ZarrStream*
make_stream(uint32_t chunk_size,
            uint32_t n_chunks_per_shard,
            uint32_t n_layers_per_shard)
{
    ZarrStreamSettings settings{ .store_path = TEST ".zarr",
                                 .version = ZarrVersion_3,
                                 .overwrite = true };

    EXPECT(ZarrStreamSettings_create_arrays(&settings, 1) ==
             ZarrStatusCode_Success,
           "Failed to create array settings");
    EXPECT(ZarrArraySettings_create_dimension_array(settings.arrays, 5) ==
             ZarrStatusCode_Success,
           "Failed to create dimension array");

    settings.arrays->data_type = ZarrDataType_uint8;

    settings.arrays[0].dimensions[0] =
      DIM("t", ZarrDimensionType_Time, 0, 1, n_layers_per_shard, nullptr, 1.0);
    settings.arrays[0].dimensions[1] =
      DIM("c", ZarrDimensionType_Channel, 1, 1, 1, nullptr, 1.0);
    settings.arrays[0].dimensions[2] = DIM("z",
                                           ZarrDimensionType_Space,
                                           chunk_sizes.back(),
                                           chunk_size,
                                           n_chunks_per_shard,
                                           "millimeter",
                                           1.0);
    settings.arrays[0].dimensions[3] = DIM("y",
                                           ZarrDimensionType_Space,
                                           frame_size,
                                           chunk_size,
                                           n_chunks_per_shard,
                                           "micrometer",
                                           1.0);
    settings.arrays[0].dimensions[4] = DIM("x",
                                           ZarrDimensionType_Space,
                                           frame_size,
                                           chunk_size,
                                           n_chunks_per_shard,
                                           "micrometer",
                                           1.0);

    auto* stream = ZarrStream_create(&settings);

    // cleanup
    ZarrStreamSettings_destroy_arrays(&settings);

    return stream;
}

int
main()
{
    int retval = 1;
    nlohmann::json results_arr = nlohmann::json::array();
    ZarrStream* stream = nullptr;

    try {
        for (auto& layers : layers_per_shard) {
            for (auto& cps : chunks_per_shard) {
                for (auto& chunk_size : chunk_sizes) {
                    const size_t chunks_xyz =
                      (frame_size + chunk_size - 1) / chunk_size;
                    if (cps > chunks_xyz) {
                        continue;
                    }

                    const auto n_chunks = chunks_xyz * chunks_xyz * chunks_xyz;
                    const auto n_frames = chunk_size * cps * layers;

                    nlohmann::json j;
                    j["chunk_size"] = chunk_size;
                    j["chunks_per_shard"] = cps;
                    j["layers_per_shard"] = layers;
                    j["n_chunks"] = n_chunks;
                    j["frames_written"] = n_frames;

                    std::cout
                      << "Testing chunk size " << chunk_size
                      << ", chunks per shard " << cps << ", layers per shard "
                      << layers << ", chunk count " << n_chunks << " ("
                      << n_frames << " frames)... " << std::flush << std::endl;

                    stream = make_stream(chunk_size, cps, layers);
                    EXPECT(stream != nullptr, "Failed to create stream");

                    auto start = std::chrono::high_resolution_clock::now();

                    for (auto i = 0; i < n_frames; ++i) {
                        size_t bytes_written = 0;
                        ZarrStatusCode status =
                          ZarrStream_append(stream,
                                            frame_data.data(),
                                            frame_data.size(),
                                            &bytes_written,
                                            nullptr);
                        EXPECT(status == ZarrStatusCode_Success,
                               "Failed to append frame ",
                               i,
                               ", status code ",
                               int(status));
                        EXPECT(bytes_written == frame_data.size(),
                               "Expected to write ",
                               frame_data.size(),
                               " bytes, but wrote ",
                               bytes_written);
                        std::cout << "." << std::flush;
                    }
                    auto end_append = std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double> elapsed_append =
                      end_append - start;

                    std::cout << "\nFinalizing... " << std::flush;
                    ZarrStream_destroy(stream);
                    stream = nullptr;
                    std::cout << "done." << std::endl;

                    auto end_destroy =
                      std::chrono::high_resolution_clock::now();
                    std::chrono::duration<double> elapsed_destroy =
                      end_destroy - start;

                    const double fps = n_frames / elapsed_append.count();

                    j["elapsed_time_append"] = elapsed_append.count();
                    j["elapsed_time_destroy"] = elapsed_destroy.count();

                    std::cout
                      << "Wrote " << n_frames << " frames in "
                      << elapsed_append.count() << " seconds (" << fps
                      << " fps); time to destroy: " << elapsed_destroy.count()
                      << " seconds" << std::endl;

                    results_arr.push_back(j);
                }
            }
        }

        retval = 0;
    } catch (const std::exception& err) {
        LOG_ERROR("Failed: ", err.what());
    }

    // write out results to file
    std::ofstream results_file(TEST "-timing-results.json");
    results_file << results_arr.dump(2) << "\n";
    results_file.close();

    // cleanup
    if (stream != nullptr) {
        ZarrStream_destroy(stream);
    }

    if (fs::exists(TEST ".zarr")) {
        fs::remove_all(TEST ".zarr");
    }

    return retval;
}
