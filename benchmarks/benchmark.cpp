#include "acquire.zarr.h"
#include <chrono>
#include <fstream>
#include <vector>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <cmath>

#define DIM(name_, type_, array_size, chunk_size, shard_size)                  \
    { .name = (name_),                                                         \
      .type = (type_),                                                         \
      .array_size_px = (array_size),                                           \
      .chunk_size_px = (chunk_size),                                           \
      .shard_size_chunks = (shard_size) }

namespace fs = std::filesystem;

struct ChunkConfig
{
    unsigned int t, c, z, y, x;
};

const std::vector<ChunkConfig> CHUNK_CONFIGS = { { 1, 1, 64, 64, 64 },
                                                 { 1, 1, 128, 128, 128 },
                                                 { 1, 1, 256, 256, 256 } };

const unsigned int ARRAY_WIDTH = 1920, ARRAY_HEIGHT = 1080, ARRAY_PLANES = 6,
                   ARRAY_CHANNELS = 3, ARRAY_TIMEPOINTS = 10;

const unsigned int NUM_RUNS = 5;

struct BenchmarkConfig
{
    ChunkConfig chunk;
    int zarr_version;
    std::string compression;
    std::string storage;
    unsigned int chunks_per_shard_x;
    unsigned int chunks_per_shard_y;
    std::string s3_endpoint;
    std::string s3_bucket;
    std::string s3_access_key;
    std::string s3_secret_key;
};

class Timer
{
    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point start;

  public:
    Timer()
      : start(Clock::now())
    {
    }
    double elapsed()
    {
        auto end = Clock::now();
        return std::chrono::duration<double>(end - start).count();
    }
};

ZarrStream*
setup_stream(const BenchmarkConfig& config)
{
    ZarrStreamSettings settings = { .store_path = "benchmark.zarr",
                                    .s3_settings = nullptr,
                                    .compression_settings = nullptr,
                                    .data_type = ZarrDataType_uint16,
                                    .version = static_cast<ZarrVersion>(
                                      config.zarr_version) };

    ZarrCompressionSettings comp_settings = {};
    if (config.compression != "none") {
        comp_settings.compressor = ZarrCompressor_Blosc1;
        comp_settings.codec = config.compression == "lz4"
                                ? ZarrCompressionCodec_BloscLZ4
                                : ZarrCompressionCodec_BloscZstd;
        comp_settings.level = 1;
        comp_settings.shuffle = 1;
        settings.compression_settings = &comp_settings;
    }

    ZarrS3Settings s3_settings = {};
    if (config.storage == "s3") {
        s3_settings = {
            .endpoint = config.s3_endpoint.c_str(),
            .bucket_name = config.s3_bucket.c_str(),
            .access_key_id = config.s3_access_key.c_str(),
            .secret_access_key = config.s3_secret_key.c_str(),
        };
        settings.s3_settings = &s3_settings;
    }

    ZarrStreamSettings_create_dimension_array(&settings, 5);
    auto* dims = settings.dimensions;

    dims[0] =
      DIM("t", ZarrDimensionType_Time, ARRAY_TIMEPOINTS, config.chunk.t, 1);
    dims[1] =
      DIM("c", ZarrDimensionType_Channel, ARRAY_CHANNELS, config.chunk.c, 1);
    dims[2] =
      DIM("z", ZarrDimensionType_Space, ARRAY_PLANES, config.chunk.z, 1);
    dims[3] = DIM("y",
                  ZarrDimensionType_Space,
                  ARRAY_HEIGHT,
                  config.chunk.y,
                  config.chunks_per_shard_y);
    dims[4] = DIM("x",
                  ZarrDimensionType_Space,
                  ARRAY_WIDTH,
                  config.chunk.x,
                  config.chunks_per_shard_x);

    return ZarrStream_create(&settings);
}

double
run_benchmark(const BenchmarkConfig& config)
{
    auto* stream = setup_stream(config);
    if (!stream)
        return -1.0;

    const size_t frame_size = ARRAY_WIDTH * ARRAY_HEIGHT * sizeof(uint16_t);
    std::vector<uint16_t> frame(ARRAY_WIDTH * ARRAY_HEIGHT, 0);
    const auto num_frames = ARRAY_PLANES * ARRAY_CHANNELS * ARRAY_TIMEPOINTS;

    Timer timer;
    size_t bytes_out;
    for (int i = 0; i < num_frames; ++i) {
        if (ZarrStream_append(stream, frame.data(), frame_size, &bytes_out) !=
            ZarrStatusCode_Success) {
            ZarrStream_destroy(stream);
            return -1.0;
        }
    }
    double elapsed = timer.elapsed();

    ZarrStream_destroy(stream);
    if (config.storage == "filesystem") {
        fs::remove_all("benchmark.zarr");
    }
    return elapsed;
}

int
main()
{
    std::ofstream csv("zarr_benchmarks.csv");
    csv << "chunk_size,zarr_version,compression,storage,chunks_per_shard_y,"
           "chunks_per_shard_x,run,time_seconds\n";

    std::vector<BenchmarkConfig> configs;
    for (const auto& chunk : CHUNK_CONFIGS) {

        // V2 configurations (no sharding)
        for (const auto& compression : { "none", "lz4", "zstd" }) {
            configs.push_back({ chunk, 2, compression, "filesystem", 1, 1 });

            if (std::getenv("ZARR_S3_ENDPOINT")) {
                configs.push_back({ chunk,
                                    2,
                                    compression,
                                    "s3",
                                    1,
                                    1,
                                    std::getenv("ZARR_S3_ENDPOINT"),
                                    std::getenv("ZARR_S3_BUCKET_NAME"),
                                    std::getenv("ZARR_S3_ACCESS_KEY_ID"),
                                    std::getenv("ZARR_S3_SECRET_ACCESS_KEY") });
            }
        }

        unsigned int max_cps_y = (ARRAY_HEIGHT + chunk.y - 1) / chunk.y;
        unsigned int max_cps_x = (ARRAY_WIDTH + chunk.x - 1) / chunk.x;

        // V3 configurations (with sharding)
        for (unsigned int cps_y = 1; cps_y <= max_cps_y; cps_y *= 2) {
            for (unsigned int cps_x = 1; cps_x <= max_cps_x; cps_x *= 2) {
                for (const auto& compression : { "none", "lz4", "zstd" }) {
                    configs.push_back(
                      { chunk, 3, compression, "filesystem", cps_x, cps_y });

                    if (std::getenv("ZARR_S3_ENDPOINT")) {
                        configs.push_back(
                          { chunk,
                            3,
                            compression,
                            "s3",
                            cps_x,
                            cps_y,
                            std::getenv("ZARR_S3_ENDPOINT"),
                            std::getenv("ZARR_S3_BUCKET_NAME"),
                            std::getenv("ZARR_S3_ACCESS_KEY_ID"),
                            std::getenv("ZARR_S3_SECRET_ACCESS_KEY") });
                    }
                }
            }
        }
    }

    for (const auto& config : configs) {
        std::string chunk_str = std::to_string(config.chunk.t) + "x" +
                                std::to_string(config.chunk.c) + "x" +
                                std::to_string(config.chunk.z) + "x" +
                                std::to_string(config.chunk.y) + "x" +
                                std::to_string(config.chunk.x);

        for (unsigned int run = 1; run <= NUM_RUNS; ++run) {
            std::cout << "Benchmarking " << chunk_str << " Zarr V"
                      << config.zarr_version
                      << ", compression: " << config.compression
                      << ", storage: " << config.storage
                      << ", CPS (y): " << config.chunks_per_shard_y
                      << ", CPS (x): " << config.chunks_per_shard_x << ", (run "
                      << run << " / " << NUM_RUNS << ")...";
            double time = run_benchmark(config);
            std::cout << " " << time << "s\n";
            if (time >= 0) {
                csv << chunk_str << "," << config.zarr_version << ","
                    << config.compression << "," << config.storage << ","
                    << config.chunks_per_shard_y << ","
                    << config.chunks_per_shard_x << "," << run << ","
                    << std::fixed << std::setprecision(3) << time << "\n";
            }
            csv.flush();
        }
    }

    return 0;
}