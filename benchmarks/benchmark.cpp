#include "acquire.zarr.h"
#include <chrono>
#include <vector>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <cmath>
#include <string>
#include <sstream>

namespace fs = std::filesystem;

const unsigned int ARRAY_WIDTH = 1920, ARRAY_HEIGHT = 1080, ARRAY_PLANES = 6,
                   ARRAY_CHANNELS = 3, ARRAY_TIMEPOINTS = 10;

struct ChunkConfig
{
    unsigned int t, c, z, y, x;
};

struct BenchmarkConfig
{
    BenchmarkConfig()
      : chunk({ 1, 1, 1, 1, 1 })
      , zarr_version(3)
      , chunks_per_shard_x(0)
      , chunks_per_shard_y(0)
    {
    }

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

    dims[0] = { .name = "t",
                .type = ZarrDimensionType_Time,
                .array_size_px = ARRAY_TIMEPOINTS,
                .chunk_size_px = config.chunk.t,
                .shard_size_chunks = 1 };
    dims[1] = { .name = "c",
                .type = ZarrDimensionType_Channel,
                .array_size_px = ARRAY_CHANNELS,
                .chunk_size_px = config.chunk.c,
                .shard_size_chunks = 1 };
    dims[2] = { .name = "z",
                .type = ZarrDimensionType_Space,
                .array_size_px = ARRAY_PLANES,
                .chunk_size_px = config.chunk.z,
                .shard_size_chunks = 1 };
    dims[3] = { .name = "y",
                .type = ZarrDimensionType_Space,
                .array_size_px = ARRAY_HEIGHT,
                .chunk_size_px = config.chunk.y,
                .shard_size_chunks = config.chunks_per_shard_y };
    dims[4] = { .name = "x",
                .type = ZarrDimensionType_Space,
                .array_size_px = ARRAY_WIDTH,
                .chunk_size_px = config.chunk.x,
                .shard_size_chunks = config.chunks_per_shard_x };

    return ZarrStream_create(&settings);
}

double
run_benchmark(const BenchmarkConfig& config)
{
    auto* stream = setup_stream(config);
    if (!stream) {
        std::cerr << "Failed to create ZarrStream\n";
        return -1.0;
    }

    const size_t frame_size = ARRAY_WIDTH * ARRAY_HEIGHT * sizeof(uint16_t);
    std::vector<uint16_t> frame(ARRAY_WIDTH * ARRAY_HEIGHT, 0);
    const auto num_frames = ARRAY_PLANES * ARRAY_CHANNELS * ARRAY_TIMEPOINTS;

    Timer timer;
    size_t bytes_out;
    for (int i = 0; i < num_frames; ++i) {
        if (ZarrStream_append(stream, frame.data(), frame_size, &bytes_out) !=
            ZarrStatusCode_Success) {
            std::cerr << "Failed to append frame " << i << "\n";
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

void
print_usage(const char* program_name)
{
    std::cerr
      << "Usage: " << program_name << " [OPTIONS]\n"
      << "Options:\n"
      << "  --chunk t,c,z,y,x    Chunk dimensions (required)\n"
      << "  --version VERSION    Zarr version (2 or 3, required)\n"
      << "  --compression TYPE   Compression type (none/lz4/zstd, required)\n"
      << "  --storage TYPE      Storage type (filesystem/s3, required)\n"
      << "  --shard-y NUM       Chunks per shard Y (required for v3)\n"
      << "  --shard-x NUM       Chunks per shard X (required for v3)\n"
      << "  --s3-endpoint URL   S3 endpoint (required for s3 storage)\n"
      << "  --s3-bucket NAME    S3 bucket name (required for s3 storage)\n"
      << "  --s3-access-key ID  S3 access key (required for s3 storage)\n"
      << "  --s3-secret-key KEY S3 secret key (required for s3 storage)\n\n"
      << "Output is written to stdout in CSV format. Values are:\n"
      << "  Chunk dimensions (t,c,z,y,x), Zarr version, Compression type,\n"
      << "  Storage type, Chunks per shard in Y, Chunks per shard in X, Time "
         "(s)\n";
}

bool
parse_chunk_config(const std::string& chunk_str, ChunkConfig& config)
{
    std::stringstream ss(chunk_str);
    std::string item;
    std::vector<unsigned int> values;

    while (std::getline(ss, item, ',')) {
        try {
            values.push_back(std::stoul(item));
        } catch (...) {
            return false;
        }
    }

    if (values.size() != 5)
        return false;

    config.t = values[0];
    config.c = values[1];
    config.z = values[2];
    config.y = values[3];
    config.x = values[4];
    return true;
}

int
main(int argc, char* argv[])
{
    BenchmarkConfig config;
    bool has_chunk = false;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--chunk" && i + 1 < argc) {
            if (!parse_chunk_config(argv[++i], config.chunk)) {
                std::cerr << "Invalid chunk configuration\n";
                print_usage(argv[0]);
                return 1;
            }
            has_chunk = true;
        } else if (arg == "--version" && i + 1 < argc) {
            config.zarr_version = std::stoi(argv[++i]);
            if (config.zarr_version != 2 && config.zarr_version != 3) {
                std::cerr << "Invalid Zarr version: " << config.zarr_version
                          << "\n";
                print_usage(argv[0]);
                return 1;
            }
        } else if (arg == "--compression" && i + 1 < argc) {
            config.compression = argv[++i];
            if (config.compression != "none" && config.compression != "lz4" &&
                config.compression != "zstd") {
                std::cerr << "Invalid compression type: '" << config.compression
                          << "'. Use 'none', 'lz4', or 'zstd'\n";
                print_usage(argv[0]);
                return 1;
            }
        } else if (arg == "--storage" && i + 1 < argc) {
            config.storage = argv[++i];
            if (config.storage != "filesystem" && config.storage != "s3") {
                std::cerr << "Invalid storage type\n";
                print_usage(argv[0]);
                return 1;
            }
        } else if (arg == "--shard-y" && i + 1 < argc) {
            config.chunks_per_shard_y = std::stoul(argv[++i]);
        } else if (arg == "--shard-x" && i + 1 < argc) {
            config.chunks_per_shard_x = std::stoul(argv[++i]);
        } else if (arg == "--s3-endpoint" && i + 1 < argc) {
            config.s3_endpoint = argv[++i];
        } else if (arg == "--s3-bucket" && i + 1 < argc) {
            config.s3_bucket = argv[++i];
        } else if (arg == "--s3-access-key" && i + 1 < argc) {
            config.s3_access_key = argv[++i];
        } else if (arg == "--s3-secret-key" && i + 1 < argc) {
            config.s3_secret_key = argv[++i];
        } else if (arg == "--help") {
            print_usage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown or incomplete argument: " << arg << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    // Validate required arguments
    if (!has_chunk || config.compression.empty() || config.storage.empty()) {
        std::cerr << "Missing required arguments\n";
        print_usage(argv[0]);
        return 1;
    }

    // Validate S3-specific requirements
    if (config.storage == "s3" &&
        (config.s3_endpoint.empty() || config.s3_bucket.empty() ||
         config.s3_access_key.empty() || config.s3_secret_key.empty())) {
        std::cerr << "Missing required S3 configuration\n";
        print_usage(argv[0]);
        return 1;
    }

    // Run benchmark
    double time = run_benchmark(config);

    if (time >= 0) {
        std::string chunk_str = std::to_string(config.chunk.t) + "x" +
                                std::to_string(config.chunk.c) + "x" +
                                std::to_string(config.chunk.z) + "x" +
                                std::to_string(config.chunk.y) + "x" +
                                std::to_string(config.chunk.x);

        // Write results to stdout
        std::cout << chunk_str << "," << config.zarr_version << ","
                  << config.compression << "," << config.storage << ","
                  << config.chunks_per_shard_y << ","
                  << config.chunks_per_shard_x << "," << std::fixed
                  << std::setprecision(3) << time << "\n";

        std::cerr << "Benchmark completed in " << time << "s\n";
        return 0;
    }

    std::cerr << "Benchmark failed\n";
    return 1;
}