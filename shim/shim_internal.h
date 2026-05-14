#pragma once

#include "acquire.zarr.h"
#include "shim_backend.h"
#include "shim_sink.h"

#include "types.stream.h"

struct multiarray_writer;
struct store;

struct shim_array
{
    char* key;
    struct dimension* dims;
    struct ngff_axis* axes;
    uint8_t rank;
    struct shim_sink sink;
    size_t frame_bytes;
    struct tile_stream_configuration config;
};

struct ZarrStream_s
{
    struct store* store;
    struct shim_array* arrays;
    size_t n_arrays;
    multiarray_tile_stream_t* multi_stream;
    struct multiarray_writer* writer;
    char* store_path;
    size_t estimated_memory;
    int max_threads;
};
