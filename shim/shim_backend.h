#pragma once

#ifdef SHIM_BACKEND_GPU
#include "multiarray.gpu.h"
#include "stream.gpu.h"

typedef struct multiarray_tile_stream_gpu multiarray_tile_stream_t;
typedef struct tile_stream_memory_info tile_stream_memory_info_t;

#define multiarray_tile_stream_create multiarray_tile_stream_gpu_create
#define multiarray_tile_stream_destroy multiarray_tile_stream_gpu_destroy
#define multiarray_tile_stream_writer multiarray_tile_stream_gpu_writer
#define tile_stream_memory_estimate tile_stream_gpu_memory_estimate

#define TILE_STREAM_TOTAL_BYTES(info)                                          \
    ((info).device_bytes + (info).host_pinned_bytes)

#else
#include "multiarray.cpu.h"
#include "stream.cpu.h"

typedef struct multiarray_tile_stream_cpu multiarray_tile_stream_t;
typedef struct tile_stream_cpu_memory_info tile_stream_memory_info_t;

#define multiarray_tile_stream_create multiarray_tile_stream_cpu_create
#define multiarray_tile_stream_destroy multiarray_tile_stream_cpu_destroy
#define multiarray_tile_stream_writer multiarray_tile_stream_cpu_writer
#define tile_stream_memory_estimate tile_stream_cpu_memory_estimate

#define TILE_STREAM_TOTAL_BYTES(info) ((info).heap_bytes)

#endif
