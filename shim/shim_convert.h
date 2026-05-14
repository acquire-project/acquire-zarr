#pragma once

#include "acquire.zarr.h"

#include "dimension.h"
#include "dtype.h"
#include "ngff.h"
#include "types.codec.h"
#include "types.lod.h"

enum dtype
shim_convert_dtype(ZarrDataType dt);

struct codec_config
shim_convert_codec(const ZarrCompressionSettings* settings);

enum ngff_axis_type
shim_convert_ngff_axis_type(ZarrDimensionType type);

enum lod_reduce_method
shim_convert_reduce_method(ZarrDownsamplingMethod method);

// Allocate and convert ZarrDimensionProperties[] to struct dimension[].
// When multiscale is true, sets downsample=1 on spatial dimensions.
// Caller owns the returned array (free with free()).
// Returns NULL on allocation failure.
struct dimension*
shim_convert_dimensions(const ZarrDimensionProperties* props,
                        size_t count,
                        const size_t* storage_dimension_order,
                        bool multiscale);

// Allocate and convert ZarrDimensionProperties[] to struct ngff_axis[].
// Maps dimension type/unit/scale to NGFF axis metadata.
// Caller owns the returned array (free with free()).
// Returns NULL on allocation failure.
struct ngff_axis*
shim_convert_ngff_axes(const ZarrDimensionProperties* props, size_t count);
