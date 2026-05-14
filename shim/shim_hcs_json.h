#pragma once

#include "acquire.zarr.h"

#include "util/strbuf.h"

// Append OME/NGFF plate attributes for `plate` to `sb`. Returns 0 on
// success, non-zero on allocation failure.
int
shim_hcs_plate_attributes_json(struct strbuf* sb, const ZarrHCSPlate* plate);

// Append OME/NGFF well attributes for `well` to `sb`. Returns 0 on success,
// non-zero on allocation failure.
int
shim_hcs_well_attributes_json(struct strbuf* sb, const ZarrHCSWell* well);
