# Design: RFC-5 Coordinate Systems & Transformations (OME-Zarr 0.6)

Status: **draft for review** · Depends on: the `ome_version` selector (PR #240) ·
Target: `ZarrOMEVersion_0_6`

## 1. Goal

Support the defining feature of OME-Zarr 0.6 — [RFC-5](https://ngff.openmicroscopy.org/rfc/5/):
**named coordinate systems** plus a **richer transformation model** (affine,
rotation, translation, sequence, …) — behind the existing `0.6` opt-in, with
`0.5` output unchanged.

RFC-5 is accepted but still `S4` (implementations updating). We pin output to a
specific dev tag (0.6.dev3) and keep it opt-in until 0.6 is released.

## 2. What RFC-5 changes (vs our current 0.5 output)

Today (0.5) we emit, per `multiscales[0]`:

```jsonc
"axes":     [ {name,type,unit}, ... ],
"datasets": [ { "path":"0", "coordinateTransformations":[ {"type":"scale","scale":[...]} ] }, ... ]
```

RFC-5 restructures this into:

- **`coordinateSystems`**: named sets of axes. The axis metadata (name/type/unit,
  plus optional `discrete`, `longName`) moves *into* the coordinate system; the
  top-level `axes` key is superseded.
- **`coordinateTransformations`** gain **`input`/`output`** referencing coordinate
  systems (or array paths), and a full transform zoo.
- Each dataset's transform maps the **array coordinate system** (`input` == the
  dataset `path`, e.g. `"0"`) to a shared physical system (`output`, e.g.
  `"intrinsic"`).
- An optional **multiscales-level `coordinateTransformations`** maps `intrinsic`
  to further systems (e.g. a rotated/registered space).

Minimal faithful translation of what we already emit:

```jsonc
"multiscales": [{
  "coordinateSystems": [
    { "name": "intrinsic",
      "axes": [ {"name":"z","type":"space","unit":"micrometer"},
                {"name":"y","type":"space","unit":"micrometer"},
                {"name":"x","type":"space","unit":"micrometer"} ] }
  ],
  "datasets": [
    { "path": "0",
      "coordinateTransformations": [
        { "type":"scale", "scale":[1,1,1],
          "input":{"path":"0"}, "output":{"name":"intrinsic"} } ] },
    { "path": "1",
      "coordinateTransformations": [
        { "type":"scale", "scale":[2,2,2],
          "input":{"path":"1"}, "output":{"name":"intrinsic"} } ] }
  ]
}]
```

### Transform zoo (RFC-5)

| Type | Params | Notes |
|------|--------|-------|
| identity | — | |
| mapAxis | `mapAxis:[int]` | axis permutation |
| translation | `translation:[num]` \| `path` | |
| scale | `scale:[num]` \| `path` | what we emit today |
| affine | `affine:[[..]]` (M×(N+1)) \| `path` | |
| rotation | `rotation:[[..]]` (N×N, det 1) \| `path` | |
| sequence | `transformations:[...]`, `input`, `output` | ordered composition |
| byDimension | `transformations:[{...,input_axes,output_axes}]` | per-axis-subset |
| coordinates / displacements | `path`, `interpolation` | non-linear; array-backed |
| inverseOf / bijection | wrap other transforms | |

`input`/`output` reference a coordinate-system `name` or a zarr path; array-backed
transforms (`path`) live under a `coordinateTransformations/` group in the container.

## 3. Key insight: the first RFC-5 PR needs **no new user API**

Everything we emit today (per-level `scale` derived from `dim.scale`) maps directly
into the RFC-5 shape. So the first slice is a **pure emission change** gated on
`ome_version == 0_6`:````

1. Build one `coordinateSystems` entry named `"intrinsic"` from the existing visible
   axes (reusing `dimension_type_to_string` + units).
2. Emit each dataset transform as today's `scale`, adding `"input": {"path": "<path>"}`
   and `"output": {"name": "intrinsic"}` (both are objects, per the 0.6rc0
   `inputOutput` schema, not bare strings).
3. Omit the top-level `axes` key in 0.6 mode (superseded); keep it in 0.5 mode.

This lands RFC-5-structural conformance with zero API surface and a clean 0.5/0.6````
branch in `MultiscaleArray::make_multiscales_metadata_()`.

## 4. Proposed staging

**PR #2a — structural RFC-5 emission (this design's core).**
- Branch `make_multiscales_metadata_()` on `config_->ome_version`.
- Add a helper that builds `coordinateSystems` from `ArrayDimensions`.
- Rewrite the datasets loop to attach `input`/`output`.
- Tests: a 0.6 multiscale store asserts `coordinateSystems`, per-dataset
  `input`/`output`, and absence of top-level `axes`; 0.5 output byte-identical.
- No C API / Python / config-file changes.

**PR #2b — user-facing transforms (additive).**
- C API: a per-array optional list of extra transforms and named coordinate
  systems, e.g. `ZarrCoordinateSystem` / `ZarrCoordinateTransform` (tagged union
  over the transform zoo). Start with `translation`, `scale`, `affine`, `rotation`,
  `sequence`; defer `coordinates`/`displacements`/`byDimension`/`bijection`/array-backed
  `path` transforms.
- Internal model on `ArrayConfig` (mirrors the omero pattern: transient C struct →
  owning C++ representation).
- Emit optional multiscales-level `coordinateTransformations`.
- Config-file + Python bindings (reuse the shared_ptr/opaque-vector pattern from omero).

**PR #2c (optional, later) — group-level & array-backed transforms.**
- Cross-image transforms in a parent group's attributes; `coordinateTransformations/`
  zarr arrays for affine/displacement fields. Larger; only if demand appears.

## 5. Internal representation sketch (PR #2b)

Mirror the omero design (`src/streaming/array.base.hh`):

```cpp
struct CoordinateAxis { std::string name; ZarrDimensionType type;
                        std::optional<std::string> unit; bool discrete{false}; };
struct CoordinateSystem { std::string name; std::vector<CoordinateAxis> axes; };

struct Transform {              // tagged union over the supported subset
    enum class Kind { Identity, Translation, Scale, Affine, Rotation, Sequence } kind;
    std::optional<std::string> input, output, name;
    std::vector<double> vector;             // translation/scale
    std::vector<std::vector<double>> matrix; // affine/rotation
    std::vector<Transform> children;         // sequence
};
```

`ArrayConfig` gains optional `coordinate_systems` and `extra_transformations`
(both default-empty ⇒ PR #2a behavior).

## 6. Open questions (need a decision)

1. **Drop `axes` in 0.6 mode?** RFC-5 supersedes top-level `axes` with
   `coordinateSystems`. Some readers still expect `axes`. **Recommendation:** follow
   the spec — emit only `coordinateSystems` in 0.6; keep `axes` in 0.5. Revisit if a
   target viewer needs both.
2. **Coordinate-system name.** RFC examples use `"intrinsic"`. **Recommendation:**
   default `"intrinsic"`; allow override later via API.
3. **HCS + 0.6.** RFC-5 doesn't redefine plate/well, so only the image-level
   metadata varies by version. Note the plate/well dicts carry no `version` key of
   their own in 0.5 or later: RFC-2 moved the version up to `ome.version`, and the
   leftover 0.5 prose requiring an inner `version` was a spec bug, fixed in
   ome/ngff-spec#84 (see ome/ngff#309). So there is nothing version-dependent to
   thread into `Plate::to_json`/`Well::to_json`.
4. **Non-space axes in transforms.** Today downsampling scales only space axes.
   RFC-5 transforms cover all axes; keep the current scale semantics (identity on
   non-space) and just restructure.

## 7. Testing plan

- C++ integration: extend `stream-3d-multiscale-to-filesystem`-style coverage with a
  0.6 variant asserting the RFC-5 shape; keep the 0.5 assertions as a regression guard.
- Python: a `0.6` multiscale store, validated against `ome-zarr-models-py` /
  `ome-zarr-py` if available in the test env (aspirational).
- Round-trip: once #2b adds API, extend `settings.io` + `test_settings.py` as we did
  for omero.

## 8. Not in scope (for now)

`coordinates`/`displacements` (array-backed non-linear fields), `byDimension`,
`bijection`, `inverseOf`, and cross-image/group-level transforms — tracked for #2c.
