# Stream a high-content screening (HCS) plate to the filesystem
#
# Each field of view is an OME-NGFF array at <plate>/<row>/<column>/<field>.
# The plate and well metadata are attributes under `ome`, which the stream
# writes as soon as it is given, so a reader can open the plate while the
# acquisition runs.
import numpy as np
from acquire_zarr import (
    ArraySettings,
    Dimension,
    DimensionType,
    StreamSettings,
    ZarrStream,
)


def plate_metadata(name, rows, columns, wells, acquisitions):
    """Build OME-NGFF 0.5 plate and well metadata.

    `wells` maps "<row>/<column>" to that well's fields of view, as
    (path, acquisition id). Returns the `ome` object for each group, keyed by
    its path relative to the plate ("" for the plate itself).
    """
    max_fields = {}  # acquisition id -> most fields in one well
    for fields in wells.values():
        for acq_id in {acq for _, acq in fields}:
            n = sum(1 for _, acq in fields if acq == acq_id)
            max_fields[acq_id] = max(max_fields.get(acq_id, 0), n)

    plate = {
        "name": name,
        "rows": [{"name": r} for r in rows],
        "columns": [{"name": c} for c in columns],
        "wells": [
            {
                "path": key,
                "rowIndex": rows.index(key.split("/")[0]),
                "columnIndex": columns.index(key.split("/")[1]),
            }
            for key in wells
        ],
        "field_count": max(len(fields) for fields in wells.values()),
        "acquisitions": [
            dict(acq, maximumfieldcount=max_fields[acq["id"]])
            for acq in acquisitions
            if acq["id"] in max_fields
        ],
    }

    metadata = {"": {"version": "0.5", "plate": plate}}
    for key, fields in wells.items():
        images = [{"path": path, "acquisition": acq} for path, acq in fields]
        metadata[key] = {"version": "0.5", "well": {"images": images}}
    return metadata


def field_of_view(key):
    return ArraySettings(
        output_key=key,
        is_ngff=True,
        data_type=np.uint16,
        dimensions=[
            Dimension(
                name="t",
                kind=DimensionType.TIME,
                array_size_px=0,
                chunk_size_px=1,
                shard_size_chunks=1,
            ),
            Dimension(
                name="y",
                kind=DimensionType.SPACE,
                array_size_px=256,
                chunk_size_px=128,
                shard_size_chunks=2,
            ),
            Dimension(
                name="x",
                kind=DimensionType.SPACE,
                array_size_px=256,
                chunk_size_px=128,
                shard_size_chunks=2,
            ),
        ],
    )


def main():
    wells = {
        "A/1": [("0", 0), ("1", 0)],
        "B/3": [("0", 0)],
    }
    metadata = plate_metadata(
        name="Experiment plate",
        rows=["A", "B"],
        columns=["1", "2", "3"],
        wells=wells,
        acquisitions=[{"id": 0, "name": "Scan 1"}],
    )

    keys = [
        f"plate/{well}/{path}"
        for well, fields in wells.items()
        for path, _ in fields
    ]
    settings = StreamSettings(
        store_path="output_hcs_plate.zarr",
        overwrite=True,
        arrays=[field_of_view(key) for key in keys],
    )
    stream = ZarrStream(settings)

    for group, ome in metadata.items():
        node_key = f"plate/{group}" if group else "plate"
        stream.write_attributes(ome, node_key=node_key, key="ome")

    frame = np.zeros((256, 256), dtype=np.uint16)
    for t in range(4):
        for key in keys:
            frame.fill(t)
            stream.append(frame, key=key)

    stream.close()


if __name__ == "__main__":
    main()
