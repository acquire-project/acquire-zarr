import json
from pathlib import Path

import numpy as np
import pytest
import zarr
from acquire_zarr import (
    ArraySettings,
    Dimension,
    DimensionType,
    StreamSettings,
    ZarrStream,
)

DIMS = {
    "t": Dimension(
        name="t",
        kind=DimensionType.TIME,
        array_size_px=2,
        chunk_size_px=1,
        shard_size_chunks=1,
    ),
    "c": Dimension(
        name="c",
        kind=DimensionType.CHANNEL,
        array_size_px=3,
        chunk_size_px=1,
        shard_size_chunks=1,
    ),
    "z": Dimension(
        name="z",
        kind=DimensionType.SPACE,
        array_size_px=4,
        chunk_size_px=1,
        shard_size_chunks=1,
    ),
    "y": Dimension(
        name="y",
        kind=DimensionType.SPACE,
        array_size_px=16,
        chunk_size_px=8,
        shard_size_chunks=1,
    ),
    "x": Dimension(
        name="x",
        kind=DimensionType.SPACE,
        array_size_px=24,
        chunk_size_px=8,
        shard_size_chunks=1,
    ),
}


@pytest.mark.parametrize(
    "input_dims,output_dims,append_dim_size",
    [
        (["t", "c", "z", "y", "x"], None, None),
        (["t", "c", "z", "y", "x"], ["t", "c", "z", "y", "x"], None),
        (["t", "z", "c", "y", "x"], ["t", "c", "z", "y", "x"], None),
        (["t", "z", "c", "y", "x"], ["t", "c", "z", "y", "x"], 5),  # unbounded dim 0
    ],
)
def test_dimension_transposition(
    store_path: Path,
    input_dims: list[str],
    output_dims: list[str] | None,
    append_dim_size: int | None,
):
    """
    Test that data received in `input_dims` order is correctly stored
    according to the specified `output_dims` order.

    Frames are written sequentially (frame 0, 1, 2, ...) where each frame
    corresponds to iterating through the append dimensions in input_dims order.
    The test verifies that these frames end up in the correct positions when
    stored according to output_dims order.

    If append_dim_size is provided, dimension 0 is treated as unbounded
    (array_size_px=0) and append_dim_size specifies the actual number of
    elements to write along that dimension.
    """
    # Build dimensions, creating a new object for dim 0 if unbounded
    dimensions = []
    for i, name in enumerate(input_dims):
        dim = DIMS[name]
        if i == 0 and append_dim_size is not None:
            dim = Dimension(  # copy with array_size_px=0
                name=dim.name,
                kind=dim.kind,
                array_size_px=0,
                chunk_size_px=dim.chunk_size_px,
                shard_size_chunks=dim.shard_size_chunks,
            )
        dimensions.append(dim)

    array = ArraySettings(
        dimensions=dimensions,
        storage_dimension_order=output_dims,
    )
    settings = StreamSettings(store_path=str(store_path), arrays=[array])
    stream = ZarrStream(settings)

    # Calculate shapes, using append_dim_size for unbounded dimension
    output_dims = input_dims if output_dims is None else output_dims

    def _get_size(name: str) -> int:
        if name == input_dims[0] and append_dim_size is not None:
            return append_dim_size
        return DIMS[name].array_size_px

    input_shape = tuple(_get_size(n) for n in input_dims)
    output_shape = tuple(_get_size(n) for n in output_dims)
    n_frames = np.prod(input_shape[:-2])
    if output_dims and output_dims != input_dims:
        assert input_shape != output_shape, (
            "Input and output shapes should differ for this test case"
        )

    # Write frames with sequential values (0, 1, 2, ...)
    # Frames are written in input dimension order
    expected_frame_values = np.arange(n_frames, dtype=np.uint8)
    for val in expected_frame_values:
        stream.append(np.full(input_shape[-2:], val, dtype=np.uint8))
    stream.close()

    # Verify metadata has axes in prescribed order
    group_metadata = json.loads(Path(store_path / "zarr.json").read_text())
    axes = group_metadata["attributes"]["ome"]["multiscales"][0]["axes"]
    axis_names = [ax["name"] for ax in axes]
    assert axis_names == output_dims, (
        f"Expected metadata axes in {output_dims} order, got {axis_names}"
    )

    # Verify data is stored in prescribed order
    written_data = np.asarray(zarr.open_array(store_path / "0"))
    assert written_data.shape == output_shape, (
        f"Expected written data with shape {output_shape}, got {written_data.shape}"
    )

    # Each frame was written with np.full(), so all pixels have the same value.
    # Extract one value per plane to get the frame numbers as stored.
    stored_frame_values = written_data[..., 0, 0]

    # Build expected frame values: start in input order, transpose if needed
    # we need to reshape because expected_frame_values is 1D initially but
    # stored_frame_values is in the full output shape
    expected_frame_values = expected_frame_values.reshape(input_shape[:-2])
    if output_dims and output_dims != input_dims:
        perm = [input_dims.index(d) for d in output_dims[:-2]]
        expected_frame_values = np.transpose(expected_frame_values, perm)

    # Verify the stored frame values match the expected transposition
    np.testing.assert_array_equal(stored_frame_values, expected_frame_values)


@pytest.mark.parametrize(
    "dims_in,dims_out,error_msg",
    [
        (
            ["z", "c", "y", "x"],
            ["c", "z", "y", "x"],
            "Transposing dimension 0.*not currently supported",
        ),
        (
            ["t", "z", "y", "x"],
            ["t", "y", "z", "x"],
            "The last two dimensions in acquisition order",
        ),
    ],
)
def test_transpose_raises_error(
    dims_in: list[str], dims_out: list[str], error_msg: str
):
    """Test that transposing dimension 0 away raises an error."""
    with pytest.raises(TypeError, match=error_msg):
        ArraySettings(
            dimensions=[DIMS[name] for name in dims_in], storage_dimension_order=dims_out
        )


def test_swap_xy(store_path: Path):
    """Test that swapping last two dimensions works correctly."""
    dims = [DIMS[name] for name in ["t", "y", "x"]]
    y_size = DIMS["y"].array_size_px
    x_size = DIMS["x"].array_size_px

    array = ArraySettings(dimensions=dims, storage_dimension_order=["t", "x", "y"])
    settings = StreamSettings(store_path=str(store_path), arrays=[array])
    stream = ZarrStream(settings)

    frame = np.arange(y_size, dtype=np.uint8)[:, np.newaxis]
    frame = np.broadcast_to(frame, (y_size, x_size))
    stream.append(frame)
    stream.close()

    data = np.asarray(zarr.open_array(store_path / "0"))
    assert data.shape == (1, x_size, y_size)

    expected = np.array([list(range(y_size))] * x_size, dtype=np.uint8)
    np.testing.assert_array_equal(data[0], expected)
