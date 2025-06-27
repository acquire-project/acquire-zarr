#include "array.dimensions.hh"
#include "macros.hh"
#include "zarr.common.hh"

ArrayDimensions::ArrayDimensions(std::vector<ZarrDimension>&& dims,
                                 ZarrDataType dtype)
  : dims_(std::move(dims))
  , dtype_(dtype)
  , chunks_per_shard_(1)
  , number_of_shards_(1)
  , bytes_per_chunk_(zarr::bytes_of_type(dtype))
  , number_of_chunks_in_memory_(1)
{
    EXPECT(dims_.size() > 2, "Array must have at least three dimensions.");

    for (auto i = 0; i < dims_.size(); ++i) {
        const auto& dim = dims_[i];
        bytes_per_chunk_ *= dim.chunk_size_px;
        chunks_per_shard_ *= dim.shard_size_chunks;

        if (i > 0) {
            number_of_chunks_in_memory_ *= zarr::chunks_along_dimension(dim);
            number_of_shards_ *= zarr::shards_along_dimension(dim);
        }
    }

    chunk_indices_for_shard_.resize(number_of_shards_);

    for (auto i = 0; i < chunks_per_shard_ * number_of_shards_; ++i) {
        const auto shard_index = shard_index_for_chunk_(i);
        shard_indices_.insert_or_assign(i, shard_index);
        shard_internal_indices_.insert_or_assign(i, shard_internal_index_(i));

        chunk_indices_for_shard_[shard_index].push_back(i);
    }
}

size_t
ArrayDimensions::ndims() const
{
    return dims_.size();
}

const ZarrDimension&
ArrayDimensions::operator[](size_t idx) const
{
    return dims_[idx];
}

const ZarrDimension&
ArrayDimensions::final_dim() const
{
    return dims_[0];
}

const ZarrDimension&
ArrayDimensions::height_dim() const
{
    return dims_[ndims() - 2];
}

const ZarrDimension&
ArrayDimensions::width_dim() const
{
    return dims_.back();
}

uint32_t
ArrayDimensions::chunk_lattice_index(uint64_t frame_id,
                                     uint32_t dim_index) const
{
    // the last two dimensions are special cases
    EXPECT(dim_index < ndims() - 2, "Invalid dimension index: ", dim_index);

    // the first dimension is a special case
    if (dim_index == 0) {
        auto divisor = dims_.front().chunk_size_px;
        for (auto i = 1; i < ndims() - 2; ++i) {
            const auto& dim = dims_[i];
            divisor *= dim.array_size_px;
        }

        CHECK(divisor);
        return frame_id / divisor;
    }

    size_t mod_divisor = 1, div_divisor = 1;
    for (auto i = dim_index; i < ndims() - 2; ++i) {
        const auto& dim = dims_[i];
        mod_divisor *= dim.array_size_px;
        div_divisor *= (i == dim_index ? dim.chunk_size_px : dim.array_size_px);
    }

    CHECK(mod_divisor);
    CHECK(div_divisor);

    return (frame_id % mod_divisor) / div_divisor;
}

uint32_t
ArrayDimensions::tile_group_offset(uint64_t frame_id) const
{
    std::vector<size_t> strides(dims_.size(), 1);
    for (auto i = dims_.size() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        const auto a = dim.array_size_px, c = dim.chunk_size_px;
        strides[i - 1] = strides[i] * ((a + c - 1) / c);
    }

    size_t offset = 0;
    for (auto i = ndims() - 3; i > 0; --i) {
        const auto idx = chunk_lattice_index(frame_id, i);
        const auto stride = strides[i];
        offset += idx * stride;
    }

    return offset;
}

uint64_t
ArrayDimensions::chunk_internal_offset(uint64_t frame_id) const
{
    const auto tile_size = zarr::bytes_of_type(dtype_) *
                           width_dim().chunk_size_px *
                           height_dim().chunk_size_px;

    uint64_t offset = 0;
    std::vector<uint64_t> array_strides(ndims() - 2, 1),
      chunk_strides(ndims() - 2, 1);

    for (auto i = (int)ndims() - 3; i > 0; --i) {
        const auto& dim = dims_[i];
        const auto internal_idx =
          (frame_id / array_strides[i]) % dim.array_size_px % dim.chunk_size_px;

        array_strides[i - 1] = array_strides[i] * dim.array_size_px;
        chunk_strides[i - 1] = chunk_strides[i] * dim.chunk_size_px;
        offset += internal_idx * chunk_strides[i];
    }

    // final dimension
    {
        const auto& dim = dims_[0];
        const auto internal_idx =
          (frame_id / array_strides.front()) % dim.chunk_size_px;
        offset += internal_idx * chunk_strides.front();
    }

    return offset * tile_size;
}

uint32_t
ArrayDimensions::number_of_chunks_in_memory() const
{
    return number_of_chunks_in_memory_;
}

size_t
ArrayDimensions::bytes_per_chunk() const
{
    return bytes_per_chunk_;
}

uint32_t
ArrayDimensions::number_of_shards() const
{
    return number_of_shards_;
}

uint32_t
ArrayDimensions::chunks_per_shard() const
{
    return chunks_per_shard_;
}

uint32_t
ArrayDimensions::chunk_layers_per_shard() const
{
    return dims_[0].shard_size_chunks;
}

uint32_t
ArrayDimensions::shard_index_for_chunk(uint32_t chunk_index) const
{
    return shard_indices_.at(chunk_index);
}

const std::vector<uint32_t>&
ArrayDimensions::chunk_indices_for_shard(uint32_t shard_index) const
{
    return chunk_indices_for_shard_.at(shard_index);
}

std::vector<uint32_t>
ArrayDimensions::chunk_indices_for_shard_layer(uint32_t shard_index,
                                               uint32_t layer) const
{
    const auto& chunk_indices = chunk_indices_for_shard(shard_index);
    const auto chunks_per_layer = number_of_chunks_in_memory_;

    std::vector<uint32_t> indices;
    indices.reserve(chunks_per_shard_);

    for (const auto& idx : chunk_indices) {
        if ((idx / chunks_per_layer) == layer) {
            indices.push_back(idx);
        }
    }

    return indices;
}

uint32_t
ArrayDimensions::shard_internal_index(uint32_t chunk_index) const
{
    return shard_internal_indices_.at(chunk_index);
}

uint32_t
ArrayDimensions::shard_index_for_chunk_(uint32_t chunk_index) const
{
    // make chunk strides
    std::vector<uint64_t> chunk_strides;
    chunk_strides.resize(dims_.size());
    chunk_strides.back() = 1;

    for (auto i = dims_.size() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        chunk_strides[i - 1] =
          chunk_strides[i] * zarr::chunks_along_dimension(dim);
    }

    // get chunk indices
    std::vector<uint32_t> chunk_lattice_indices(ndims());
    for (auto i = ndims() - 1; i > 0; --i) {
        chunk_lattice_indices[i] =
          chunk_index % chunk_strides[i - 1] / chunk_strides[i];
    }

    // make shard strides
    std::vector<uint32_t> shard_strides(ndims(), 1);
    for (auto i = ndims() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        shard_strides[i - 1] =
          shard_strides[i] * zarr::shards_along_dimension(dim);
    }

    std::vector<uint32_t> shard_lattice_indices;
    for (auto i = 0; i < ndims(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices[i] /
                                        dims_[i].shard_size_chunks);
    }

    uint32_t index = 0;
    for (auto i = 0; i < ndims(); ++i) {
        index += shard_lattice_indices[i] * shard_strides[i];
    }

    return index;
}

uint32_t
ArrayDimensions::shard_internal_index_(uint32_t chunk_index) const
{
    // make chunk strides
    std::vector<uint64_t> chunk_strides;
    chunk_strides.resize(dims_.size());
    chunk_strides.back() = 1;

    for (auto i = dims_.size() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        chunk_strides[i - 1] =
          chunk_strides[i] * zarr::chunks_along_dimension(dim);
    }

    // get chunk indices
    std::vector<size_t> chunk_lattice_indices(ndims());
    for (auto i = ndims() - 1; i > 0; --i) {
        chunk_lattice_indices[i] =
          chunk_index % chunk_strides[i - 1] / chunk_strides[i];
    }
    chunk_lattice_indices[0] = chunk_index / chunk_strides.front();

    // make shard lattice indices
    std::vector<size_t> shard_lattice_indices;
    for (auto i = 0; i < ndims(); ++i) {
        shard_lattice_indices.push_back(chunk_lattice_indices[i] /
                                        dims_[i].shard_size_chunks);
    }

    std::vector<size_t> chunk_internal_strides(ndims(), 1);
    for (auto i = ndims() - 1; i > 0; --i) {
        const auto& dim = dims_[i];
        chunk_internal_strides[i - 1] =
          chunk_internal_strides[i] * dim.shard_size_chunks;
    }

    size_t index = 0;

    for (auto i = 0; i < ndims(); ++i) {
        index += (chunk_lattice_indices[i] % dims_[i].shard_size_chunks) *
                 chunk_internal_strides[i];
    }

    return index;
}
