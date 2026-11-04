#include "discard.sink.hh"
#include "macros.hh"

zarr::DiscardSink::DiscardSink(bool truncate_to_fit)
  : truncate_to_fit_(truncate_to_fit)
{
}

bool
zarr::DiscardSink::write(size_t offset, ConstByteSpan data)
{
    if (truncate_to_fit_ && offset != 0 && !data.empty()) {
        LOG_ERROR("Truncating sink requires writes at offset 0; got offset ",
                  offset);
        return false;
    }
    return true;
}

bool
zarr::DiscardSink::flush_()
{
    return true;
}
