#include "sink.hh"
#include "macros.hh"

bool
zarr::finalize_sink(std::unique_ptr<zarr::Sink>&& sink)
{
    if (sink == nullptr) {
        LOG_INFO("Sink is null. Nothing to finalize.");
        return true;
    }

    if (!sink->flush_()) {
        return false;
    }

    sink.reset();
    return true;
}