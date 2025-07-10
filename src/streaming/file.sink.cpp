#include "file.sink.hh"
#include "macros.hh"

#include <string_view>

void
init_handle(void**, std::string_view);

void
destroy_handle(void**);
bool
seek_and_write(void**, size_t, ConstByteSpan);

bool
flush_file(void**);

zarr::FileSink::FileSink(std::string_view filename)
{
    init_handle(&handle_, filename);
}

zarr::FileSink::~FileSink()
{
    destroy_handle(&handle_);
}

bool
zarr::FileSink::write(size_t offset, ConstByteSpan data)
{
    if (data.data() == nullptr || data.size() == 0) {
        return true;
    }

    return seek_and_write(&handle_, offset, data);
}

bool
zarr::FileSink::flush_()
{
    return flush_file(&handle_);
}