#include "file.sink.hh"
#include "macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

zarr::FileSink::FileSink(std::string_view filename)
  : file_(filename.data(), std::ios::binary | std::ios::trunc)
{
}

bool
zarr::FileSink::write(size_t offset, ConstByteSpan data)
{
    const auto bytes_of_buf = data.size();
    if (data.data() == nullptr || bytes_of_buf == 0) {
        return true;
    }

    file_.seekp(offset);
    file_.write(reinterpret_cast<const char*>(data.data()), bytes_of_buf);
    return true;
}

bool
zarr::FileSink::write_vectors(size_t offset,
                              const std::vector<ConstByteSpan>& data)
{
    LOG_ERROR("Not yet implemented.");
    return false;
}

bool
zarr::FileSink::flush_()
{
    file_.flush();
    return true;
}
