#include "file.sink.hh"
#include "macros.hh"

#include <filesystem>

namespace fs = std::filesystem;

zarr::FileSink::FileSink(std::string_view filename, bool truncate)
  : file_(filename.data(),
          truncate ? (std::ios::binary | std::ios::trunc) : std::ios::binary)
{
    EXPECT(file_.is_open(), "Failed to open file ", filename);
}

bool
zarr::FileSink::write(size_t offset, std::span<const std::byte> data)
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
zarr::FileSink::flush_()
{
    file_.flush();
    return true;
}
