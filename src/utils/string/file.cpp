#include "utils/string/file.hpp"

namespace utils
{

std::string read_file(const char *filename)
{
    std::ifstream in(filename, std::ios::in | std::ios::binary);

    if (in)
        return std::string(std::istreambuf_iterator<char>(in),
                           std::istreambuf_iterator<char>());

    auto error_message = fmt::format("{0}{1}", "Fail to read: ", filename);
    throw std::runtime_error(error_message);
}

void write_file(const std::string& content, const std::string& path)
{
    std::ofstream stream;
    stream.open(path.c_str());
    stream << content;
    stream.close();
}

}
