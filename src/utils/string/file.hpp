#pragma once

#include <fstream>
#include <ostream>
#include <streambuf>
#include <string>
#include <cerrno>

namespace utils
{

std::string read_file(const char *filename)
{
    std::ifstream in(filename, std::ios::in | std::ios::binary);
    if (in)
        return std::string(std::istreambuf_iterator<char>(in),
                           std::istreambuf_iterator<char>());
    throw(errno);
}

void write_file(const std::string& content, const std::string& path)
{
    std::ofstream stream;
    stream.open(path.c_str());
    stream << content;
    stream.close();
}

}
