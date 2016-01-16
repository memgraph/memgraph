#pragma once

#include <fstream>
#include <streambuf>
#include <string>
#include <cerrno>

std::string read_file(const char *filename)
{
    std::ifstream in(filename, std::ios::in | std::ios::binary);
    if (in)
        return std::string(std::istreambuf_iterator<char>(in),
                           std::istreambuf_iterator<char>());
    throw(errno);
}
