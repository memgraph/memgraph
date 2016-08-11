#pragma once

#include <fstream>
#include <ostream>
#include <streambuf>
#include <string>
#include <cerrno>
#include <stdexcept>

#include <fmt/format.h>

namespace utils
{

std::string read_file(const char *filename);

void write_file(const std::string& content, const std::string& path);

}
