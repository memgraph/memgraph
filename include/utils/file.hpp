#pragma once

#include <fstream>

namespace utils
{

inline bool fexists(const char *filename)
{
  std::ifstream ifile(filename);
  return (bool)ifile;
}

inline bool fexists(const std::string& filename)
{
  std::ifstream ifile(filename.c_str());
  return (bool)ifile;
}

}
