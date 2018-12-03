#pragma once
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <vector>

namespace profiler {

struct Package {
  std::string name;
  size_t size;
  size_t capacity;
};

extern std::vector<Package> profiler_info;

inline void WriteToFile(const std::string &file_name) {
  std::ofstream file;
  file.open(file_name);
  for (const auto &package : profiler_info) {
    file << package.name << "\t" << package.size << "\t" << package.capacity
         << std::endl;
  }
  file.close();
}
template <typename T>
class dector : public std::vector<T> {
 private:
  std::string file_;
  int line_;

 public:
  dector(std::string file, int line) : file_(file), line_(line) {}

  ~dector() {
    std::string vector_name = file_ + ":" + std::to_string(line_);
    profiler::Package package{vector_name, this->size(), this->capacity()};
    profiler::profiler_info.push_back(package);
  }

  void SetName(std::string file, int line) {
    this->file_ = file;
    this->line_ = line;
  }
};
}
