#pragma once

#include <string>

#include "utils/numerics/ceil.hpp"

class Buffer {
 public:
  Buffer(size_t capacity, size_t chunk_size)
      : capacity(capacity), chunk_size(chunk_size) {}

  Buffer& append(const std::string& string) {
    return this->append(string.c_str(), string.size());
  }

  Buffer& append(const char* string, size_t n) {
    auto new_size = size() + n;

    if (capacity < new_size) {
      capacity = new_size;
      data = static_cast<char*>(realloc(data, new_size));
    }

    size = new_size;
  }

  Buffer& operator<<(const std::string& string) {}

  size_t size() const { return str.size(); }

 private:
  size_t size_, capacity, chunk_size;
  char* data;
};
