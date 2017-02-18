#pragma once

#include <string>

namespace utils {

class StringBuffer {
 public:
  StringBuffer() = default;
  ~StringBuffer() = default;

  StringBuffer(const StringBuffer &) = delete;
  StringBuffer(StringBuffer &&) = default;

  StringBuffer &operator=(const StringBuffer &) = delete;
  StringBuffer &operator=(StringBuffer &&) = default;

  StringBuffer(std::string::size_type count) { resize(count); }

  void resize(std::string::size_type count) { data.resize(count); }

  StringBuffer &operator<<(const std::string &str) {
    data += str;
    return *this;
  }

  StringBuffer &operator<<(const char *str) {
    data += str;
    return *this;
  }

  StringBuffer &operator<<(char c) {
    data += c;
    return *this;
  }

  std::string &str() { return data; }

 private:
  std::string data;
};
}
