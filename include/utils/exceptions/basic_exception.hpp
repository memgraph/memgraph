#pragma once

#include <fmt/format.h>
#include <stdexcept>

#include "utils/auto_scope.hpp"
#include "utils/stacktrace.hpp"

class BasicException : public std::exception {
 public:
  BasicException(const std::string &message, uint64_t stacktrace_size) noexcept
      : message_(message),
        stacktrace_size_(stacktrace_size) {}
  BasicException(const std::string &message) noexcept : message_(message),
                                                        stacktrace_size_(10) {}

  template <class... Args>
  BasicException(const std::string &format, Args &&... args) noexcept
      : BasicException(fmt::format(format, std::forward<Args>(args)...)) {}

  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
  uint64_t stacktrace_size_;

#ifndef NDEBUG
  void generate_stacktrace() {
    Stacktrace stacktrace;

    int size = std::min(stacktrace_size_, stacktrace.size());
    for (int i = 0; i < size; i++) {
      message_.append(fmt::format("\n at {} ({})", stacktrace[i].function,
                                  stacktrace[i].location));
    }
  }
#endif
};
