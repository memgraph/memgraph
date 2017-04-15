#pragma once

#include <fmt/format.h>
#include <fmt/ostream.h>
#include <stdexcept>

#include "utils/auto_scope.hpp"
#include "utils/stacktrace/stacktrace.hpp"

class StacktraceException : public std::exception {
 public:
  StacktraceException(const std::string &message) noexcept : message_(message) {
    Stacktrace stacktrace;
    stacktrace_ = stacktrace.dump();
  }

  template <class... Args>
  StacktraceException(const std::string &format, Args &&... args) noexcept
      : StacktraceException(fmt::format(format, std::forward<Args>(args)...)) {}

  template <class... Args>
  StacktraceException(const char *format, Args &&... args) noexcept
      : StacktraceException(fmt::format(std::string(format),
                                        std::forward<Args>(args)...)) {}

  const char *what() const noexcept override { return message_.c_str(); }

  const char *trace() const noexcept { return stacktrace_.c_str(); }

 private:
  std::string message_;
  std::string stacktrace_;
};
