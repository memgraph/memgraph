// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/**
 * @file
 * @brief This file stores the common exceptions used across the project.
 */
#pragma once

#include <exception>
#include <string_view>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include "utils/stacktrace.hpp"

namespace utils {

/**
 * @brief Base class for all regular exceptions.
 *
 * All custom exceptions should inherit from this class. It stores the message
 * with which it was constructed. To retrieve the message, use
 * @c BasicException::what method. In case you need to store a stack trace, you
 * should use @c StacktraceException
 *
 * @sa StacktraceException
 * @sa NotYetImplemented
 */
class BasicException : public std::exception {
 public:
  /**
   * @brief Constructor (C strings).
   *
   * @param message C-style string error message.
   *                The string contents are copied upon construction.
   *                Hence, responsibility for deleting the `char*` lies
   *                with the caller.
   */
  explicit BasicException(const char *message) noexcept : msg_(message) {}

  /**
   * @brief Constructor (C++ STL strings).
   *
   * @param message The error message.
   */
  explicit BasicException(const std::string &message) noexcept : msg_(message) {}

  /**
   * @brief Constructor (C++ STL string_view).
   *
   * @param message The error message.
   */
  explicit BasicException(const std::string_view message) noexcept : msg_(message) {}

  /**
   * @brief Constructor with format string (C++ STL strings).
   *
   * @param format The error format message.
   * @param args Arguments for format string.
   */
  template <class... Args>
  explicit BasicException(const std::string &format, Args &&...args) noexcept
      : BasicException(fmt::format(format, std::forward<Args>(args)...)) {}

  /**
   * @brief Constructor with format string (C strings).
   *
   * @param format The error format message. The string contents are copied upon
   * construction. Hence, the responsibility for deleting `char*` lies with the
   * caller.
   * @param args Arguments for format string.
   */
  template <class... Args>
  explicit BasicException(const char *format, Args &&...args) noexcept
      : BasicException(fmt::format(std::string(format), std::forward<Args>(args)...)) {}

  /**
   * @brief Virtual destructor to allow for subclassing.
   */
  virtual ~BasicException() {}

  /**
   * @brief Returns a pointer to the (constant) error description.
   *
   * @return A pointer to a `const char*`. The underlying memory
   *         is in possession of the @c BasicException object. Callers must
   *         not attempt to free the memory.
   */
  const char *what() const noexcept override { return msg_.c_str(); }

 protected:
  /**
   * @brief Error message.
   */
  std::string msg_;
};

/**
 * @brief Base class for all exceptions which need to store the stack trace.
 *
 * When you need to store the stack trace from the place this exception is
 * created, you should use this class. The error message can be obtained using
 * @c StacktraceException::what method, while the stack trace itself can be
 * obtained via @c StacktraceException::trace method. If the stack trace is not
 * needed, you should use @c BasicException.
 *
 * @sa BasicException
 * @sa NotYetImplemented
 */
class StacktraceException : public std::exception {
 public:
  /**
   * @brief Constructor (C strings).
   *
   * @param message C-style string error message.
   *                The string contents are copied upon construction.
   *                Hence, responsibility for deleting the `char*` lies
   *                with the caller.
   */
  explicit StacktraceException(const char *message) noexcept : message_(message), stacktrace_(Stacktrace().dump()) {}

  /**
   * @brief Constructor (C++ STL strings).
   *
   * @param message The error message.
   */
  explicit StacktraceException(const std::string &message) noexcept
      : message_(message), stacktrace_(Stacktrace().dump()) {}

  /**
   * @brief Constructor with format string (C++ STL strings).
   *
   * @param format The error format message.
   * @param args Arguments for format string.
   */
  template <class... Args>
  explicit StacktraceException(const std::string &format, Args &&...args) noexcept
      : StacktraceException(fmt::format(format, std::forward<Args>(args)...)) {}

  /**
   * @brief Constructor with format string (C strings).
   *
   * @param format The error format message. The string contents are copied upon
   * construction. Hence, the responsibility for deleting `char*` lies with the
   * caller.
   * @param args Arguments for format string.
   */
  template <class... Args>
  explicit StacktraceException(const char *format, Args &&...args) noexcept
      : StacktraceException(fmt::format(std::string(format), std::forward<Args>(args)...)) {}

  /**
   * @brief Virtual destructor to allow for subclassing.
   */
  virtual ~StacktraceException() {}

  /**
   * @brief Returns a pointer to the (constant) error description.
   *
   * @return A pointer to a `const char*`. The underlying memory
   *         is in possession of the @c StacktraceException object. Callers must
   *         not attempt to free the memory.
   */
  const char *what() const noexcept override { return message_.c_str(); }

  /**
   * @brief Returns a pointer to the (constant) stack trace message.
   *
   * @return A pointer to a `const char*`. The underlying memory
   *         is in possession of the @c StacktraceException object. Callers must
   *         not attempt to free the memory.
   */
  const char *trace() const noexcept { return stacktrace_.c_str(); }

 private:
  std::string message_;
  std::string stacktrace_;
};

/**
 * @brief Raise this exception for functionality which is yet to be implemented.
 */
class NotYetImplemented final : public BasicException {
 public:
  explicit NotYetImplemented(const std::string &what) noexcept : BasicException("Not yet implemented: " + what) {}

  template <class... Args>
  explicit NotYetImplemented(const std::string &format, Args &&...args) noexcept
      : NotYetImplemented(fmt::format(format, std::forward<Args>(args)...)) {}
};

}  // namespace utils
