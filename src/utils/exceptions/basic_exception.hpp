//
// Created by buda on 18/02/17.
//
#pragma once

#include <fmt/format.h>
#include <stdexcept>

/**
 * @brief BasicException
 *
 * Just a wrapper around std::exception.
 */
class BasicException : public std::exception {
 public:
  /** Constructor (C strings).
   *
   *  @param message C-style string error message.
   *                 The string contents are copied upon construction.
   *                 Hence, responsibility for deleting the char* lies
   *                 with the caller.
   */
  explicit BasicException(const char *message) : msg_(message) {}

  /**
   * Constructor (C++ STL strings).
   *
   * @param message The error message.
   */
  explicit BasicException(const std::string &message) : msg_(message) {}

  /** Constructor with format string (C++ STL strings).
   *
   * @param format The error format message.
   * @param args Arguments for format string.
   */
  template <class... Args>
  explicit BasicException(const std::string &format, Args &&... args) noexcept
      : BasicException(fmt::format(format, std::forward<Args>(args)...)) {}

  /** Constructor with format string (C strings).
   *
   * @param format The error format message. The string contents are copied upon
   * construction. Hence, the responsibility for deleting char* lies with the
   * caller.
   * @param args Arguments for format string.
   */
  template <class... Args>
  explicit BasicException(const char *format, Args &&... args) noexcept
      : BasicException(fmt::format(std::string(format),
                                   std::forward<Args>(args)...)) {}

  /**
   * Destructor. Virtual to allow for subclassing.
   */
  virtual ~BasicException() {}

  /**
   * Returns a pointer to the (constant) error description.
   *
   * @return A pointer to a const char*. The underlying memory
   *         is in possession of the BasicException object. Callers must
   *         not attempt to free the memory.
   */
  const char *what() const noexcept override { return msg_.c_str(); }

 protected:
  /**
   * Error message.
   */
  std::string msg_;
};
