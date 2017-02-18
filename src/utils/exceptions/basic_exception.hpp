//
// Created by buda on 18/02/17.
//
#pragma once

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
  explicit BasicException(const char* message) : msg_(message) {}

  /**
   * Constructor (C++ STL strings).
   *
   * @param message The error message.
   */
  explicit BasicException(const std::string& message) : msg_(message) {}

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
  const char* what() const noexcept override { return msg_.c_str(); }

 protected:
  /**
   * Error message.
   */
  std::string msg_;
};
