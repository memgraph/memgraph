// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <exception>
#include <iostream>
#include <sstream>
#include <string>

namespace mg_exception {

// Instead of writing this utility function, we could have used `fmt::format`, but that's not an ideal option here
// because that would introduce dependency that would be propagated to the client code (if exceptions here would be
// used). Since the functionality here is not complex + the code is not on a critical path, we opted for a pure C++
// solution.
template <typename FirstArg, typename... Args>
std::string StringSerialize(FirstArg &&firstArg, Args &&...args) {
  std::stringstream stream;
  stream << std::forward<FirstArg>(firstArg);
  ((stream << " " << std::forward<Args>(args)), ...);
  return stream.str();
}

struct UnknownException : public std::exception {
  const char *what() const noexcept override { return "Unknown exception!"; }
};

struct NotEnoughMemoryException : public std::exception {
  NotEnoughMemoryException()
      : message_{
            StringSerialize("Not enough memory! For more details please visit", "https://memgr.ph/memory-control")} {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

struct AllocationException : public std::exception {
  AllocationException()
      : message_{StringSerialize("Could not allocate memory. For more details please visit",
                                 "https://memgr.ph/memory-control")} {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

struct InsufficientBufferException : public std::exception {
  const char *what() const noexcept override { return "Buffer is not sufficient to process procedure!"; }
};

struct OutOfRangeException : public std::exception {
  const char *what() const noexcept override { return "Index out of range!"; }
};

struct LogicException : public std::exception {
  const char *what() const noexcept override { return "Logic exception, check the procedure signature!"; }
};

struct DeletedObjectException : public std::exception {
  const char *what() const noexcept override { return "Object is deleted!"; }
};

struct InvalidArgumentException : public std::exception {
  const char *what() const noexcept override { return "Invalid argument!"; }
};

struct InvalidIDException : public std::exception {
  InvalidIDException() : message_{"Invalid ID!"} {}
  explicit InvalidIDException(std::uint64_t identifier) : message_{StringSerialize("Invalid ID =", identifier)} {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

struct KeyAlreadyExistsException : public std::exception {
  KeyAlreadyExistsException() : message_{"Key you are trying to set already exists!"} {}
  explicit KeyAlreadyExistsException(const std::string &key)
      : message_{StringSerialize("Key you are trying to set already exists! KEY = ", key)} {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

struct ImmutableObjectException : public std::exception {
  const char *what() const noexcept override { return "Object you are trying to change is immutable!"; }
};

struct ValueConversionException : public std::exception {
  const char *what() const noexcept override { return "Error in value conversion!"; }
};

struct SerializationException : public std::exception {
  const char *what() const noexcept override { return "Error in serialization!"; }
};

}  // namespace mg_exception
