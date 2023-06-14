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

// TODO(gitbuda): Add more info how to deal with exceptions.
// TODO(gitbuda): An issue here is that the message.hpp also has to be shipped.
// fmt dependency is a further problem.
// PROPOSAL: Format all manually to avoid any additional client complexity.
// #include "utils/message.hpp"

namespace mg_exception {

// TODO(gitbuda): Write explanation why StringSerialize.
template <typename FirstArg, typename... Args>
std::string StringSerialize(FirstArg &&firstArg, Args &&...args) {
  std::stringstream stream;
  stream << firstArg;
  ((stream << " " << args), ...);
  return stream.str();
}

struct NotEnoughMemoryException : public std::exception {
  const char *what() const noexcept override { return "Not enough memory!"; }
};

struct UnknownException : public std::exception {
  const char *what() const noexcept override { return "Unknown exception!"; }
};

struct AllocationException : public std::exception {
  AllocationException()
      : message_(StringSerialize("Could not allocate memory.",
                                 "https://memgraph.com/docs/memgraph/reference-guide/"
                                 "memory-control#controlling-the-memory-usage-of-a-procedure")) {}
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
  InvalidIDException() : message_("Invalid ID!") {}
  explicit InvalidIDException(std::uint64_t identifier) : message_{StringSerialize("Invalid ID =", identifier)} {}
  const char *what() const noexcept override { return message_.c_str(); }

 private:
  std::string message_;
};

struct KeyAlreadyExistsException : public std::exception {
  const char *what() const noexcept override { return "Key you are trying to set already exists!"; }
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
