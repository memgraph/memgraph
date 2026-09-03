// Copyright 2026 Memgraph Ltd.
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

#include <cstddef>
#include <fstream>
#include <ios>
#include <string>
#include <utility>

#include "utils/exceptions.hpp"

namespace memgraph::utils {

/// A stream of bytes read once, front to back.
///
/// Deliberately narrower than an input stream: there is no seeking and no going back, so a source
/// can be something arriving over a network as easily as a file on disk. Code that must revisit
/// bytes it has already read needs its own buffer, or a different interface.
///
/// A source is read by one consumer at a time.
class ByteSource {
 public:
  ByteSource() = default;
  ByteSource(ByteSource const &) = delete;
  auto operator=(ByteSource const &) -> ByteSource & = delete;
  ByteSource(ByteSource &&) = delete;
  auto operator=(ByteSource &&) -> ByteSource & = delete;
  virtual ~ByteSource() = default;

  /// Reads into `out` and returns how many bytes were read, which may be fewer than `size` even
  /// though more is still to come. Blocks only while there is nothing at all to return. Zero means
  /// the source is exhausted. Throws if the source failed, which is what separates a failure from
  /// an orderly end.
  virtual auto Read(char *out, std::size_t size) -> std::size_t = 0;
};

/// A file on local disk, read through in one pass.
class FileByteSource final : public ByteSource {
 public:
  explicit FileByteSource(std::string path) : path_{std::move(path)}, stream_{path_, std::ios::binary} {
    if (!stream_.is_open()) {
      throw BasicException("Couldn't open file {}", path_);
    }
  }

  auto Read(char *out, std::size_t size) -> std::size_t override {
    stream_.read(out, static_cast<std::streamsize>(size));
    // A failed read reports nothing read, which is indistinguishable from the end of the file. Left
    // alone it would end the load early and call the result complete.
    if (stream_.bad()) [[unlikely]] {
      throw BasicException("Failed to read file {}", path_);
    }
    return static_cast<std::size_t>(stream_.gcount());
  }

 private:
  // Declared before the stream, which is opened from it.
  std::string path_;
  std::ifstream stream_;
};

}  // namespace memgraph::utils
