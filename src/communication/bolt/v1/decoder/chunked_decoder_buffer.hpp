// Copyright 2024 Memgraph Ltd.
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

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include <fmt/format.h>

#include "communication/bolt/v1/constants.hpp"

namespace memgraph::communication::bolt {

/**
 * This class is used as the return value of the GetChunk function of the
 * ChunkedDecoderBuffer. It represents the 3 situations that can happen when
 * reading a chunk.
 */
enum class ChunkState : uint8_t {
  // The chunk isn't complete, we have to read more data
  Partial,

  // The chunk is whole and correct and has been loaded into the buffer
  Whole,

  // The chunk size is 0 meaning that the message is done
  Done
};

/**
 * @brief ChunkedDecoderBuffer
 *
 * Has methods for getting chunks and reading their data.
 *
 * Getting a chunk copies the chunk into the internal buffer from which
 * the data can then be read. While getting a chunk the buffer checks the
 * chunk for validity and then copies only data from the chunk. The headers
 * aren't copied so that the decoder can read only the raw encoded data.
 */
template <typename TBuffer>
class ChunkedDecoderBuffer {
 public:
  explicit ChunkedDecoderBuffer(TBuffer &buffer) : buffer_(buffer) { data_.reserve(kChunkMaxDataSize); }

  /**
   * Reads data from the internal buffer.
   *
   * @param data a pointer to where the data should be read
   * @param len the length of data that should be read
   * @returns true if exactly len of data was copied into data,
   *          false otherwise
   */
  bool Read(uint8_t *data, size_t len) {
    if (len > Size()) return false;
    memcpy(data, &data_[pos_], len);
    pos_ += len;
    if (Size() == 0) {
      pos_ = 0;
      data_.clear();
    }
    return true;
  }

  /**
   * Peeks data from the internal buffer.
   * Reads data, but doesn't remove it from the buffer.
   *
   * @param data a pointer to where the data should be read
   * @param len the length of data that should be read
   * @param offset offset from the beginning of the data
   * @returns true if exactly len of data was copied into data,
   *          false otherwise
   */
  bool Peek(uint8_t *data, size_t len, size_t offset = 0) {
    if (len + offset > Size()) return false;
    memcpy(data, &data_[pos_ + offset], len);
    return true;
  }

  /**
   * Gets a chunk from the underlying raw data buffer.
   *
   * @returns ChunkState::Partial if the chunk isn't whole
   *          ChunkState::Whole if the chunk is whole
   *          ChunkState::Done if the chunk size is 0 (that signals that the
   *                           message is whole)
   */
  ChunkState GetChunk() {
    uint8_t *data = buffer_.data();
    size_t size = buffer_.size();

    if (size < 2) {
      return ChunkState::Partial;
    }

    size_t chunk_size = data[0];
    chunk_size <<= 8;
    chunk_size += data[1];

    if (chunk_size == 0) {
      // The message is done.
      buffer_.Shift(2);
      return ChunkState::Done;
    }

    if (size < chunk_size + 2) {
      return ChunkState::Partial;
    }

    std::copy(data + 2, data + chunk_size + 2, std::back_inserter(data_));
    buffer_.Shift(chunk_size + 2);

    return ChunkState::Whole;
  }

  /**
   * Gets the size of currently available data in the loaded chunk.
   *
   * @returns size of available data
   */
  size_t Size() { return data_.size() - pos_; }

 private:
  TBuffer &buffer_;
  std::vector<uint8_t> data_;
  size_t pos_{0};
};
}  // namespace memgraph::communication::bolt
