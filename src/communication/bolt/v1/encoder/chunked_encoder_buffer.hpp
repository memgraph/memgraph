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

#include "communication/bolt/v1/constants.hpp"

namespace memgraph::communication::bolt {

/**
 * @brief ChunkedEncoderBuffer
 *
 * Has methods for writing data into the chunk buffer.
 *
 * Writing data stores data in the internal buffer and chunking data sends
 * the currently stored data to the OutputStream. Flushing prepends the data
 * length to each chunk.
 *
 * | chunk header | --- chunk --- | another chunk | -- end marker -- |
 * | ------- whole chunk -------- |  whole chunk  | chunk of size 0  |
 *
 * | --------------------------- message --------------------------- |
 * | --------------------------- buffer  --------------------------- |
 *
 * NOTE: To send a message end marker (chunk of size 0) it is necessary to
 * explicitly call the `Flush` method on an empty buffer. In that way the user
 * can control when the message is over and the whole message isn't
 * unnecessarily buffered in memory.
 *
 * The current implementation stores only a single chunk into memory and sends
 * it immediately to the output stream when new data arrives.
 *
 * @tparam TOutputStream the output stream that should be used
 */
template <class TOutputStream>
class ChunkedEncoderBuffer {
 public:
  explicit ChunkedEncoderBuffer(TOutputStream &output_stream) : output_stream_(output_stream) {}

  /**
   * Writes n values into the buffer. If n is bigger than whole chunk size
   * values are automatically chunked and sent to the output buffer.
   *
   * @param values data array of bytes
   * @param n is the number of bytes
   */
  void Write(const uint8_t *values, size_t n) {
    size_t written = 0;

    while (n > 0) {
      // Define the number of bytes which will be copied into the chunk because
      // the internal storage is a fixed length array.
      size_t size = n < kChunkMaxDataSize - have_ ? n : kChunkMaxDataSize - have_;

      // Copy `size` values to the chunk array.
      std::memcpy(chunk_.data() + kChunkHeaderSize + have_, values + written, size);

      // Update positions. The position pointer and incoming size have to be
      // updated because all incoming values have to be processed.
      written += size;
      have_ += size;
      n -= size;

      // If the chunk is full, send it to the output stream and clear the
      // internal storage to make space for other incoming values that are left
      // in the values array.
      if (have_ == kChunkMaxDataSize) Flush(true);
    }
  }

  /**
   * Wrap the data from the chunk array (append the size header) and send
   * the whole chunk into the output stream.
   *
   * @param have_more this parameter is passed to the underlying output stream
   *                  `Write` method to indicate wether we have more data
   *                  waiting to be sent (in order to optimize network packets)
   */
  bool Flush(bool have_more = false) {
    // Write the size of the chunk.
    chunk_[0] = have_ >> 8;
    chunk_[1] = have_ & 0xFF;

    // Write the data to the stream.
    auto ret = output_stream_.Write(chunk_.data(), kChunkHeaderSize + have_, have_more);

    // Cleanup.
    Clear();

    return ret;
  }

  /** Clears the internal buffers. */
  void Clear() { have_ = 0; }

  /**
   * Returns a boolean indicating whether there is data in the buffer.
   * @returns true if there is data in the buffer,
   *          false otherwise
   */
  bool HasData() { return have_ > 0; }

 private:
  // The output stream used.
  TOutputStream &output_stream_;

  // Buffer for a single chunk.
  std::array<uint8_t, kChunkWholeSize> chunk_;

  // Amount of data in chunk array.
  size_t have_{0};
};
}  // namespace memgraph::communication::bolt
