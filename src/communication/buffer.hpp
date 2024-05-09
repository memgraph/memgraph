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

#include <vector>

#include "io/network/stream_buffer.hpp"

namespace memgraph::communication {

/**
 * @brief Buffer
 *
 * Has methods for writing and reading raw data.
 *
 * Allocating, writing and written stores data in the buffer. The stored
 * data can then be read using the pointer returned with the data function.
 * This implementation stores data in a variable sized array (a vector).
 * The internal array can only grow in size.
 *
 * This buffer is NOT thread safe. It is intended to be used in the network
 * stack where all execution when it is being done is being done on a single
 * thread.
 */
class Buffer final {
 private:
  // Initial capacity of the internal buffer.
  static constexpr size_t kBufferInitialSize = 65'536;

 public:
  Buffer();

  Buffer(const Buffer &) = delete;
  Buffer(Buffer &&) = delete;
  Buffer &operator=(const Buffer &) = delete;
  Buffer &operator=(Buffer &&) = delete;
  ~Buffer() = default;

  /**
   * This class provides all functions from the buffer that are needed to allow
   * reading data from the buffer.
   */
  class ReadEnd {
   public:
    explicit ReadEnd(Buffer *buffer);

    ReadEnd(const ReadEnd &) = delete;
    ReadEnd(ReadEnd &&) = delete;
    ReadEnd &operator=(const ReadEnd &) = delete;
    ReadEnd &operator=(ReadEnd &&) = delete;
    ~ReadEnd() = default;

    uint8_t *data();

    size_t size() const;

    void Shift(size_t len);

    void Resize(size_t len);

    void Clear();

    void ShrinkBuffer(size_t size);

   private:
    Buffer *buffer_;
  };

  /**
   * This class provides all functions from the buffer that are needed to allow
   * writing data to the buffer.
   */
  class WriteEnd {
   public:
    explicit WriteEnd(Buffer *buffer);

    WriteEnd(const WriteEnd &) = delete;
    WriteEnd(WriteEnd &&) = delete;
    WriteEnd &operator=(const WriteEnd &) = delete;
    WriteEnd &operator=(WriteEnd &&) = delete;
    ~WriteEnd() = default;

    io::network::StreamBuffer Allocate();

    void Written(size_t len);

    void Resize(size_t len);

    void Clear();

   private:
    Buffer *buffer_;
  };

  /**
   * This function returns a pointer to the associated ReadEnd object for this
   * buffer.
   */
  ReadEnd *read_end();

  /**
   * This function returns a pointer to the associated WriteEnd object for
   * this buffer.
   */
  WriteEnd *write_end();

 private:
  /**
   * This function returns a pointer to the internal buffer. It is used for
   * reading data from the buffer.
   */
  uint8_t *data();

  /**
   * This function returns the size of available data for reading.
   */
  size_t size() const;

  /**
   * This method shifts the available data for len. It is used when you read
   * some data from the buffer and you want to remove it from the buffer.
   *
   * @param len the length of data that has to be removed from the start of
   *            the buffer
   */
  void Shift(size_t len);

  /**
   * Allocates a new StreamBuffer from the internal buffer.
   * This function returns a pointer to the first currently free memory
   * location in the internal buffer. Also, it returns the size of the
   * available memory.
   */
  io::network::StreamBuffer Allocate();

  /**
   * This method is used to notify the buffer that the data has been written.
   * To write data to this buffer you should do this:
   * Call Allocate(), then write to the returned data pointer.
   * IMPORTANT: Don't write more data then the returned size, you will cause
   * a memory overflow. Then call Written(size) with the length of data that
   * you have written into the buffer.
   *
   * @param len the size of data that has been written into the buffer
   */
  void Written(size_t len);

  /**
   * This method resizes the internal data buffer.
   * It is used to notify the buffer of the incoming message size.
   * If the requested size is larger than the buffer size then the buffer is
   * resized, if the requested size is smaller than the buffer size then
   * nothing is done.
   *
   * @param len the desired size of the buffer
   */
  void Resize(size_t len);

  /**
   * This method clears the buffer. It doesn't release the underlying storage
   * space.
   */
  void Clear();

  /**
   * This method resizes the internal data buffer.
   * It can only shrink the buffer to the larger of size and len
   */
  void ShrinkBuffer(size_t new_size);

  std::vector<uint8_t> data_;
  size_t have_{0};
  ReadEnd read_end_;
  WriteEnd write_end_;
};
}  // namespace memgraph::communication
