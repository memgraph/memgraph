#pragma once

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include "glog/logging.h"

#include "communication/bolt/v1/constants.hpp"
#include "io/network/stream_buffer.hpp"
#include "utils/bswap.hpp"

namespace communication::bolt {

/**
 * @brief Buffer
 *
 * Has methods for writing and reading raw data.
 *
 * Allocating, writing and written stores data in the buffer. The stored
 * data can then be read using the pointer returned with the data function.
 * The current implementation stores data in a single fixed length buffer.
 *
 * @tparam Size the size of the internal byte array, defaults to the maximum
 *         size of a chunk in the Bolt protocol
 */
template <size_t Size = WHOLE_CHUNK_SIZE>
class Buffer {
 private:
  using StreamBufferT = io::network::StreamBuffer;

 public:
  Buffer() = default;

  /**
   * Allocates a new StreamBuffer from the internal buffer.
   * This function returns a pointer to the first currently free memory
   * location in the internal buffer. Also, it returns the size of the
   * available memory.
   */
  StreamBufferT Allocate() {
    return StreamBufferT{&data_[size_], Size - size_};
  }

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
  void Written(size_t len) {
    size_ += len;
    DCHECK(size_ <= Size) << "Written more than storage has space!";
  }

  /**
   * This method shifts the available data for len. It is used when you read
   * some data from the buffer and you want to remove it from the buffer.
   *
   * @param len the length of data that has to be removed from the start of
   *            the buffer
   */
  void Shift(size_t len) {
    DCHECK(len <= size_) << "Tried to shift more data than the buffer has!";
    memmove(data_, data_ + len, size_ - len);
    size_ -= len;
  }

  /**
   * This method clears the buffer.
   */
  void Clear() { size_ = 0; }

  /**
   * This function returns a pointer to the internal buffer. It is used for
   * reading data from the buffer.
   */
  uint8_t *data() { return data_; }

  /**
   * This function returns the size of available data for reading.
   */
  size_t size() { return size_; }

 private:
  uint8_t data_[Size];
  size_t size_{0};
};
}  // namespace communication::bolt
