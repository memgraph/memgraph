#pragma once

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/decoder/buffer.hpp"
#include "logging/loggable.hpp"
#include "utils/assert.hpp"

namespace communication::bolt {

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
class ChunkedDecoderBuffer : public Loggable {
 private:
  using StreamBufferT = io::network::StreamBuffer;

 public:
  ChunkedDecoderBuffer(Buffer &buffer) : Loggable("ChunkedDecoderBuffer"), buffer_(buffer) {}

  /**
   * Reads data from the internal buffer.
   *
   * @param data a pointer to where the data should be read
   * @param len the length of data that should be read
   * @returns true if exactly len of data was copied into data,
   *          false otherwise
   */
  bool Read(uint8_t *data, size_t len) {
    if (len > size_ - pos_) return false;
    memcpy(data, &data_[pos_], len);
    pos_ += len;
    return true;
  }

  /**
   * Gets a chunk from the underlying raw data buffer.
   * When getting a chunk this function validates the chunk.
   * If the chunk isn't yet finished the function just returns false.
   * If the chunk is finished (all data has been read) and the chunk isn't
   * valid, then the function automatically deletes the invalid chunk
   * from the underlying buffer and returns false.
   *
   * @returns true if a chunk was successfully copied into the internal
   *          buffer, false otherwise
   */
  bool GetChunk() {
    uint8_t *data = buffer_.data();
    size_t size = buffer_.size();
    if (size < 2) {
      logger.trace("Size < 2");
      return false;
    }

    size_t chunk_size = data[0];
    chunk_size <<= 8;
    chunk_size += data[1];
    if (size < chunk_size + 4) {
      logger.trace("Chunk size is {} but only have {} data bytes.", chunk_size, size);
      return false;
    }

    if (data[chunk_size + 2] != 0 || data[chunk_size + 3] != 0) {
      logger.trace("Invalid chunk!");
      buffer_.Shift(chunk_size + 4);
      // TODO: raise an exception!
      return false;
    }

    pos_ = 0;
    size_ = chunk_size;
    memcpy(data_, data + 2, size - 4);
    buffer_.Shift(chunk_size + 4);

    return true;
  }

 private:
  Buffer &buffer_;
  uint8_t data_[MAX_CHUNK_SIZE];
  size_t size_{0};
  size_t pos_{0};
};
}
