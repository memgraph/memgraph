#pragma once

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include <glog/logging.h>

#include "communication/bolt/v1/constants.hpp"
#include "communication/bolt/v1/decoder/buffer.hpp"
#include "utils/assert.hpp"

namespace communication::bolt {

/**
 * This class is used as the return value of the GetChunk function of the
 * ChunkedDecoderBuffer. It represents the 3 situations that can happen when
 * reading a chunk.
 */
enum class ChunkState : uint8_t {
  // The chunk isn't complete, we have to read more data
  Partial,

  // The chunk is invalid, it's tail isn't 0x00 0x00
  Invalid,

  // The chunk is whole and correct and has been loaded into the buffer
  Whole
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
class ChunkedDecoderBuffer {
 private:
  using StreamBufferT = io::network::StreamBuffer;

 public:
  ChunkedDecoderBuffer(Buffer<> &buffer) : buffer_(buffer) {}

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
  ChunkState GetChunk() {
    uint8_t *data = buffer_.data();
    size_t size = buffer_.size();
    if (size < 2) {
      DLOG(WARNING) << "Size < 2";
      return ChunkState::Partial;
    }

    size_t chunk_size = data[0];
    chunk_size <<= 8;
    chunk_size += data[1];
    if (size < chunk_size + 4) {
      DLOG(WARNING) << fmt::format(
          "Chunk size is {} but only have {} data bytes.", chunk_size, size);
      return ChunkState::Partial;
    }

    if (data[chunk_size + 2] != 0 || data[chunk_size + 3] != 0) {
      DLOG(WARNING) << "Invalid chunk!";
      buffer_.Shift(chunk_size + 4);
      return ChunkState::Invalid;
    }

    pos_ = 0;
    size_ = chunk_size;
    memcpy(data_, data + 2, size - 4);
    buffer_.Shift(chunk_size + 4);

    return ChunkState::Whole;
  }

  /**
   * Gets the size of currently available data in the loaded chunk.
   *
   * @returns size of available data
   */
  size_t Size() { return size_ - pos_; }

 private:
  Buffer<> &buffer_;
  uint8_t data_[MAX_CHUNK_SIZE];
  size_t size_{0};
  size_t pos_{0};
};
}
