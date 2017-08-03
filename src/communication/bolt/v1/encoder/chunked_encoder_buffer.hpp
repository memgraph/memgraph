#pragma once

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include <glog/logging.h>

#include "communication/bolt/v1/constants.hpp"
#include "utils/assert.hpp"
#include "utils/bswap.hpp"

namespace communication::bolt {

// TODO: implement a better flushing strategy + optimize memory allocations!
// TODO: see how bolt splits message over more TCP packets
//       -> test for more TCP packets!

/**
 * @brief ChunkedEncoderBuffer
 *
 * Has methods for writing and flushing data into the message buffer.
 *
 * Writing data stores data in the internal buffer and flushing data sends
 * the currently stored data to the Socket. Chunking prepends data length and
 * appends chunk end marker (0x00 0x00).
 *
 * | chunk header | --- chunk --- | end marker | ---------- another chunk ... |
 * | ------------- whole chunk ----------------| ---------- another chunk ... |
 *
 * | ------------------------ message --------------------------------------- |
 * | ------------------------ buffer  --------------------------------------- |
 *
 * The current implementation stores the whole message into a single buffer
 * which is std::vector.
 *
 * @tparam Socket the output socket that should be used
 */
template <class Socket>
class ChunkedEncoderBuffer {
 public:
  ChunkedEncoderBuffer(Socket &socket) : socket_(socket) {}

  /**
   * Writes n values into the buffer. If n is bigger than whole chunk size
   * values are automatically chunked.
   *
   * @param values data array of bytes
   * @param n is the number of bytes
   */
  void Write(const uint8_t *values, size_t n) {
    while (n > 0) {
      // Define number of bytes  which will be copied into chunk because
      // chunk is a fixed length array.
      auto size = n < MAX_CHUNK_SIZE + CHUNK_HEADER_SIZE - pos_
                      ? n
                      : MAX_CHUNK_SIZE + CHUNK_HEADER_SIZE - pos_;

      // Copy size values to chunk array.
      std::memcpy(chunk_.data() + pos_, values, size);

      // Update positions. Position pointer and incomming size have to be
      // updated because all incomming values have to be processed.
      pos_ += size;
      n -= size;

      // If chunk is full copy it into the message buffer and make space for
      // other incomming values that are left in the values array.
      if (pos_ == CHUNK_HEADER_SIZE + MAX_CHUNK_SIZE) Chunk();
    }
  }

  /**
   * Wrap the data from chunk array (append header and end marker) and put
   * the whole chunk into the buffer.
   */
  void Chunk() {
    // 1. Write the size of the chunk (CHUNK HEADER).
    uint16_t size = pos_ - CHUNK_HEADER_SIZE;
    // Write the higher byte.
    chunk_[0] = size >> 8;
    // Write the lower byte.
    chunk_[1] = size & 0xFF;

    // 2. Write last two bytes in the whole chunk (CHUNK END MARKER).
    // The last two bytes are always 0x00 0x00.
    chunk_[pos_++] = 0x00;
    chunk_[pos_++] = 0x00;

    debug_assert(pos_ <= WHOLE_CHUNK_SIZE,
                 "Internal variable pos_ is bigger than the whole chunk size.");

    // 3. Remember first chunk size.
    if (first_chunk_size_ == -1) first_chunk_size_ = pos_;

    // 4. Copy whole chunk into the buffer.
    size_ += pos_;
    buffer_.reserve(size_);
    std::copy(chunk_.begin(), chunk_.begin() + pos_,
              std::back_inserter(buffer_));

    // 5. Cleanup.
    //     * pos_ has to be reset to the size of chunk header (reserved
    //       space for the chunk size)
    pos_ = CHUNK_HEADER_SIZE;
  }

  /**
   * Sends the whole buffer(message) to the client.
   * @returns true if the data was successfully sent to the client
   *          false otherwise
   */
  bool Flush() {
    // Call chunk if is hasn't been called.
    if (pos_ > CHUNK_HEADER_SIZE) Chunk();

    // Early return if buffer is empty because there is nothing to write.
    if (size_ == 0) return true;

    // Flush the whole buffer.
    if (!socket_.Write(buffer_.data() + offset_, size_ - offset_)) return false;
    DLOG(INFO) << "Flushed << " << size_ << " bytes.";

    // Cleanup.
    Clear();
    return true;
  }

  /**
   * Sends only the first message chunk in the buffer to the client.
   * @returns true if the data was successfully sent to the client
   *          false otherwise
   */
  bool FlushFirstChunk() {
    // Call chunk if is hasn't been called.
    if (pos_ > CHUNK_HEADER_SIZE) Chunk();

    // Early return if buffer is empty because there is nothing to write.
    if (size_ == 0) return false;

    // Early return if there is no first chunk
    if (first_chunk_size_ == -1) return false;

    // Flush the first chunk
    if (!socket_.Write(buffer_.data(), first_chunk_size_)) return false;
    DLOG(INFO) << "Flushed << " << first_chunk_size_ << " bytes.";

    // Cleanup.
    // Here we use offset as a method of deleting from the front of the
    // data vector. Because the first chunk will always be relatively
    // small comparing to the rest of the data it is more optimal just to
    // skip the first part of the data than to shift everything in the
    // vector buffer.
    offset_ = first_chunk_size_;
    first_chunk_size_ = -1;
    return true;
  }

  /**
   * Clears the internal buffers.
   */
  void Clear() {
    buffer_.clear();
    size_ = 0;
    first_chunk_size_ = -1;
    offset_ = 0;
  }

  /**
   * Returns a boolean indicating whether there is data in the buffer.
   * @returns true if there is data in the buffer,
   *          false otherwise
   */
  bool HasData() { return buffer_.size() > 0 || size_ > 0; }

 private:
  /**
   * A client socket.
   */
  Socket &socket_;

  /**
   * Buffer for a single chunk.
   */
  std::array<uint8_t, WHOLE_CHUNK_SIZE> chunk_;

  /**
   * Buffer for the message which will be sent to a client.
   */
  std::vector<uint8_t> buffer_;

  /**
   * Size of the message.
   */
  size_t size_{0};

  /**
   * Size of first chunk in the buffer.
   */
  int32_t first_chunk_size_{-1};

  /**
   * Offset from the start of the buffer.
   */
  size_t offset_{0};

  /**
   * Current position in chunk array.
   */
  size_t pos_{CHUNK_HEADER_SIZE};
};
}
