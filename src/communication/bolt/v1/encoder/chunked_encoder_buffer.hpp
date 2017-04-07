#pragma once

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include "communication/bolt/v1/bolt_exception.hpp"
#include "communication/bolt/v1/constants.hpp"
#include "logging/loggable.hpp"
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
class ChunkedEncoderBuffer : public Loggable {
 public:
  ChunkedEncoderBuffer(Socket &socket) : Loggable("Chunked Encoder Buffer"), socket_(socket) {}

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
      // chunk is a fixed lenght array.
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

    // 3. Copy whole chunk into the buffer.
    size_ += pos_;
    buffer_.reserve(size_);
    std::copy(chunk_.begin(), chunk_.begin() + pos_,
              std::back_inserter(buffer_));

    // 4. Cleanup.
    //     * pos_ has to be reset to the size of chunk header (reserved
    //       space for the chunk size)
    pos_ = CHUNK_HEADER_SIZE;
  }

  /**
   * Sends the whole buffer(message) to the client.
   */
  void Flush() {
    // Call chunk if is hasn't been called.
    if (pos_ > CHUNK_HEADER_SIZE) Chunk();

    // Early return if buffer is empty because there is nothing to write.
    if (size_ == 0) return;

    // Flush the whole buffer.
    bool written = socket_.Write(buffer_.data(), size_);
    if (!written) throw BoltException("Socket write failed!");
    logger.trace("Flushed {} bytes.", size_);

    // Cleanup.
    buffer_.clear();
    size_ = 0;
  }

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
   * Current position in chunk array.
   */
  size_t pos_{CHUNK_HEADER_SIZE};
};
}
