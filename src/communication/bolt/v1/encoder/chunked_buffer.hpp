#pragma once

#include <cstring>
#include <memory>
#include <vector>
#include <algorithm>

#include "communication/bolt/v1/config.hpp"
#include "logging/default.hpp"
#include "utils/types/byte.hpp"
#include "utils/bswap.hpp"

namespace communication::bolt {

// maximum chunk size = 65536 bytes data
static constexpr size_t CHUNK_SIZE = 65536;

/**
 * Bolt chunked buffer.
 * Has methods for writing and flushing data.
 * Writing data stores data in the internal buffer and flushing data sends
 * the currently stored data to the Socket with prepended data length and
 * appended chunk tail (0x00 0x00).
 *
 * @tparam Socket the output socket that should be used
 */
template <class Socket>
class ChunkedBuffer {
 public:
  ChunkedBuffer(Socket &socket) : socket_(socket), logger_(logging::log->logger("Chunked Buffer")) {}

  void Write(const uint8_t* values, size_t n) {
    logger_.trace("Write {} bytes", n);

    // total size of the buffer is now bigger for n
    size_ += n;

    // reserve enough space for the new data
    buffer_.reserve(size_);

    // copy new data
    std::copy(values, values + n, std::back_inserter(buffer_));
  }

  void Flush() {
    size_t size = buffer_.size(), n = 0, pos = 0;
    uint16_t head;

    while (size > 0) {
      head = n = std::min(CHUNK_SIZE, size);
      head = bswap(head);

      logger_.trace("Flushing chunk of {} bytes", n);

      // TODO: implement better flushing strategy!
      socket_.Write(reinterpret_cast<const uint8_t *>(&head), sizeof(head));
      socket_.Write(buffer_.data() + pos, n);

      head = 0;
      socket_.Write(reinterpret_cast<const uint8_t *>(&head), sizeof(head));

      size -= n;
      pos += n;
    }

    // GC
    // TODO: impelement a better strategy
    buffer_.clear();

    // clear size
    size_ = 0;
  }

 private:
  Socket& socket_;
  Logger logger_;
  std::vector<uint8_t> buffer_;
  size_t size_{0};
};
}
