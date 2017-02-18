#pragma once

#include <cstring>
#include <memory>
#include <vector>

#include "communication/bolt/v1/config.hpp"
#include "logging/default.hpp"
#include "utils/types/byte.hpp"

namespace bolt {

template <class Stream>
class ChunkedBuffer {
  static constexpr size_t C = bolt::config::C; /* chunk size */

 public:
  ChunkedBuffer(Stream &stream) : stream(stream) {
    logger = logging::log->logger("Chunked Buffer");
  }

  void write(const byte *values, size_t n) {
    logger.trace("Write {} bytes", n);

    // total size of the buffer is now bigger for n
    size += n;

    // reserve enough spece for the new data
    buffer.reserve(size);

    // copy new data
    std::copy(values, values + n, std::back_inserter(buffer));
  }

  void flush() {
    stream.get().write(&buffer.front(), size);

    logger.trace("Flushed {} bytes", size);

    // GC
    // TODO: impelement a better strategy
    buffer.clear();

    // reset size
    size = 0;
  }

  ~ChunkedBuffer() {}

 private:
  Logger logger;
  // every new stream.write creates new TCP package
  std::reference_wrapper<Stream> stream;
  std::vector<byte> buffer;
  size_t size{0};
};
}
