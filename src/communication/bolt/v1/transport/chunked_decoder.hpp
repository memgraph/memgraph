#pragma once

#include <cassert>
#include <cstring>
#include <functional>

#include "logging/default.hpp"
#include "utils/exceptions/basic_exception.hpp"
#include "utils/likely.hpp"
#include "utils/types/byte.hpp"

namespace bolt {

template <class Stream>
class ChunkedDecoder {
 public:
  class DecoderError : public BasicException {
   public:
    using BasicException::BasicException;
  };

  ChunkedDecoder(Stream &stream) : stream(stream) {}

  /* Decode chunked data
   *
   * Chunk format looks like:
   *
   * |Header|     Data     ||Header|    Data      || ... || End |
   * |  2B  |  size bytes  ||  2B  |  size bytes  || ... ||00 00|
   */
  bool decode(const byte *&chunk, size_t n) {
    while (n > 0) {
      // get size from first two bytes in the chunk
      auto size = get_size(chunk);

      if (UNLIKELY(size + 2 > n))
        throw DecoderError("Chunk size larger than available data.");

      // advance chunk to pass those two bytes
      chunk += 2;
      n -= 2;

      // if chunk size is 0, we're done!
      if (size == 0) return true;

      stream.get().write(chunk, size);

      chunk += size;
      n -= size;
    }

    return false;
  }

  bool operator()(const byte *&chunk, size_t n) { return decode(chunk, n); }

 private:
  std::reference_wrapper<Stream> stream;

  size_t get_size(const byte *chunk) {
    return size_t(chunk[0]) << 8 | chunk[1];
  }
};
}
