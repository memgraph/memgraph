#pragma once

#include <openssl/ssl.h>

#include "io/network/stream_reader.hpp"
#include "logging/default.hpp"

namespace io {
using namespace memory::literals;

template <class Derived, class Stream>
class SecureStreamReader : public StreamReader<Derived, Stream> {
 public:
  struct Buffer {
    char* ptr;
    size_t len;
  };

  SecureStreamReader(uint32_t flags = 0)
      : StreamReader<Derived, Stream>(flags) {}

  void on_data(Stream& stream) {
    while (true) {
      // allocate the buffer to fill the data
      auto buf = this->derived().on_alloc(stream);

      // read from the buffer at most buf.len bytes
      auto len = stream.socket.read(buf.ptr, buf.len);

      if (LIKELY(len > 0)) {
        buf.len = len;
        return this->derived().on_read(stream, buf);
      }

      auto err = stream.socket.error(len);

      // the socket is not ready for reading yet
      if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE ||
          err == SSL_ERROR_WANT_X509_LOOKUP) {
        return;
      }

      // the socket notified a close event
      if (err == SSL_ERROR_ZERO_RETURN) return stream.close();

      // some other error occurred, check errno
      return this->derived().on_error(stream);
    }
  }
};
}
