#pragma once

#include "io/network/stream_listener.hpp"
#include "memory/literals.hpp"

namespace io {
using namespace memory::literals;

template <class Derived, class Stream>
class StreamReader : public StreamListener<Derived, Stream> {
 public:
  struct Buffer {
    char* ptr;
    size_t len;
  };

  StreamReader(uint32_t flags = 0)
      : StreamListener<Derived, Stream>(flags),
        logger(logging::log->logger("io::StreamReader")) {}

  bool accept(Socket& socket) {
    logger.trace("accept");

    // accept a connection from a socket
    auto s = socket.accept(nullptr, nullptr);

    if (!s.is_open()) return false;

    // make the recieved socket non blocking
    s.set_non_blocking();

    auto& stream = this->derived().on_connect(std::move(s));

    // we want to listen to an incoming event which is edge triggered and
    // we also want to listen on the hangup event
    stream.event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;

    // add the connection to the event listener
    this->add(stream);

    return true;
  }

  void on_data(Stream& stream) {
    logger.trace("on data");

    while (true) {
      if (UNLIKELY(!stream.alive())) {
        stream.close();
        break;
      }

      // allocate the buffer to fill the data
      auto buf = this->derived().on_alloc(stream);

      // read from the buffer at most buf.len bytes
      buf.len = stream.socket.read(buf.ptr, buf.len);

      // check for read errors
      if (buf.len == -1) {
        // this means we have read all available data
        if (LIKELY(errno == EAGAIN)) {
          break;
        }

        // some other error occurred, check errno
        this->derived().on_error(stream);
        break;
      }

      // end of file, the client has closed the connection
      if (UNLIKELY(buf.len == 0)) {
        stream.close();
        break;
      }

      this->derived().on_read(stream, buf);
    }
  }

 private:
  Logger logger;
};
}
