#pragma once

#include "io/network/stream_buffer.hpp"
#include "io/network/stream_listener.hpp"
#include "memory/literals.hpp"

namespace io::network {

/**
 * This class is used to get data from a socket that has been notified
 * with a data available event.
 */
template <class Derived, class Stream>
class StreamReader : public StreamListener<Derived, Stream> {
 public:
  StreamReader(uint32_t flags = 0)
      : StreamListener<Derived, Stream>(flags),
        logger_(logging::log->logger("io::StreamReader")) {}

  bool Accept(Socket &socket) {
    logger_.trace("Accept");

    // accept a connection from a socket
    Socket s;
    if (!socket.Accept(&s)) return false;

    logger_.info("Client {}:{} connected.", s.endpoint().address(),
                 s.endpoint().port());
    logger_.trace(
        "Accepted a connection: scoket {}, address '{}', family {}, port {}",
        s.id(), s.endpoint().address(), s.endpoint().family(),
        s.endpoint().port());

    if (!s.SetKeepAlive()) return false;
    if (!s.SetNoDelay()) return false;

    auto &stream = this->derived().OnConnect(std::move(s));

    // we want to listen to an incoming event which is edge triggered and
    // we also want to listen on the hangup event
    stream.event_.events = EPOLLIN | EPOLLRDHUP;

    // add the connection to the event listener
    this->Add(stream);

    return true;
  }

  void OnData(Stream &stream) {
    logger_.trace("On data");

    if (UNLIKELY(!stream.Alive())) {
      logger_.trace("Calling OnClose because the stream isn't alive!");
      this->derived().OnClose(stream);
      return;
    }

    // allocate the buffer to fill the data
    auto buf = stream.Allocate();

    // read from the buffer at most buf.len bytes
    int len = stream.socket_.Read(buf.data, buf.len);

    // check for read errors
    if (len == -1) {
      // this means we have read all available data
      if (LIKELY(errno == EAGAIN || errno == EWOULDBLOCK)) {
        return;
      }

      // some other error occurred, check errno
      this->derived().OnError(stream);
      return;
    }

    // end of file, the client has closed the connection
    if (UNLIKELY(len == 0)) {
      logger_.trace("Calling OnClose because the socket is closed!");
      this->derived().OnClose(stream);
      return;
    }

    // notify the stream that it has new data
    stream.Written(len);

    this->derived().OnRead(stream);
  }

 private:
  Logger logger_;
};
}
