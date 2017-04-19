#pragma once

#include <malloc.h>
#include <sys/epoll.h>

#include "io/network/socket.hpp"
#include "logging/default.hpp"
#include "utils/exceptions.hpp"
#include "utils/likely.hpp"

namespace io::network {

class EpollError : utils::StacktraceException {
 public:
  using utils::StacktraceException::StacktraceException;
};

/**
 * Wrapper class for epoll.
 * Creates an object that listens on file descriptor status changes.
 * see: man 4 epoll
 */
class Epoll {
 public:
  using Event = struct epoll_event;

  Epoll(int flags) : logger_(logging::log->logger("io::Epoll")) {
    epoll_fd_ = epoll_create1(flags);

    if (UNLIKELY(epoll_fd_ == -1))
      throw EpollError("Can't create epoll file descriptor");
  }

  template <class Stream>
  void Add(Stream& stream, Event* event) {
    auto status = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, stream, event);

    if (UNLIKELY(status))
      throw EpollError("Can't add an event to epoll listener.");
  }

  int Wait(Event* events, int max_events, int timeout) {
    return epoll_wait(epoll_fd_, events, max_events, timeout);
  }

  int id() const { return epoll_fd_; }

 private:
  int epoll_fd_;
  Logger logger_;
};
}
