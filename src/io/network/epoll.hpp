#pragma once

#include <errno.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <malloc.h>
#include <sys/epoll.h>

#include "io/network/socket.hpp"
#include "utils/exceptions.hpp"
#include "utils/likely.hpp"

namespace io::network {

/**
 * Wrapper class for epoll.
 * Creates an object that listens on file descriptor status changes.
 * see: man 4 epoll
 */
class Epoll {
 public:
  using Event = struct epoll_event;

  Epoll(int flags) : epoll_fd_(epoll_create1(flags)) {
    // epoll_create1 returns an error if there is a logical error in our code
    // (for example invalid flags) or if there is irrecoverable error. In both
    // cases it is best to terminate.
    CHECK(epoll_fd_ != -1) << "Error on epoll_create1, errno: " << errno
                           << ", message: " << strerror(errno);
  }

  void Add(int fd, Event *event) {
    auto status = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, event);
    // epoll_ctl can return an error on our logical error or on irrecoverable
    // error. There is a third possibility that some system limit is reached. In
    // that case we could return an erorr and close connection. Chances of
    // reaching system limit in normally working memgraph is extremely unlikely,
    // so it is correct to terminate even in that case.
    CHECK(!status) << "Error on epoll_ctl, errno: " << errno
                   << ", message: " << strerror(errno);
  }

  int Wait(Event *events, int max_events, int timeout) {
    auto num_events = epoll_wait(epoll_fd_, events, max_events, timeout);
    // If this check fails there was logical error in our code.
    CHECK(num_events != -1 || errno == EINTR)
        << "Error on epoll_wait, errno: " << errno
        << ", message: " << strerror(errno);
    // num_events can be -1 if errno was EINTR (epoll_wait interrupted by signal
    // handler). We treat that as no events, so we return 0.
    return num_events == -1 ? 0 : num_events;
  }

 private:
  const int epoll_fd_;
};
}
