#pragma once

#include <glog/logging.h>

#include "io/network/epoll.hpp"
#include "utils/crtp.hpp"

namespace io::network {

/**
 * This class listens to events on an epoll object and calls
 * callback functions to process them.
 */

template <class Listener>
class SocketEventDispatcher {
 public:
  explicit SocketEventDispatcher(
      std::vector<std::unique_ptr<Listener>> &listeners, uint32_t flags = 0)
      : epoll_(flags), listeners_(listeners) {}

  void AddListener(int fd, Listener &listener, uint32_t events) {
    // Add the listener associated to fd file descriptor to epoll.
    epoll_event event;
    event.events = events;
    event.data.ptr = &listener;
    epoll_.Add(fd, &event);
  }

  // Returns true if there was event before timeout.
  bool WaitAndProcessEvents() {
    // Waits for an event/multiple events and returns a maximum of max_events
    // and stores them in the events array. It waits for wait_timeout
    // milliseconds. If wait_timeout is achieved, returns 0.
    const auto n = epoll_.Wait(events_, kMaxEvents, 200);
    DLOG_IF(INFO, n > 0) << "number of events: " << n;

    // Go through all events and process them in order.
    for (int i = 0; i < n; ++i) {
      auto &event = events_[i];
      Listener *listener = reinterpret_cast<Listener *>(event.data.ptr);

      // Check if the listener that we got in the epoll event still exists
      // because it might have been deleted in a previous call.
      auto it =
          std::find_if(listeners_.begin(), listeners_.end(),
                       [&](const auto &l) { return l.get() == listener; });
      // If the listener doesn't exist anymore just ignore the event.
      if (it == listeners_.end()) continue;

      // Even though it is possible for multiple events to be reported we handle
      // only one of them. Since we use epoll in level triggered mode
      // unprocessed events will be reported next time we call epoll_wait. This
      // kind of processing events is safer since callbacks can destroy listener
      // and calling next callback on listener object will segfault. More subtle
      // bugs are also possible: one callback can handle multiple events
      // (maybe even in a subtle implicit way) and then we don't want to call
      // multiple callbacks since we are not sure if those are valid anymore.
      try {
        if (event.events & EPOLLIN) {
          // We have some data waiting to be read.
          listener->OnData();
          continue;
        }

        if (event.events & EPOLLRDHUP) {
          listener->OnClose();
          continue;
        }

        // There was an error on the server side.
        if (!(event.events & EPOLLIN) || event.events & (EPOLLHUP | EPOLLERR)) {
          listener->OnError();
          continue;
        }

      } catch (const std::exception &e) {
        listener->OnException(e);
      }
    }

    return n > 0;
  }

 private:
  static const int kMaxEvents = 64;
  // TODO: epoll is really ridiculous here. We don't plan to handle thousands of
  // connections so ppoll would actually be better or (even plain nonblocking
  // socket).
  Epoll epoll_;
  Epoll::Event events_[kMaxEvents];
  std::vector<std::unique_ptr<Listener>> &listeners_;
};

}  // namespace io::network
