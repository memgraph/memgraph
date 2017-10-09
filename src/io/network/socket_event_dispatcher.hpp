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
  SocketEventDispatcher(uint32_t flags = 0) : epoll_(flags) {}

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

      Listener &listener = *reinterpret_cast<Listener *>(event.data.ptr);

      // TODO: revise this. Reported events will be combined so continue is not
      // probably what we want to do.
      try {
        // Hangup event.
        if (UNLIKELY(event.events & EPOLLRDHUP)) {
          listener.OnClose();
          continue;
        }

        // There was an error on the server side.
        if (UNLIKELY(!(event.events & EPOLLIN) ||
                     event.events & (EPOLLHUP | EPOLLERR))) {
          listener.OnError();
          continue;
        }

        // We have some data waiting to be read.
        listener.OnData();
      } catch (const std::exception &e) {
        listener.OnException(e);
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
};

/**
 * Implements Listener concept, suitable for inheritance.
 */
class BaseListener {
 public:
  BaseListener(Socket &socket) : socket_(socket) {}

  void OnClose() { socket_.Close(); }

  // If server is listening on socket and there is incoming connection OnData
  // event will be triggered.
  void OnData() {}

  void OnException(const std::exception &e) {
    // TODO: this actually sounds quite bad, maybe we should close socket here
    // because we don'y know in which state Listener class is.
    LOG(ERROR) << "Exception was thrown while processing event on socket "
               << socket_.fd() << " with message: " << e.what();
  }

  void OnError() {
    LOG(ERROR) << "Error on server side occured in epoll";
    socket_.Close();
  }

 protected:
  Socket &socket_;
};

}  // namespace io::network
