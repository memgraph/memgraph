#pragma once

#include "io/network/epoll.hpp"
#include "logging/default.hpp"
#include "utils/crtp.hpp"

namespace io::network {

/**
 * This class listens to events on an epoll object and calls
 * callback functions to process them.
 */
template <class Derived, size_t max_events = 64, int wait_timeout = -1>
class EventListener : public Crtp<Derived> {
 public:
  using Crtp<Derived>::derived;

  EventListener(uint32_t flags = 0)
      : listener_(flags), logger_(logging::log->logger("io::EventListener")) {}

  void WaitAndProcessEvents() {
    // TODO hardcoded a wait timeout because of thread joining
    // when you shutdown the server. This should be wait_timeout of the
    // template parameter and should almost never change from that.
    // thread joining should be resolved using a signal that interrupts
    // the system call.

    // waits for an event/multiple events and returns a maximum of
    // max_events and stores them in the events array. it waits for
    // wait_timeout milliseconds. if wait_timeout is achieved, returns 0

    auto n = listener_.Wait(events_, max_events, 200);

#ifndef NDEBUG
#ifndef LOG_NO_TRACE
    if (n > 0) logger_.trace("number of events: {}", n);
#endif
#endif

    // go through all events and process them in order
    for (int i = 0; i < n; ++i) {
      auto &event = events_[i];

      try {
        // hangup event
        if (UNLIKELY(event.events & EPOLLRDHUP)) {
          this->derived().OnCloseEvent(event);
          continue;
        }

        // there was an error on the server side
        if (UNLIKELY(!(event.events & EPOLLIN) ||
                     event.events & (EPOLLHUP | EPOLLERR))) {
          this->derived().OnErrorEvent(event);
          continue;
        }

        // we have some data waiting to be read
        this->derived().OnDataEvent(event);
      } catch (const std::exception &e) {
        this->derived().OnExceptionEvent(
            event, "Error occured while processing event \n{}", e.what());
      }
    }

    // this will be optimized out :D
    if (wait_timeout < 0) return;

    // if there was events, continue to wait on new events
    if (n != 0) return;

    // wait timeout occurred and there were no events. if wait_timeout
    // is -1 there will never be any timeouts so client should provide
    // an empty function. in that case the conditional above and the
    // function call will be optimized out by the compiler
    this->derived().OnWaitTimeout();
  }

 protected:
  Epoll listener_;
  Epoll::Event events_[max_events];

 private:
  Logger logger_;
};
}
