#pragma once

#include "io/network/epoll.hpp"
#include "utils/crtp.hpp"

namespace io
{

template <class Derived, size_t max_events = 64, int wait_timeout = -1>
class EventListener : public Crtp<Derived>
{
public:
    using Crtp<Derived>::derived;

    EventListener(uint32_t flags = 0) : listener(flags) {}

    void wait_and_process_events()
    {
        // TODO hardcoded a wait timeout because of thread joining
        // when you shutdown the server. This should be wait_timeout of the
        // template parameter and should almost never change from that.
        // thread joining should be resolved using a signal that interrupts
        // the system call.

        // waits for an event/multiple events and returns a maximum of
        // max_events and stores them in the events array. it waits for
        // wait_timeout milliseconds. if wait_timeout is achieved, returns 0
        auto n = listener.wait(events, max_events, 200);

        // go through all events and process them in order
        for (int i = 0; i < n; ++i) {
            auto &event = events[i];

            try {
                // hangup event
                if (UNLIKELY(event.events & EPOLLRDHUP)) {
                    this->derived().on_close_event(event);
                    continue;
                }

                // there was an error on the server side
                if (UNLIKELY(!(event.events & EPOLLIN) ||
                             event.events & (EPOLLHUP | EPOLLERR))) {
                    this->derived().on_error_event(event);
                    continue;
                }

                // we have some data waiting to be read
                this->derived().on_data_event(event);
            } catch (const std::exception &e) {
                this->derived().on_exception_event(
                    event, "Error occured while processing event \n{}",
                    e.what());
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
        this->derived().on_wait_timeout();
    }

protected:
    Epoll listener;
    Epoll::Event events[max_events];
};
}
