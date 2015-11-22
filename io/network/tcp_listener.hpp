#pragma once

#include "socket.hpp"
#include "epoll.hpp"
#include "tcp_stream.hpp"

#include "utils/crtp.hpp"

namespace io
{

template <class Derived, size_t max_events = 64, int wait_timeout = -1>
class TcpListener : public Crtp<Derived>
{
public:
    using Crtp<Derived>::derived;

    TcpListener(uint32_t flags = 0) : listener(flags) {}

    void add(TcpStream& stream)
    {
        // add the stream to the event listener
        listener.add(stream.socket, &stream.event);
    }

    void wait_and_process_events()
    {
        // waits for an event/multiple events and returns a maximum of
        // max_events and stores them in the events array. it waits for
        // wait_timeout milliseconds. if wait_timeout is achieved, returns 0
        auto n = listener.wait(events, max_events, wait_timeout);

        // go through all events and process them in order
        for(int i = 0; i < n; ++i)
        {
            auto& event = events[i];
            auto& stream = *reinterpret_cast<TcpStream*>(event.data.ptr);

            // a tcp stream was closed
            if(UNLIKELY(event.events & EPOLLRDHUP))
            {
                this->derived().on_close(stream);
                continue;
            }

            // there was an error on the server side
            if(UNLIKELY(!(event.events & EPOLLIN) ||
                          event.events & (EPOLLHUP | EPOLLERR)))
            {
                this->derived().on_error(stream);
                continue;
            }

            // we have some data waiting to be read
            this->derived().on_data(stream);
        }

        // this will be optimized out :D
        if(wait_timeout < 0)
            return;

        // if there was events, continue to wait on new events
        if(n != 0)
            return;

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
