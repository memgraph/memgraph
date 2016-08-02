#pragma once

#include <malloc.h>
#include <sys/epoll.h>

#include "socket.hpp"
#include "utils/likely.hpp"

namespace io
{

class EpollError : BasicException
{
public:
    using BasicException::BasicException;
};

class Epoll
{
public:
    using Event = struct epoll_event;

    Epoll(int flags)
    {
        epoll_fd = epoll_create1(flags);

        if(UNLIKELY(epoll_fd == -1))
            throw EpollError("Can't create epoll file descriptor");
    }

    template <class Stream>
    void add(Stream& stream, Event* event)
    {
        auto status = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, stream, event);

        if(UNLIKELY(status))
            throw EpollError("Can't add an event to epoll listener.");
    }

    int wait(Event* events, int max_events, int timeout)
    {
        return epoll_wait(epoll_fd, events, max_events, timeout);
    }

    int id() const
    {
        return epoll_fd;
    }

private:
    int epoll_fd;
};

}
