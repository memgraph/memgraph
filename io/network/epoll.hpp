#pragma once

#include <malloc.h>
#include <sys/epoll.h>

#include "socket.hpp"
#include "utils/likely.hpp"

namespace io
{

class Epoll
{
public:
    using Event = struct epoll_event;

    Epoll(int flags)
    {
        epoll_fd = epoll_create1(flags);
    }

    void add(Socket& socket, Event* event)
    {
        auto status = epoll_ctl(epoll_fd, EPOLL_CTL_ADD, socket, event);

        if(UNLIKELY(status))
            throw NetworkError("Can't add connection to epoll listener.");
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
