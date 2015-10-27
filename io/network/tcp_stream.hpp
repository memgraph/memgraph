#pragma once

#include "epoll.hpp"
#include "socket.hpp"

namespace io
{

class TcpStream
{
public:
    TcpStream(Socket&& socket, uint32_t events)
        : socket(std::move(socket))
    {
        event.events = events;
        event.data.ptr = this;
    }

    void close()
    {
        LOG_DEBUG("CLOSE");
        delete reinterpret_cast<TcpStream*>(event.data.ptr);
    }

    Socket socket;
    Epoll::Event event;
};

}
