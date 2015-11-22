#pragma once

#include "epoll.hpp"
#include "socket.hpp"

#include "memory/literals.hpp"

namespace io
{

class TcpStream
{
public:
    TcpStream(Socket&& socket) : socket(std::move(socket))
    {
        event.data.ptr = this;
    }

    void close()
    {
        delete reinterpret_cast<TcpStream*>(event.data.ptr);
    }

    int id() const { return socket.id(); }

    Socket socket;
    Epoll::Event event;

    // custom data we can pass on
    void* data;
};

}
