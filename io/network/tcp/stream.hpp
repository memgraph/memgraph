#pragma once

#include "io/network/epoll.hpp"
#include "io/network/socket.hpp"

namespace io
{
namespace tcp
{

class Stream
{
public:
    Stream(Socket&& socket) : socket(std::move(socket))
    {
        // save this to epoll event data baton to access later
        event.data.ptr = this;
    }

    Stream(Stream&& stream)
    {
        socket = std::move(stream.socket);
        event = stream.event;
        event.data.ptr = this;
    }

    void close()
    {
        delete reinterpret_cast<Stream*>(event.data.ptr);
    }

    int id() const { return socket.id(); }

    Socket socket;
    Epoll::Event event;
};

}
}
