#pragma once

#include "stream_reader.hpp"

namespace io
{

template <class Derived, class Stream>
class Server : public StreamReader<Derived, Stream>
{
public:
    bool accept(Socket& socket)
    {
        // accept a connection from a socket
        auto s = socket.accept(nullptr, nullptr);
        LOG_DEBUG("socket " << s.id() << " accepted");

        if(!s.is_open())
            return false;

        // make the recieved socket non blocking
        s.set_non_blocking();

        auto& stream = this->derived().on_connect(std::move(s));

        // we want to listen to an incoming event which is edge triggered and
        // we also want to listen on the hangup event
        stream.event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;

        // add the connection to the event listener
        this->add(stream);

        return true;
    }
};

}
