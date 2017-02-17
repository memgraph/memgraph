#pragma once

#include "io/network/stream_reader.hpp"

namespace io
{

template <class Derived, class Stream>
class Client : public StreamReader<Derived, Stream>
{
public:
    bool connect(const std::string& host, const std::string& port)
    {
        return connect(host.c_str(), port.c_str());
    }

    bool connect(const char* host, const char* port)
    {
        auto socket = io::Socket::connect(host, port);

        if(!socket.is_open())
            return false;

        socket.set_non_blocking();

        auto& stream = this->derived().on_connect(std::move(socket));

        stream.event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
        this->add(stream);

        return true;
    }
};

}
