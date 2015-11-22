#pragma once

#include "io/network/tcp_stream.hpp"

namespace htpp
{
using memory::literals::operator "" _kB;

class Connection
{
    Connection(io::Socket&& socket) : stream(std::move(socket))
    {
        stream.data = this;
    }

    void close()
    {
        delete reinterpret_cast<Connection*>(stream.data);
    }

    struct Buffers
    {
        char headers[8_kB];
        char body[64_kB];

        static constexpr size_t size = sizeof headers + sizeof body;
    };

    // tcp stream reads into these buffers
    Buffers buffers;

    // tcp stream this connection reads from
    io::TcpStream stream;
};

}
