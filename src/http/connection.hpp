#pragma once

#include "io/network/tcp/stream.hpp"
#include "memory/literals.hpp"

namespace http
{
using namespace memory::literals;

template <class Req, class Res>
class Connection : public io::tcp::Stream
{
public:
    struct Buffers
    {
        char headers[8_kB];
        char body[64_kB];

        static constexpr size_t size = sizeof headers + sizeof body;
    };

    Connection(io::Socket&& socket) : io::tcp::Stream(std::move(socket)),
        response(this->socket) {}

    // tcp stream reads into these buffers
    Buffers buffers;

    Req request;
    Res response;
};

}
