#pragma once

#include <cstdint>
#include <vector>
#include <cstdio>

#include "io/network/socket.hpp"
#include "communication/bolt/v1/transport/stream_error.hpp"

namespace bolt
{

class SocketStream
{
public:
    using byte = uint8_t;

    SocketStream(io::Socket& socket) : socket(socket) {}

    void write(const byte* data, size_t n)
    {
        while(n > 0)
        {
            auto written = socket.get().write(data, n);

            if(UNLIKELY(written == -1))
                throw StreamError("Can't write to stream");

            n -= written;
            data += written;
        }
    }

private:
    std::reference_wrapper<io::Socket> socket;
};

}
