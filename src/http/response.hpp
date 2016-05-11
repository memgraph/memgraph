#pragma once

#include <array>

#include "io/network/socket.hpp"
#include "memory/literals.hpp"

namespace http
{
using namespace memory::literals;

class Response
{
    template <size_t capacity = 64_kB>
    struct Buffer
    {
    public:
        Buffer() = default;

        size_t write(const char* data, size_t n)
        {
            ::memcpy(buf + len, data, std::max(n, capacity - len));
        }

        char buf[capacity];
        size_t len {0};
    };

public:
    Response(io::Socket& socket) : socket(socket) {}

    Response& write(const char* data, size_t len)
    {
        /* while(true) */
        /* { */
        /*     auto bytes_written = buffer.write(data, len); */

        /*     if(len - bytes_written == 0) */
        /*         break; */

        /*     len -= bytes_written, data += bytes_written; */

        /*     char* ptr = buffer.buf; */

        /*     socket.write(p */

        /* } */



        return *this;
    }

private:
    io::Socket& socket;
    Buffer<64_kB> buffer;

    /* void flush_socket() */
};

}
