#pragma once

#include <uv.h>

#include "core.hpp"
#include "uvloop.hpp"

namespace uv
{

class TcpStream
{
public:
    TcpStream(UvLoop& loop);

    template <typename T>
    T* data();

    template <typename T>
    void data(T* value);

    void close(callback_t callback);

    operator uv_handle_t*();
    operator uv_tcp_t*();
    operator uv_stream_t*();

private:
    uv_tcp_t stream;
};

}
