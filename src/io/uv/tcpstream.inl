#pragma once

#include "io/uv/tcpstream.hpp"

namespace uv
{

TcpStream::TcpStream(UvLoop& loop)
{
    uv_tcp_init(loop, &stream);
}

template <typename T>
T* TcpStream::data()
{
    return reinterpret_cast<T*>(stream.data);
}

template <typename T>
void TcpStream::data(T* value)
{
    stream.data = reinterpret_cast<void*>(value);
}

void TcpStream::close(callback_t callback)
{
    uv_close(reinterpret_cast<uv_handle_t*>(&stream), callback);
}

TcpStream::operator uv_tcp_t*()
{
    return &stream;
}

TcpStream::operator uv_stream_t*()
{
    return reinterpret_cast<uv_stream_t*>(&stream);
}

TcpStream::operator uv_handle_t*()
{
    return reinterpret_cast<uv_handle_t*>(&stream);
}

}
