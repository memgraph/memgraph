#ifndef MEMGRAPH_SERVER_UV_UVBUFFER_INL
#define MEMGRAPH_SERVER_UV_UVBUFFER_INL

#include <cstdlib>
#include <cstring>

#include "uvbuffer.hpp"

namespace uv
{

UvBuffer::UvBuffer()
    : capacity(0)
{
    buffer.len = 0;
    buffer.base = nullptr;
}

UvBuffer::UvBuffer(size_t capacity)
    : capacity(capacity)
{
    buffer.len = 0; 
    buffer.base = static_cast<char*>(malloc(capacity));
}

UvBuffer::UvBuffer(const std::string& data)
    : capacity(data.size())
{
    buffer.len = data.size();
    buffer.base = static_cast<char*>(malloc(capacity));

    std::memcpy(buffer.base, data.c_str(), buffer.len);
}

UvBuffer::~UvBuffer()
{
    if(buffer.base == nullptr)
        return;

    free(buffer.base);
}

size_t UvBuffer::size() const noexcept
{
    return buffer.len;
}

size_t UvBuffer::length() const noexcept
{
    return this->size();
}

void UvBuffer::clear()
{
    buffer.len = 0;
}

UvBuffer& UvBuffer::append(const std::string& data)
{
    return this->append(data.c_str(), data.size());
}

UvBuffer& UvBuffer::append(const char* data, size_t n)
{
    auto new_size = size() + n;

    if(capacity < new_size)
    {
        capacity = new_size;
        buffer.base = static_cast<char*>(realloc(buffer.base, new_size));
    }

    auto ptr = buffer.base + size();

    for(size_t i = 0; i < n; ++i, ++ptr, ++data)
        *ptr = *data;

    buffer.len = new_size;

    return *this;
}

UvBuffer& UvBuffer::operator<<(const std::string& data)
{
    return this->append(data);
}

UvBuffer::operator uv_buf_t*()
{
    return &buffer;
}

}

#endif
