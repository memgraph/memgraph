#ifndef MEMGRAPH_SERVER_UV_UVBUFFER_HPP
#define MEMGRAPH_SERVER_UV_UVBUFFER_HPP

#include <string>
#include <uv.h>

namespace uv
{

class UvBuffer
{
public:
    UvBuffer();
    UvBuffer(size_t capacity);
    UvBuffer(const std::string& data);

    ~UvBuffer();

    size_t size() const noexcept;
    size_t length() const noexcept;

    void clear();

    UvBuffer& append(const std::string& data);
    UvBuffer& append(const char* data, size_t n);

    UvBuffer& operator<<(const std::string& data);

    operator uv_buf_t*();

private:
    uv_buf_t buffer;
    size_t capacity;
};

}

#endif
