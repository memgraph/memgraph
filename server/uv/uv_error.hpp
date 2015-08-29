#ifndef MEMGRAPH_SERVER_UV_UV_ERROR_HPP
#define MEMGRAPH_SERVER_UV_UV_ERROR_HPP

#include <stdexcept>
#include <string>

namespace uv
{

class UvError : public std::runtime_error
{
public:
    UvError(const std::string& message)
        : std::runtime_error(message) {}
};

}

#endif
