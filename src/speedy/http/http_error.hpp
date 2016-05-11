#ifndef MEMGRAPH_SERVER_HTTP_HTTP_ERROR_HPP
#define MEMGRAPH_SERVER_HTTP_HTTP_ERROR_HPP

#include <stdexcept>
#include <string>

namespace http
{

class HttpError : public std::runtime_error
{
public:
    HttpError(const std::string& message)
        : std::runtime_error(message) {}
};

}

#endif
