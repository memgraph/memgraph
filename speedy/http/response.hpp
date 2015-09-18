#ifndef MEMGRAPH_SERVER_HTTP_RESPONSE_HPP
#define MEMGRAPH_SERVER_HTTP_RESPONSE_HPP

#include <map>

#include "uv/uv.hpp"
#include "status_codes.hpp"

namespace http
{

class HttpConnection;

class Response
{
public:
    Response(HttpConnection& connection);
    
    void send(const std::string& body);
    void send(Status code, const std::string& body);

    Response& status(Status code);

    std::map<std::string, std::string> headers;

private:
    HttpConnection& connection;
    uv::UvBuffer buffer;

    Status code;
};

}

#endif
