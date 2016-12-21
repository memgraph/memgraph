#ifndef MEMGRAPH_SERVER_HTTP_RESPONSE_HPP
#define MEMGRAPH_SERVER_HTTP_RESPONSE_HPP

#include <map>

#include "io/uv/uv.hpp"
#include "status_codes.hpp"

namespace http
{

static constexpr size_t buffer_size = 65536;

template <class Req, class Res>
class HttpConnection;

template <class Req, class Res>
class Response
{
    using connection_t = HttpConnection<Req, Res>;
    using response_t = Response<Req, Res>;

public:
    explicit Response(connection_t& connection);
    
    void send(const std::string& body);
    void send(Status code, const std::string& body);

    std::map<std::string, std::string> headers;

    Status status;

private:
    connection_t& connection;
    uv::BlockBuffer<buffer_size> buffer;
};

}

#endif
