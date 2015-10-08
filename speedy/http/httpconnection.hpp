#ifndef MEMGRAPH_SERVER_HTTP_CONNECTION_HPP
#define MEMGRAPH_SERVER_HTTP_CONNECTION_HPP

#include "io/uv/uv.hpp"

#include "httpparser.hpp"
#include "request.hpp"
#include "response.hpp"

#include "utils/memory/block_allocator.hpp"

namespace http
{

template <class Req, class Res>
class HttpServer;

template <class Req, class Res>
class HttpConnection
{
    friend class HttpServer<Req, Res>;

    using server_t = HttpServer<Req, Res>;
    using connection_t = HttpConnection<Req, Res>;
    using parser_t = HttpParser<Req, Res>;

public:
    HttpConnection(uv::UvLoop& loop, HttpServer<Req, Res>& server);

    void close();

    server_t& server;
    uv::TcpStream client;

    parser_t parser;
    
    Req request;
    Res response;

    bool keep_alive;
};

}

#endif
