#ifndef MEMGRAPH_SERVER_HTTP_CONNECTION_HPP
#define MEMGRAPH_SERVER_HTTP_CONNECTION_HPP

#include "server/uv/uv.hpp"

#include "httpparser.hpp"
#include "request.hpp"
#include "response.hpp"

namespace http
{

class HttpServer;

class HttpConnection
{
public:
    HttpConnection(uv::UvLoop& loop, HttpServer& server);

    void close();

    HttpServer& server;
    uv::TcpStream client;

    HttpParser parser;
    
    Request request;
    Response response;

    bool keep_alive;
};

}

#endif
