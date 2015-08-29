#ifndef MEMGRAPH_SERVER_HTTP_CONNECTION_INL
#define MEMGRAPH_SERVER_HTTP_CONNECTION_INL

#include <uv.h>

#include "httpconnection.hpp"

namespace http
{

HttpConnection::HttpConnection(uv::UvLoop& loop, HttpServer& server)
    : server(server), client(loop), response(*this)
{
    client.data(this);
    parser.data(this);
}

void HttpConnection::close()
{
    client.close([](uv_handle_t* client) -> void {
        delete reinterpret_cast<HttpConnection*>(client->data);
    });
}

}

#endif
