#ifndef MEMGRAPH_SERVER_HTTP_CONNECTION_INL
#define MEMGRAPH_SERVER_HTTP_CONNECTION_INL

#include <uv.h>

#include "httpconnection.hpp"

namespace http
{

template <class Req, class Res>
HttpConnection<Req, Res>::HttpConnection(uv::UvLoop& loop, server_t& server)
    : server(server), client(loop), response(*this)
{
    client.data(this);
    parser.data(this);
}

template <class Req, class Res>
void HttpConnection<Req, Res>::close()
{
    client.close([](uv_handle_t* client) -> void {
        // terrible bug, this can happen even though a query is running and
        // then the query is using memory which is deallocated

        //delete reinterpret_cast<connection_t*>(client->data);
    });
}

}

#endif
