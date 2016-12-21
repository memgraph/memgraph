#ifndef UVMACHINE_HTTP_HTTPSERVER_INL
#define UVMACHINE_HTTP_HTTPSERVER_INL

#include "httpserver.hpp"

namespace http
{

static BlockAllocator<65536> buffer_allocator;

template <class Req, class Res>
HttpServer<Req, Res>::HttpServer(uv::UvLoop& l)
    : loop(l), stream(l) {}

template <class Req, class Res>
void HttpServer<Req, Res>::listen(const Ipv4& ip, request_cb_t callback)
{
    request_cb = callback;
    stream.data(this);

    uv_tcp_bind(stream, ip, 0);
    uv_listen(stream, 128, server_t::on_connect);
}

template <class Req, class Res>
HttpServer<Req, Res>::operator uv_tcp_t*()
{
    return stream;
}

template <class Req, class Res>
HttpServer<Req, Res>::operator uv_stream_t*()
{
    return stream;
}

template <class Req, class Res>
void HttpServer<Req, Res>::on_connect(uv_stream_t* tcp_server, int)
{
    server_t& server = *reinterpret_cast<server_t*>(tcp_server->data);

    auto connection = new connection_t(server.loop, server);

    uv_accept(server, connection->client);

    uv_read_start(connection->client, server_t::on_alloc, server_t::on_read);
}

template <class Req, class Res>
void HttpServer<Req, Res>::
    on_alloc(uv_handle_t*, size_t, uv_buf_t* buf)
{
    buf->base = static_cast<char*>(buffer_allocator.acquire());
    buf->len = buffer_allocator.size;
}

template <class Req, class Res>
void HttpServer<Req, Res>::
    on_read(uv_stream_t* tcp_client, ssize_t nread, const uv_buf_t* buf)
{
    connection_t& conn = *reinterpret_cast<connection_t*>(tcp_client->data);

    if(nread >= 0)
        conn.parser.execute(conn.server.settings, buf->base, nread);
    else
        conn.close();

    buffer_allocator.release(buf->base);
}

template <class Req, class Res>
void HttpServer<Req, Res>::respond(connection_t& conn)
{
    conn.server.request_cb(conn.request, conn.response);
}

}

#endif
