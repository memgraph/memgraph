#ifndef UVMACHINE_HTTP_HTTPSERVER_INL
#define UVMACHINE_HTTP_HTTPSERVER_INL

#include "httpserver.hpp"

namespace http
{

HttpServer::HttpServer(uv::UvLoop& loop)
    : loop(loop), stream(loop) {}

void HttpServer::listen(const Ipv4& ip, request_cb_t callback)
{
    request_cb = callback;
    stream.data(this);

    uv_tcp_bind(stream, ip, 0);
    uv_listen(stream, 128, HttpServer::on_connect);
}

HttpServer::operator uv_tcp_t*()
{
    return stream;
}

HttpServer::operator uv_stream_t*()
{
    return stream;
}

void HttpServer::on_connect(uv_stream_t* tcp_server, int)
{
    HttpServer& server = *reinterpret_cast<HttpServer*>(tcp_server->data);

    auto connection = new HttpConnection(server.loop, server);

    uv_accept(server, connection->client);

    uv_read_start(connection->client,
                  HttpServer::on_alloc,
                  HttpServer::on_read);
}

void HttpServer::on_alloc(uv_handle_t*, size_t suggested_size, uv_buf_t* buf)
{
    buf->base = static_cast<char*>(malloc(sizeof(char) * suggested_size));
    buf->len = suggested_size;
}

void HttpServer::on_read(uv_stream_t* tcp_client,
                         ssize_t nread,
                         const uv_buf_t* buf)
{
    HttpConnection& conn =
        *reinterpret_cast<HttpConnection*>(tcp_client->data);

    if(nread >= 0)
        conn.parser.execute(conn.server.settings, buf->base, nread);
    else
        conn.close();

    delete buf->base;
}

void HttpServer::respond(HttpConnection& conn)
{
    conn.server.request_cb(conn.request, conn.response);
}

}

#endif
