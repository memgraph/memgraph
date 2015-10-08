#ifndef MEMGRAPH_SERVER_HTTP_HTTPSERVER_HPP
#define MEMGRAPH_SERVER_HTTP_HTTPSERVER_HPP

#include <iostream>
#include <uv.h>

#include "io/uv/uv.hpp"
#include "httpparsersettings.hpp"
#include "httpconnection.hpp"
#include "request.hpp"
#include "response.hpp"
#include "ipv4.hpp"

#include "utils/memory/block_allocator.hpp"

namespace http
{

template <class Req, class Res>
class HttpServer
{
public:
    using request_cb_t = std::function<void(Req&, Res&)>;

    HttpServer(uv::UvLoop& loop);

    void listen(const Ipv4& ip, request_cb_t callback);

    operator uv_tcp_t*();
    operator uv_stream_t*();

private:
    using server_t = HttpServer<Req, Res>;
    using connection_t = HttpConnection<Req, Res>;

    friend class HttpParser<Req, Res>;

    uv::UvLoop& loop;
    uv::TcpStream stream;
    HttpParserSettings<Req, Res> settings;

    request_cb_t request_cb;

    static void on_connect(uv_stream_t* tcp_server, int status);

    static void on_read(uv_stream_t* tcp_client,
                        ssize_t nread,
                        const uv_buf_t* buf);

    static void on_alloc(uv_handle_t* tcp_client,
                         size_t suggested_size,
                         uv_buf_t* buf);

    static void respond(HttpConnection<Req, Res>& conn);
};

}

#endif
