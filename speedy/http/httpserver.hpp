#ifndef MEMGRAPH_SERVER_HTTP_HTTPSERVER_HPP
#define MEMGRAPH_SERVER_HTTP_HTTPSERVER_HPP

#include <iostream>
#include <uv.h>

#include "uv/uv.hpp"
#include "httpparsersettings.hpp"
#include "httpconnection.hpp"
#include "request.hpp"
#include "response.hpp"
#include "ipv4.hpp"

namespace http
{
    typedef std::function<void(Request&, Response&)> request_cb_t;

class HttpServer
{
    friend class HttpParser;
public:
    HttpServer(uv::UvLoop& loop);

    void listen(const Ipv4& ip, request_cb_t callback);

    operator uv_tcp_t*();
    operator uv_stream_t*();

private:
    uv::UvLoop& loop;
    uv::TcpStream stream;
    HttpParserSettings settings;

    request_cb_t request_cb;

    static void on_connect(uv_stream_t* tcp_server, int status);

    static void on_read(uv_stream_t* tcp_client,
                        ssize_t nread,
                        const uv_buf_t* buf);

    static void on_alloc(uv_handle_t* tcp_client,
                         size_t suggested_size,
                         uv_buf_t* buf);

    static void respond(HttpConnection& conn);
};

}

#endif
