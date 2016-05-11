#ifndef MEMGRAPH_SERVER_HTTP_HTTPSERVER_INL
#define MEMGRAPH_SERVER_HTTP_HTTPSERVER_INL

#include "httpparser.hpp"
#include "httpserver.hpp"
#include "httpconnection.hpp"
#include "httpparsersettings.hpp"

namespace http
{

template <class Req, class Res>
HttpParser<Req, Res>::HttpParser()
{
    http_parser_init(&parser, HTTP_REQUEST);
}

template <class Req, class Res>
size_t HttpParser<Req, Res>::
    execute(settings_t& settings, const char* data, size_t size)
{
    return http_parser_execute(&parser, settings, data, size);
}

template <class Req, class Res>
template <typename T>
T* HttpParser<Req, Res>::data()
{
    return reinterpret_cast<T*>(parser.data);
}

template <class Req, class Res>
template <typename T>
void HttpParser<Req, Res>::data(T* value)
{
    parser.data = reinterpret_cast<void*>(value);
}

template <class Req, class Res>
int HttpParser<Req, Res>::on_message_begin(http_parser*)
{
    return 0;
}

template <class Req, class Res>
int HttpParser<Req, Res>::
    on_url(http_parser* parser, const char* at, size_t length)
{
    connection_t& conn = *reinterpret_cast<connection_t*>(parser->data);
    conn.request.url = std::string(at, length);

    return 0;
}

template <class Req, class Res>
int HttpParser<Req, Res>::
    on_status_complete(http_parser*, const char*, size_t)
{
    return 0;
}

template <class Req, class Res>
int HttpParser<Req, Res>::
    on_header_field(http_parser* parser, const char* at, size_t length)
{
    connection_t& conn = *reinterpret_cast<connection_t*>(parser->data);
    conn.parser.last_field = std::string(at, length);

    return 0;
}

template <class Req, class Res>
int HttpParser<Req, Res>::
    on_header_value(http_parser* parser, const char* at, size_t length)
{
    connection_t& conn = *reinterpret_cast<connection_t*>(parser->data);
    conn.request.headers[conn.parser.last_field] = std::string(at, length);

    return 0;
}

template <class Req, class Res>
int HttpParser<Req, Res>::on_headers_complete(http_parser* parser)
{
    connection_t& conn = *reinterpret_cast<connection_t*>(parser->data);

    conn.request.version.major = parser->http_major;
    conn.request.version.minor = parser->http_minor;

    conn.request.method = static_cast<Method>(parser->method);
    conn.keep_alive = http_should_keep_alive(parser) == true;

    return 0;
}

template <class Req, class Res>
int HttpParser<Req, Res>::
    on_body(http_parser* parser, const char* at, size_t length)
{    
    if(length == 0)
        return 0;

    connection_t& conn = *reinterpret_cast<connection_t*>(parser->data);
    conn.request.body.append(at, length);

    return 0;
}

template <class Req, class Res>
int HttpParser<Req, Res>::on_message_complete(http_parser* parser)
{
    connection_t& conn = *reinterpret_cast<connection_t*>(parser->data);
    conn.server.respond(conn);

    return 0;
}
   
}

#endif
