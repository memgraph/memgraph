#ifndef MEMGRAPH_SERVER_HTTP_HTTPSERVER_INL
#define MEMGRAPH_SERVER_HTTP_HTTPSERVER_INL

#include "httpparser.hpp"
#include "httpserver.hpp"
#include "httpconnection.hpp"
#include "httpparsersettings.hpp"

namespace http
{

HttpParser::HttpParser()
{
    http_parser_init(&parser, HTTP_REQUEST);
}

size_t HttpParser::execute(HttpParserSettings& settings,
                           const char* data,
                           size_t size)
{
    return http_parser_execute(&parser, settings, data, size);
}

template <typename T>
T* HttpParser::data()
{
    return reinterpret_cast<T*>(parser.data);
}

template <typename T>
void HttpParser::data(T* value)
{
    parser.data = reinterpret_cast<void*>(value);
}

int HttpParser::on_message_begin(http_parser*)
{
    return 0;
}

int HttpParser::on_url(http_parser* parser,
                       const char* at,
                       size_t length)
{
    HttpConnection& conn = *reinterpret_cast<HttpConnection*>(parser->data);
    conn.request.url = std::string(at, length);

    return 0;
}

int HttpParser::on_status_complete(http_parser*, const char*, size_t)
{
    return 0;
}

int HttpParser::on_header_field(http_parser* parser,
                                const char* at,
                                size_t length)
{
    HttpConnection& conn = *reinterpret_cast<HttpConnection*>(parser->data);
    conn.parser.last_field = std::string(at, length);

    return 0;
}

int HttpParser::on_header_value(http_parser* parser,
                                const char* at,
                                size_t length)
{
    HttpConnection& conn = *reinterpret_cast<HttpConnection*>(parser->data);
    conn.request.headers[conn.parser.last_field] = std::string(at, length);

    return 0;
}

int HttpParser::on_headers_complete(http_parser* parser)
{
    HttpConnection& conn = *reinterpret_cast<HttpConnection*>(parser->data);

    conn.request.version.major = parser->http_major;
    conn.request.version.minor = parser->http_minor;

    conn.request.method = static_cast<Method>(parser->method);
    conn.keep_alive = http_should_keep_alive(parser) == true;

    return 0;
}

int HttpParser::on_body(http_parser* parser, const char* at, size_t length)
{    
    if(length == 0)
        return 0;

    HttpConnection& conn = *reinterpret_cast<HttpConnection*>(parser->data);
    conn.request.body.append(at, length);

    return 0;
}

int HttpParser::on_message_complete(http_parser* parser)
{
    HttpConnection& conn = *reinterpret_cast<HttpConnection*>(parser->data);
    conn.server.respond(conn);

    return 0;
}
   
}

#endif
