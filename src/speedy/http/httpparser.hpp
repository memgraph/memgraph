#ifndef MEMGRAPH_SERVER_HTTP_HTTPPARSER_HPP
#define MEMGRAPH_SERVER_HTTP_HTTPPARSER_HPP

#include <iostream>
#include <cstdlib>

#include <http_parser.h>

namespace http
{

template <class Req, class Res>
class HttpConnection;

template <class Req, class Res>
class HttpParserSettings;

template <class Req, class Res>
class HttpParser
{
    using connection_t = HttpConnection<Req, Res>;
    using settings_t = HttpParserSettings<Req, Res>;

    friend class HttpParserSettings<Req, Res>;
public:
    HttpParser();

    static void init();

    size_t execute(settings_t& settings, const char* data, size_t size);

    template <typename T>
    T* data();

    template <typename T>
    void data(T* value);

private:
    http_parser parser;

    std::string last_field;

    static int on_message_begin(http_parser* parser);

    static int on_url(http_parser* parser,
                      const char* at,
                      size_t length);

    static int on_status_complete(http_parser* parser,
                                  const char* at,
                                  size_t length);

    static int on_header_field(http_parser* parser,
                               const char* at,
                               size_t length);

    static int on_header_value(http_parser* parser,
                               const char* at,
                               size_t length);

    static int on_headers_complete(http_parser* parser);

    static int on_body(http_parser* parser,
                       const char* at,
                       size_t length);

    static int on_message_complete(http_parser* parser);
};

}

#endif
