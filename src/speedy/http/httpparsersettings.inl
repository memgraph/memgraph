#ifndef MEMGRAPH_SERVER_HTTP_HTTPPARSERSETTINGS_INL
#define MEMGRAPH_SERVER_HTTP_HTTPPARSERSETTINGS_INL

#include "httpparsersettings.hpp"
#include "httpparser.hpp"

namespace http
{

template <class Req, class Res>
HttpParserSettings<Req, Res>::HttpParserSettings()
{
    settings.on_header_field = HttpParser<Req, Res>::on_header_field;
    settings.on_header_value = HttpParser<Req, Res>::on_header_value;
    settings.on_headers_complete = HttpParser<Req, Res>::on_headers_complete;
    settings.on_body = HttpParser<Req, Res>::on_body;
    settings.on_status = HttpParser<Req, Res>::on_status_complete;
    settings.on_message_begin = HttpParser<Req, Res>::on_message_begin;
    settings.on_message_complete = HttpParser<Req, Res>::on_message_complete;
    settings.on_url = HttpParser<Req, Res>::on_url;
}

template <class Req, class Res>
HttpParserSettings<Req, Res>::operator http_parser_settings*()
{
    return &settings;
}

}

#endif
