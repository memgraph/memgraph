#ifndef MEMGRAPH_SERVER_HTTP_HTTPPARSERSETTINGS_INL
#define MEMGRAPH_SERVER_HTTP_HTTPPARSERSETTINGS_INL

#include "httpparsersettings.hpp"
#include "httpparser.hpp"

namespace http
{

HttpParserSettings::HttpParserSettings()
{
    settings.on_header_field = HttpParser::on_header_field;
    settings.on_header_value = HttpParser::on_header_value;
    settings.on_headers_complete = HttpParser::on_headers_complete;
    settings.on_body = HttpParser::on_body;
    settings.on_status = HttpParser::on_status_complete;
    settings.on_message_begin = HttpParser::on_message_begin;
    settings.on_message_complete = HttpParser::on_message_complete;
    settings.on_url = HttpParser::on_url;
}

HttpParserSettings::operator http_parser_settings*()
{
    return &settings;
}

}

#endif
