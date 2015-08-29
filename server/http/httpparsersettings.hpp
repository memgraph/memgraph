#ifndef MEMGRAPH_SERVER_HTTP_HTTPPARSERSETTINGS_HPP
#define MEMGRAPH_SERVER_HTTP_HTTPPARSERSETTINGS_HPP

#include <http_parser.h>

namespace http
{

class HttpParserSettings
{
public:
    HttpParserSettings();

    operator http_parser_settings*();

private:
    http_parser_settings settings;
};

}

#endif
