#ifndef MEMGRAPH_SERVER_HTTP_METHOD_HPP
#define MEMGRAPH_SERVER_HTTP_METHOD_HPP

#include <http_parser.h>

namespace http
{

enum Method
{
    GET    = HTTP_GET,
    HEAD   = HTTP_HEAD,
    POST   = HTTP_POST,
    PUT    = HTTP_PUT,
    DELETE = HTTP_DELETE,
};

}

#endif
