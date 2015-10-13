#ifndef MEMGRAPH_SPEEDY_REQUEST_HPP
#define MEMGRAPH_SPEEDY_REQUEST_HPP

#include <vector>

#include "http/request.hpp"

namespace sp
{

class Request : public http::Request
{
public:
    using http::Request::Request;

    // todo json body insitu parsing
    // http://rapidjson.org/md_doc_dom.html

    std::vector<std::string> params;
};

}

#endif
