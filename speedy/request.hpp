#pragma once

#include <vector>
#include "rapidjson/document.h"

#include "http/request.hpp"

namespace sp
{

class Request : public http::Request
{
public:
    using http::Request::Request;

    // http://rapidjson.org/md_doc_dom.html
    rapidjson::Document json;

    std::vector<std::string> params;
};

}
