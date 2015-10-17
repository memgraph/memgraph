#ifndef MEMGRAPH_SPEEDY_RAPIDJSON_MIDDLEWARE_HPP
#define MEMGRAPH_SPEEDY_RAPIDJSON_MIDDLEWARE_HPP

#include <iostream>

#include "request.hpp"
#include "response.hpp"
#include "http/status_codes.hpp"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

namespace sp
{

bool rapidjson_middleware(sp::Request& req, sp::Response& res)
{
    if(!req.json.Parse(req.body.c_str()).HasParseError())
        return true;

    auto error_str = rapidjson::GetParseError_En(req.json.GetParseError());
    std::string parse_error = "JSON parse error: " + std::string(error_str);

    res.send(http::Status::BadRequest, parse_error);
    return false;
}

}

#endif
