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
    // the body is empty and json parsing isn't necessary
    if (req.body.empty())
        return true;

    if (req.json.Parse(req.body.c_str()).HasParseError()) {
        auto errorCode = rapidjson::GetParseError_En(req.json.GetParseError());
        std::string parseError = "JSON parse error: " + std::string(errorCode);
        res.send(http::Status::BadRequest, parseError);
        return false;
    }

    return true;
}

}

#endif
