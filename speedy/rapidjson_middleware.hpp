#pragma once

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

    // the body is successfuly parsed
    if(!req.json.Parse(req.body.c_str()).HasParseError())
        return true;

    // some kind of parse error occurred
    // return the error message to the client
    auto error_str = rapidjson::GetParseError_En(req.json.GetParseError());
    std::string parse_error = "JSON parse error: " + std::string(error_str);
    res.send(http::Status::BadRequest, parse_error);

    // stop further execution
    return false;
}

}
