#pragma once

#include "api/restful/resource.hpp"

#pragma url /ping
class Ping : public Resource<Ping, GET>
{
public:
    using Resource::Resource;

    void get(sp::Request& req, sp::Response& res)
    {
        return res.send(http::Status::NoContent, "");
    }
};
