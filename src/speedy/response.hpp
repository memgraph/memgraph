#pragma once

#include "request.hpp"
#include "http/response.hpp"

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

namespace sp
{

class Response : public http::Response<Request, Response>
{
public:
    using http::Response<Request, Response>::Response;

    void json(http::Status code, const rapidjson::Document& document)
    {
        rapidjson::StringBuffer strbuf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
        document.Accept(writer);
        // TODO: error handling
        this->send(code, strbuf.GetString());
    }

    void json(const rapidjson::Document& document)
    {
        this->json(http::Status::Ok, document);
    }
};

using request_cb_t = std::function<void(Request&, Response&)>;

}
