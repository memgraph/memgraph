#ifndef MEMGRAPH_SPEEDY_RESPONSE_HPP
#define MEMGRAPH_SPEEDY_RESPONSE_HPP

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

    std::string json_string(const rapidjson::Document& document)
    {
        rapidjson::StringBuffer strbuf;
        rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
        document.Accept(writer);
        // TODO: error handling
        auto str = strbuf.GetString();
        return str;
    }

    void json(http::Status code, const rapidjson::Document& document)
    {
        auto str = json_string(document);
        this->send(code, str);
    }

    void json(const rapidjson::Document& document)
    {
        auto str = json_string(document);
        this->send(str);
    }
};

using request_cb_t = std::function<void(Request&, Response&)>;

}

#endif
