#pragma once

#include <string>

#include "memory/literals.hpp"
#include "cypher_replacer.hpp"

using namespace memory::literals;

class Cypher
{
    static std::string body_start;
    static std::string body_end;

public:
    Cypher()
    {
        request.reserve(64_kB);
    }

    std::string& operator()(std::string query)
    {
        request.clear();
        replacer(query);

        // request begin and headers
        request += "POST /db/data/transaction/commit HTTP/1.1\r\n" \
                   "Host: localhost:7474\r\n" \
                   "Authorization: Basic bmVvNGo6cGFzcw==\r\n" \
                   "Accept: application/json; charset=UTF-8\r\n" \
                   "Content-Type: application/json\r\n" \
                   "Content-Length: ";

        // content length
        auto size = body_start.size() + query.size() + body_end.size();
        request += std::to_string(size);

        // headers end
        request += "\r\n\r\n";

        // write body
        request += body_start;
        request += query;
        request += body_end;

        return request;
    }

private:
    std::string request;
    CypherReplacer replacer;
};

std::string Cypher::body_start = "{\"statements\":[{\"statement\":\"";
std::string Cypher::body_end = "\"}]}";
