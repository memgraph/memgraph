#ifndef MEMGRAPH_SPEEDY_HPP
#define MEMGRAPH_SPEEDY_HPP

#include <vector>

#include "io/uv/uv.hpp"
#include "http/http.hpp"
#include "r3_include.h"

namespace speedy
{

class Speedy
{
private:
    http::HttpServer server;
    http::Ipv4 ip;
    node *n;
public:
    Speedy(uv::UvLoop& loop, const http::Ipv4& ip);
    void get(const std::string &path, http::request_cb_t callback); 
    void listen();
};

}

#include "speedy.inl"

#endif
