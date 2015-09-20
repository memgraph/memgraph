#ifndef MEMGRAPH_SPEEDY_HPP
#define MEMGRAPH_SPEEDY_HPP

#include "io/uv/uv.hpp"
#include "http/http.hpp"

namespace speedy
{

class Speedy
{
private:
    http::HttpServer server;
    http::Ipv4 ip;
public:
    Speedy(uv::UvLoop& loop, const http::Ipv4& ip);
    void get(const std::string path, http::request_cb_t callback); 
    void listen();
    ~Speedy();
};

}

#include "speedy.inl"

#endif
