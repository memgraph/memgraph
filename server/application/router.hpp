#ifndef MEMGRAPH_SERVER_APPLICATION_ROUTER_HPP
#define MEMGRAPH_SERVER_APPLICATION_ROUTER_HPP

// TODO: find more appropriate way to include header files
#include "../uv/uv.hpp"
#include "../http/http.hpp"

namespace application
{

// TODO: find out more appropriate name, like express
class Router 
{
private:
    uv::UvLoop loop;
    http::HttpServer server;
    http::Ipv4 ip;
public:
    Router(const http::Ipv4& ip);
    void get(const std::string path, http::request_cb_t callback); 
    void listen();
    ~Router();
};

}

#endif
