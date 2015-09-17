#ifndef MEMGRAPH_SERVER_APPLICATION_ROUTER_INL
#define MEMGRAPH_SERVER_APPLICATION_ROUTER_INL

#include "router.hpp"

namespace application
{

Router::Router(const http::Ipv4& ip) : server(loop), ip(ip)
{
}

void Router::get(const std::string path, http::request_cb_t callback)
{
}

void Router::listen()
{
    server.listen(ip, [](http::Request& req, http::Response& res) {
        res.send(req.url);
    });

    std::cout << "Server is UP" << std::endl;
    
    loop.run(uv::UvLoop::Mode::Default);
}

Router::~Router()
{
}

}

#endif
