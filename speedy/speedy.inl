#ifndef MEMGRAPH_SPEEDY_INL
#define MEMGRAPH_SPEEDY_INL

#include "speedy.hpp"

namespace speedy
{

Speedy::Speedy(const http::Ipv4& ip) : server(loop), ip(ip)
{
}

void Speedy::get(const std::string path, http::request_cb_t callback)
{
}

void Speedy::listen()
{
    server.listen(ip, [](http::Request& req, http::Response& res) {
        res.send(req.url);
    });

    std::cout << "Server is UP" << std::endl;
    
    loop.run(uv::UvLoop::Mode::Default);
}

Speedy::~Speedy()
{
}

}

#endif
