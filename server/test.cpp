#include <iostream>

#include "uvmachine.hpp"

int main(void)
{
    uv::UvLoop loop;
    http::HttpServer server(loop);

    http::Ipv4 ip("0.0.0.0", 3400);

    server.listen(ip, [](http::Request& req, http::Response& res) {
        res.send(req.url);
    });

    loop.run(uv::UvLoop::Mode::Default);

    return 0;
}
