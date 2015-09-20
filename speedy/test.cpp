#include <iostream>

#include "speedy.hpp"

int main(void)
{
    uv::UvLoop loop;
    http::Ipv4 ip("0.0.0.0", 3400);

    speedy::Speedy app(loop, ip);
    app.listen();

    loop.run(uv::UvLoop::Mode::Default);

    return 0;
}
