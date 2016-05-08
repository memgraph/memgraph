#include <iostream>

#include "speedy/speedy.hpp"
#include "resources/include.hpp"

int main(void)
{
    uv::UvLoop loop;
    speedy::Speedy app(loop);

    init(app);

    http::Ipv4 ip("0.0.0.0", 3400);
    app.listen(ip);

    loop.run(uv::UvLoop::Mode::Default);

    return 0;
}
