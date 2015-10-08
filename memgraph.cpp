#include <iostream>
#include <vector>

#include "speedy/speedy.hpp"
#include "api/resources/include.hpp"

#include "utils/auto_scope.hpp"

int main(int argc, char** argv)
{
    uv::UvLoop loop;
    sp::Speedy app(loop);

    init(app);

    http::Ipv4 ip("0.0.0.0", 3400);
    app.listen(ip);

    loop.run(uv::UvLoop::Mode::Default);

    return 0;
}
