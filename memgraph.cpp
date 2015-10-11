#include <iostream>
#include <vector>

#include "debug/log.hpp"
#include "utils/ioc/container.hpp"

#include "database/db.hpp"

#include "speedy/speedy.hpp"
#include "api/resources/include.hpp"

#include "threading/pool.hpp"
#include "threading/task.hpp"

int main()
{
    ioc::Container container;

    container.singleton<Db>();

    auto loop = container.singleton<uv::UvLoop>();
    auto app = container.singleton<sp::Speedy, uv::UvLoop>("/db/data");

    container.singleton<Pool>(4);
    container.singleton<Task, uv::UvLoop, Pool>();

    init(container);

    http::Ipv4 ip("0.0.0.0", 7474);
    app->listen(ip);

    loop->run(uv::UvLoop::Mode::Default);

    return 0;
}
