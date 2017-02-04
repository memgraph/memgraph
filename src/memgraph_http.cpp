#include <iostream>
#include <vector>

#include "debug/log.hpp"
#include "utils/ioc/container.hpp"

#include "database/graph_db.hpp"

#include "api/resources/include.hpp"
#include "speedy/speedy.hpp"

#include "threading/pool.hpp"
#include "threading/task.hpp"

#include "utils/terminate_handler.hpp"

int main(int argc, char *argv[])
{
    if (argc < 2) {
        std::cout << "Port not defined" << std::endl;
        std::exit(0);
    }

    auto port = std::stoi(argv[1]);

    std::set_terminate(&terminate_handler);

    ioc::Container container;

    container.singleton<Db>();

    auto loop = container.singleton<uv::UvLoop>();

    auto app = container.singleton<sp::Speedy, uv::UvLoop>("/db/data");
    container.singleton<Task, uv::UvLoop>();

    init(container);

    http::Ipv4 ip("0.0.0.0", port);
    app->listen(ip);

    loop->run(uv::UvLoop::Mode::Default);

    return 0;
}
