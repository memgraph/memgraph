#include <iostream>
#include <vector>

#include "debug/log.hpp"
#include "utils/ioc/container.hpp"

#include "database/db.hpp"

#include "speedy/speedy.hpp"
#include "api/resources/include.hpp"

#include "threading/pool.hpp"
#include "threading/task.hpp"

#include <execinfo.h>

// TODO: move to separate header or source file
// TODO: log to local file or remote database
void stacktrace() noexcept
{
    void *array[50];
    int size = backtrace(array, 50);    
    std::cout << __FUNCTION__ << " backtrace returned " << size << " frames\n\n";
    char **messages = backtrace_symbols(array, size);
    for (int i = 0; i < size && messages != NULL; ++i) {
        std::cout << "[bt]: (" << i << ") " << messages[i] << std::endl;
    }
    std::cout << std::endl;
    free(messages);
}

// TODO: log to local file or remote database
void on_terminate() noexcept
{
    if (auto exc = std::current_exception()) { 
        try {
            std::rethrow_exception(exc);
        } catch (std::exception &ex) {
            std::cout << ex.what() << std::endl << std::endl;
            stacktrace();
        }
    }
    std::_Exit(EXIT_FAILURE);
}

int main()
{
    std::set_terminate(&on_terminate);

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
