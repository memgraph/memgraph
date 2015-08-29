#ifndef MEMGRAPH_THREADING_WORKER_HPP
#define MEMGRAPH_THREADING_WORKER_HPP

#include <thread>

template <class F, class... Args>
class Worker
{
public:
    Worker(F&& f, Args&&... args)
        : thread(f, args...) {}

    void join()
    {
        thread.join();
    }

private:
    std::thread thread;
};

#endif
