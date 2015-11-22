#pragma once

#include <list>
#include <thread>
#include <atomic>

#include "debug/log.hpp"
#include "tcp_listener.hpp"

namespace io
{

template <class T>
class TcpServer : TcpListener<TcpServer<T>, 64, -1>
{
public:
    TcpServer(const char* addr, const char* port)
        : stream(std::move(Socket::bind(addr, port))) {}

    ~TcpServer()
    {
        stop();

        for(auto& worker : workers)
            worker.join();
    }

    template <class F>
    void listen(size_t n, size_t backlog, F&& f)
    {
        for(size_t i = 0; i < n; ++i)
        {
            workers.emplace_back();
            auto& w = workers.back();

            threads[i] = std::thread([this, &w]() {
                while(alive)
                {
                    LOG_DEBUG("Worker " << hash(std::this_thread::get_id())
                              << " waiting... ");
                    w.wait_and_process_events();
                }
            });
        }

        stream.socket.listen(backlog);
    }

    void stop()
    {
        alive.store(false, std::memory_order_release);
    }

private:
    std::list<std::thread> threads;
    std::list<T> workers;
    std::atomic<bool> alive { true };

    TcpStream stream;
    size_t idx = 0;

    void on_close(TcpStream& stream)
    {
        LOG_DEBUG("server on_close!!!!");
    }

    void on_error(TcpStream& stream)
    {
        LOG_DEBUG("server on_error!!!!");
    }

    void on_data(TcpStream&)
    {
        while (true)
        {
            LOG_DEBUG("Trying to accept... ");
            if(!workers[idx].accept(socket))
            {
                LOG_DEBUG("Did not accept!");
                break;
            }

            idx = (idx + 1) % workers.size();
            LOG_DEBUG("Accepted a new connection!");
        }
    }

    void on_wait_timeout()
    {

    }
};

}
