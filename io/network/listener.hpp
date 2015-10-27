#pragma once

#include <thread>
#include <atomic>

#include "socket.hpp"
#include "epoll.hpp"
#include "tcp_stream.hpp"

#include "utils/crtp.hpp"

namespace io
{

template <class Derived>
class Listener : public Crtp<Derived>
{
public:
    Listener() : listener(0)
    {
        thread = std::thread([this]() { loop(); });
    }

    Listener(Listener&& other)
    {
        this->thread = std::move(other.thread);
        this->listener = std::move(other.listener);
    }

    ~Listener()
    {
        alive.store(false, std::memory_order_release);
        thread.join();
    }

    void add(Socket& socket, Epoll::Event& event)
    {
        listener.add(socket, &event);
    }

private:
    void loop()
    {
        constexpr size_t MAX_EVENTS = 64;

        Epoll::Event events[MAX_EVENTS];

        while(alive.load(std::memory_order_acquire))
        {
            auto n = listener.wait(events, MAX_EVENTS, -1);

            for(int i = 0; i < n; ++i)
            {
                auto& event = events[i];
                auto stream = reinterpret_cast<TcpStream*>(event.data.ptr);

                if(!(event.events & EPOLLIN))
                {
                    LOG_DEBUG("error !EPOLLIN");
                    this->derived().on_error(stream);
                    continue;
                }

                if(event.events & EPOLLHUP)
                {
                    LOG_DEBUG("error EPOLLHUP");
                    this->derived().on_error(stream);
                    continue;
                }

                if(event.events & EPOLLERR)
                {
                    LOG_DEBUG("error EPOLLERR");
                    this->derived().on_error(stream);
                    continue;
                }

                this->derived().on_read(stream);
            }
        }
    }

    std::atomic<bool> alive {true};
    std::thread thread;

    Epoll listener;
};

}
