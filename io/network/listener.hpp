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
    }

    Listener(Listener&& other) : listener(0)
    {
        this->thread = std::move(other.thread);
        this->listener = std::move(other.listener);
    }

    ~Listener()
    {
        LOG_DEBUG("JOIN THREAD");
        alive.store(false, std::memory_order_release);

        if(thread.joinable())
            thread.join();
    }
    
    void start()
    {
        thread = std::thread([this]() { loop(); });
    }

    void add(Socket& socket, Epoll::Event& event)
    {
        listener.add(socket, &event);
    }

//protected:
    void loop()
    {
        LOG_DEBUG("Thread starting " << listener.id() << " " << hash(std::this_thread::get_id()));

        constexpr size_t MAX_EVENTS = 64;

        Epoll::Event events[MAX_EVENTS];

        while(alive.load(std::memory_order_acquire))
        {
            auto n = listener.wait(events, MAX_EVENTS, 1000);

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

        LOG_DEBUG("Thread exiting");
    }

    std::atomic<bool> alive {true};
    std::thread thread;

    Epoll listener;
};

}
