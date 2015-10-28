#pragma once

#include <list>

#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <cstring>

#include <atomic>

#include "epoll.hpp"

namespace io
{

class TcpConnection
{

};

class TcpServer
{
public:
    TcpServer(const char* port)
        : socket(Socket::create(port)), epoll(0)
    {
        socket.set_non_blocking();
        socket.listen(128);

        listener.data.fd = socket;
        listener.events = EPOLLIN | EPOLLET;

        epoll.add(socket, &listener);
    }

    void start(int max_events)
    {
        Epoll::EventBuffer events(max_events);

        while(alive)
        {
            auto n = epoll.wait(events, events.size(), -1);
            
            for(int i = 0; i < n; ++i)
                process_event(events[i]);
        }
    }

    void stop()
    {
        alive.store(false, std::memory_order_release);
    }

private:
    Socket socket;
    Epoll epoll;
    Epoll::event listener;
    std::atomic<bool> alive { true };
    std::list<Socket> sockets;

    void process_event(Epoll::event& event)
    {
        //if(UNLIKELY(event.events & (EPOLLERR | EPOLLHUP | EPOLLIN)))

        if ((event.events & EPOLLERR) ||
              (event.events & EPOLLHUP) ||
              (!(event.events & EPOLLIN)))
        {
#ifndef NDEBUG
            LOG_DEBUG("Epoll error!");
#endif
            // close the connection
            close(event.data.fd);
            return;
        }

        if(event.data.fd == socket)
        {
            while(accept_connection()) {}
            return;
        }

        while(read_data(event)) {}
    }

    bool accept_connection()
    {
#ifndef NDEBUG
        struct sockaddr addr;
        socklen_t len;
        
        auto clientt = socket.accept(&addr, &len);
#else
        auto clientt = socket.accept(nullptr, nullptr);
#endif
        if(UNLIKELY(!clientt.is_open()))
            return false;

#ifndef NDEBUG
        char host[NI_MAXHOST], port[NI_MAXSERV];
        auto status = getnameinfo(&addr, len,
                                  host, sizeof host,
                                  port, sizeof port,
                                  NI_NUMERICHOST | NI_NUMERICSERV);

        if(status)
        {
            LOG_DEBUG("Accepted connection on descriptor " << clientt
                      << " (host: " << std::string(host)
                      << ", port: " << std::string(port) << ")");
        }
#endif
        sockets.emplace_back(std::move(clientt));
        auto& client = sockets.back();

        client.set_non_blocking();

        auto ev = new Epoll::event;
        ev->events = EPOLLIN | EPOLLET;
        ev->data.fd = client;

        epoll.add(client, ev);
        return true;
    }

    bool read_data(Epoll::event& event)
    {
        char buf[512];

        auto count = read(event.data.fd, buf, sizeof buf);

        if(count == -1)
            return false;

        if(!count)
        {
            close(event.data.fd);
            return false;
        }

        const char* response = "HTTP/1.1 200 OK\r\nContent-Length:0\r\nConnection:Keep-Alive\r\n\r\n";

        write(event.data.fd, response, sizeof response);
        return true;
    }
};

}
