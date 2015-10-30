#pragma once

#include <stdexcept>
#include <cstring>
#include <cstdio>
#include <cassert>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>

#include "addrinfo.hpp"
#include "utils/likely.hpp"

namespace io
{

class Socket
{
    Socket(int socket) : socket(socket) {}

    Socket(int family, int socket_type, int protocol)
    {
        socket = ::socket(family, socket_type, protocol);
    }

public:
    Socket(const Socket&) = delete;
    
    Socket(Socket&& other)
    {
        this->socket = other.socket;
        other.socket = -1;
    }

    ~Socket()
    {
        if(socket == -1)
            return;
        
        LOG_DEBUG("CLosing Socket " << socket);
        close(socket);
    }

    bool is_open()
    {
        return socket != -1;
    }

    static Socket create(const char* addr, const char* port)
    {
        auto info = AddrInfo::get(addr, port);

        for(struct addrinfo* it = info; it != nullptr; it = it->ai_next)
        {
            LOG_DEBUG("Trying socket...");

            auto s = Socket(it->ai_family, it->ai_socktype, it->ai_protocol);

            if(!s.is_open())
                continue;

            int on = 1;
            if(setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)))
                continue;

            if(s.bind(it->ai_addr, it->ai_addrlen))
                return std::move(s);
        }

        throw NetworkError("Unable to bind to socket");
    }

    bool bind(struct sockaddr* addr, socklen_t len)
    {
        assert(socket != -1);
        return ::bind(socket, addr, len) == 0;
    }

    void set_non_blocking()
    {
        auto flags = fcntl(socket, F_GETFL, 0);

        if(UNLIKELY(flags == -1))
            throw NetworkError("Cannot read flags from socket");

        flags |= O_NONBLOCK;

        auto status = fcntl(socket, F_SETFL, flags);

        if(UNLIKELY(status == -1))
            throw NetworkError("Cannot set NON_BLOCK flag to socket");
    }

    void listen(int backlog)
    {
        auto status = ::listen(socket, backlog);

        if(UNLIKELY(status == -1))
            throw NetworkError("Cannot listen on socket");
    }

    Socket accept(struct sockaddr* addr, socklen_t* len)
    {
        return Socket(::accept(socket, addr, len));
    }

    operator int() { return socket; }

    int id() const
    {
        return socket;
    }

private:
    int socket;
};

}
