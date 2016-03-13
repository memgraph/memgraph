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
    Socket(int family, int socket_type, int protocol)
    {
        socket = ::socket(family, socket_type, protocol);
    }

public:
    Socket(int socket = -1) : socket(socket) {}

    Socket(const Socket&) = delete;

    Socket(Socket&& other)
    {
        *this = std::forward<Socket>(other);
    }

    ~Socket()
    {
        if(socket == -1)
            return;

        close(socket);
    }

    Socket& operator=(Socket&& other)
    {
        this->socket = other.socket;
        other.socket = -1;
        return *this;
    }

    bool is_open()
    {
        return socket != -1;
    }

    static Socket connect(const std::string& addr, const std::string& port)
    {
        return connect(addr.c_str(), port.c_str());
    }

    static Socket connect(const char* addr, const char* port)
    {
        auto info = AddrInfo::get(addr, port);

        for(struct addrinfo* it = info; it != nullptr; it = it->ai_next)
        {
            auto s = Socket(it->ai_family, it->ai_socktype, it->ai_protocol);

            if(!s.is_open())
                continue;

            if(::connect(s, it->ai_addr, it->ai_addrlen) == 0)
                return std::move(s);
        }

        throw NetworkError("Unable to connect to socket");
    }

    static Socket bind(const std::string& addr, const std::string& port)
    {
        return bind(addr.c_str(), port.c_str());
    }

    static Socket bind(const char* addr, const char* port)
    {
        auto info = AddrInfo::get(addr, port);

        for(struct addrinfo* it = info; it != nullptr; it = it->ai_next)
        {
            auto s = Socket(it->ai_family, it->ai_socktype, it->ai_protocol);

            if(!s.is_open())
                continue;

            int on = 1;
            if(setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)))
                continue;

            if(::bind(s, it->ai_addr, it->ai_addrlen) == 0)
                return std::move(s);
        }

        throw NetworkError("Unable to bind to socket");
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

    size_t write(const std::string& str)
    {
        return ::write(socket, str.c_str(), str.size());
    }

    size_t write(const char* data, size_t len)
    {
        return ::write(socket, data, len);
    }

    size_t read(char* buffer, size_t len)
    {
        return ::read(socket, buffer, len);
    }

private:
    int socket;
};

}
