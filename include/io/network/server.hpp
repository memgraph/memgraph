#pragma once

#include "io/network/stream_reader.hpp"

namespace io
{

template <class Derived>
class Server : public EventListener<Derived>
{
public:
    Server(Socket &&socket) : socket(std::forward<Socket>(socket)),
        logger(logging::log->logger("io::Server"))
    {
        event.data.fd = this->socket;

        // TODO: EPOLLET is hard to use -> figure out how should EPOLLET be used
        // event.events = EPOLLIN | EPOLLET;
        event.events = EPOLLIN;

        this->listener.add(this->socket, &event);
    }

    void on_close_event(Epoll::Event &event) { ::close(event.data.fd); }

    void on_error_event(Epoll::Event &event) { ::close(event.data.fd); }

    void on_data_event(Epoll::Event &event)
    {
        if (UNLIKELY(socket != event.data.fd)) return;

        this->derived().on_connect();
    }

    template <class... Args>
    void on_exception_event(Epoll::Event &event, Args &&... args)
    {
        // TODO: Do something about it
        logger.warn("epoll exception");
    }

protected:
    Epoll::Event event;
    Socket socket;
    Logger logger;
};
}
