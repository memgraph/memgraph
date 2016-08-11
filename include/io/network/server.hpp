#pragma once

#include "io/network/stream_reader.hpp"

namespace io
{

template <class Derived>
class Server : public EventListener<Derived>
{
public:
    Server(Socket&& socket) : socket(std::forward<Socket>(socket))
    {
        event.data.fd = this->socket;
        event.events = EPOLLIN | EPOLLET;

        this->listener.add(this->socket, &event);
    }

    void on_close_event(Epoll::Event& event)
    {
        ::close(event.data.fd);
    }

    void on_error_event(Epoll::Event& event)
    {
        ::close(event.data.fd);
    }

    void on_data_event(Epoll::Event& event)
    {
        if(UNLIKELY(socket != event.data.fd))
            return;

        this->derived().on_connect();
    }

protected:
    Epoll::Event event;
    Socket socket;
};

}
