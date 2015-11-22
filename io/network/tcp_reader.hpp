#pragma once

#include "tcp_listener.hpp"
#include "tcp_stream.hpp"

namespace io
{

template <class Derived>
class TcpReader : public TcpListener<TcpReader<Derived>>, public Crtp<Derived>
{
    using listener_t = TcpListener<TcpReader<Derived>>;
    using Crtp<Derived>::derived;

public:
    TcpReader() : listener_t(0) {}

    struct Buffer
    {
        char* ptr;
        size_t len;
    };

    bool accept(Socket& socket)
    {
        // accept a connection from a socket
        auto s = socket.accept(nullptr, nullptr);

        if(!s.is_open())
            return false;

        // make the recieved socket non blocking
        s.set_non_blocking();

        auto& stream = derived().on_connect(std::move(s));

        // we want to listen to an incoming event whish is edge triggered and
        // we also want to listen on the hangup event
        stream.event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;

        // add the connection to the event listener
        this->add(stream);

        return true;
    }

    void on_close(TcpStream& stream)
    {
        derived().on_close(stream);
    }

    void on_error(TcpStream& stream)
    {
        derived().on_error(stream);
    }

    void on_wait_timeout()
    {
        derived().on_wait_timeout();
    }

    void on_data(TcpStream& stream)
    {
        constexpr size_t suggested_size = 64_kB;

        while(true)
        {
            Buffer buf = derived().on_alloc(suggested_size);

            buf.len = read(stream.socket, buf.ptr, buf.len);

            if(buf.len == -1)
            {
                if(UNLIKELY(errno != EAGAIN))
                    derived().on_error(stream);

                break;
            }

            if(UNLIKELY(buf.len == 0))
            {
                stream.close();
                break;
            }

            derived().on_read(stream, buf);
        }
    }
};

}
