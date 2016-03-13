#pragma once

#include "io/network/server.hpp"
#include "debug/log.hpp"
#include "connection.hpp"

namespace http
{

/* const char* body = "Now that the internet can be accessed on any mobile device, whether a laptop, desktop, tablets, smartphone, websites are designed in responsive web version. It is the ability to change the page and font size according to the screen size of the user. desktop, tablets, smartphone, websites are designed in responsive web version. It is the ability to change the page and font size according to the screen size of the user. Thus a website is accessible anytime on any instrument. CSS3 frameworks were widely accepted in 2014 and is growing in 2015. It reduces time and money by helping not creating different sites for different users"; */

/* const char* body = ""; */

std::string response = "HTTP/1.1 200 OK\r\nContent-Length:0\r\n\r\n";

template <class Req, class Res>
class Parser : public io::Server<Parser<Req, Res>, Connection<Req, Res>>
{
    using Connection = Connection<Req, Res>;
    using Buffer = typename io::StreamReader<Parser<Req, Res>, Connection>::Buffer;

public:
    char buf[65536];

    Parser() = default;

    Connection& on_connect(io::Socket&& socket)
    {
        auto stream = new Connection(std::move(socket));
        LOG_DEBUG("on_connect socket " << stream->id());

        return *stream;
    }

    void on_error(Connection& conn)
    {
        LOG_DEBUG("on_error: " << conn.id());
    }

    void on_wait_timeout() {}

    Buffer on_alloc(Connection& conn)
    {
        LOG_DEBUG("on_alloc socket " << conn.id());
        return Buffer { buf, sizeof buf };
    }

    void on_read(Connection& conn, Buffer& buf)
    {
        LOG_DEBUG("on_read socket " << conn.id());
        auto& socket = conn.socket;
        socket.write(response.c_str(), response.size());
    }

    void on_close(Connection& conn)
    {
        LOG_DEBUG("on_close socket " << conn.id());
        conn.close();
    }
};

}
