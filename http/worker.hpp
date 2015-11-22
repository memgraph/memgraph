#pragma once

#include "io/network/tcp_reader.hpp"
#include "debug/log.hpp"

namespace http
{

const char* body = "Now that the internet can be accessed on any mobile device, whether a laptop, desktop, tablets, smartphone, websites are designed in responsive web version. It is the ability to change the page and font size according to the screen size of the user. desktop, tablets, smartphone, websites are designed in responsive web version. It is the ability to change the page and font size according to the screen size of the user. Thus a website is accessible anytime on any instrument. CSS3 frameworks were widely accepted in 2014 and is growing in 2015. It reduces time and money by helping not creating different sites for different users";

std::string response = "HTTP/1.1 200 OK\r\nContent-Length:"
    + std::to_string(strlen(body)) + "\r\n\r\n" + body;

class Worker : public io::TcpReader<Worker>
{
public:
    char buf[65536];

    Worker() = default;

    io::TcpStream& on_connect(io::Socket&& socket)
    {
        auto stream = new io::TcpStream(std::move(socket));
        LOG_DEBUG("on_connect socket " << stream->id());

        return *stream;
    }

    void on_error(io::TcpStream& stream)
    {
        LOG_DEBUG("on_error: " << stream.id());
    }

    void on_wait_timeout()
    {
        LOG_DEBUG("Worker on_wait_timeout");
    }

    Buffer on_alloc(size_t suggested_size)
    {
        LOG_DEBUG("Allocating buffer");

        return Buffer { buf, sizeof buf };
    }

    void on_read(io::TcpStream& stream, Buffer& buf)
    {
        LOG_DEBUG("on_read (socket: " << stream.id() <<  "): '"
            << std::string(buf.ptr, buf.len) << "'");

        auto& socket = stream.socket;

        auto n = write(socket, response.c_str(), response.size());

        LOG_DEBUG("Responded with " << n << " characters");
    }

    void on_close(io::TcpStream& stream)
    {
        LOG_DEBUG("on_close: " << stream.id());
        stream.close();
    }
};

}
