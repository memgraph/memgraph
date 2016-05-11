#ifndef MEMGRAPH_SERVER_HTTP_IPV4_HPP
#define MEMGRAPH_SERVER_HTTP_IPV4_HPP

#include <string>
#include <ostream>
#include <uv.h>

namespace http
{

class Ipv4
{
public:
    Ipv4(const std::string& address, uint16_t port);

    operator const sockaddr_in&() const;

    operator const sockaddr*() const;

    friend std::ostream& operator<< (std::ostream& stream, const Ipv4& ip) {
        return stream << ip.address << ':' << ip.port;
    }

protected:
    sockaddr_in socket_address;

    const std::string& address;
    uint16_t port;
};

}

#endif
