#ifndef MEMGRAPH_SERVER_HTTP_IPV4_INL
#define MEMGRAPH_SERVER_HTTP_IPV4_INL

#include "ipv4.hpp"
#include "http_error.hpp"

namespace http
{

Ipv4::Ipv4(const std::string& address, uint16_t port)
    : address(address), port(port)
{
    auto status = uv_ip4_addr(address.c_str(), port, &socket_address);
    
    if(status != 0)
        throw HttpError("Not a valid IP address/port (" + address + ":"
                        + std::to_string(port) + ")");
}

Ipv4::operator const sockaddr_in&() const
{
    return socket_address;
}

Ipv4::operator const sockaddr*() const
{
    return (const struct sockaddr*)&socket_address;
}
    
}

#endif
