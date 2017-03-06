#include "io/network/network_endpoint.hpp"
#include "io/network/network_error.hpp"

#include <arpa/inet.h>
#include <netdb.h>

namespace io::network {

NetworkEndpoint::NetworkEndpoint() : port_(0), family_(0) {
  memset(address_, 0, sizeof address_);
  memset(port_str_, 0, sizeof port_str_);
}

NetworkEndpoint::NetworkEndpoint(const char* addr, const char* port) {
  if (addr == nullptr) throw NetworkEndpointException("Address can't be null!");
  if (port == nullptr) throw NetworkEndpointException("Port can't be null!");

  // strncpy isn't used because it does not guarantee an ending null terminator
  snprintf(address_, sizeof address_, "%s", addr);
  snprintf(port_str_, sizeof port_str_, "%s", port);

  is_address_valid();

  int ret = sscanf(port, "%hu", &port_);
  if (ret != 1) throw NetworkEndpointException("Port isn't valid!");
}

NetworkEndpoint::NetworkEndpoint(const std::string& addr,
                                 const std::string& port)
    : NetworkEndpoint(addr.c_str(), port.c_str()) {}

NetworkEndpoint::NetworkEndpoint(const char* addr, unsigned short port) {
  if (addr == nullptr) throw NetworkEndpointException("Address can't be null!");

  snprintf(address_, sizeof address_, "%s", addr);
  snprintf(port_str_, sizeof port_str_, "%hu", port);
  port_ = port;

  is_address_valid();
}

void NetworkEndpoint::is_address_valid() {
  in_addr addr4;
  in6_addr addr6;
  int ret = inet_pton(AF_INET, address_, &addr4);
  if (ret != 1) {
    ret = inet_pton(AF_INET6, address_, &addr6);
    if (ret != 1)
      throw NetworkEndpointException(
          "Address isn't a valid IPv4 or IPv6 address!");
    else
      family_ = 6;
  } else
    family_ = 4;
}

const char* NetworkEndpoint::address() { return address_; }
const char* NetworkEndpoint::port_str() { return port_str_; }
unsigned short NetworkEndpoint::port() { return port_; }
unsigned char NetworkEndpoint::family() { return family_; }
}
