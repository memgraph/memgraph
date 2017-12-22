#include <arpa/inet.h>
#include <netdb.h>
#include <algorithm>

#include "glog/logging.h"

#include "io/network/network_endpoint.hpp"

namespace io::network {

NetworkEndpoint::NetworkEndpoint() : port_(0), family_(0) {
  memset(address_, 0, sizeof address_);
  memset(port_str_, 0, sizeof port_str_);
}

NetworkEndpoint::NetworkEndpoint(const char *addr, const char *port) {
  if (!addr || !port) LOG(FATAL) << "Address or port is nullptr";

  // strncpy isn't used because it does not guarantee an ending null terminator
  snprintf(address_, sizeof address_, "%s", addr);
  snprintf(port_str_, sizeof port_str_, "%s", port);

  in_addr addr4;
  in6_addr addr6;
  int ret = inet_pton(AF_INET, address_, &addr4);
  if (ret != 1) {
    ret = inet_pton(AF_INET6, address_, &addr6);
    if (ret != 1) {
      LOG(FATAL) << "Not a valid IPv4 or IPv6 address: " << *addr;
    }
    family_ = 6;
  } else {
    family_ = 4;
  }

  ret = sscanf(port, "%hu", &port_);
  if (ret != 1) LOG(FATAL) << "Not a valid port: " << *port;
}

NetworkEndpoint::NetworkEndpoint(const std::string &addr,
                                 const std::string &port)
    : NetworkEndpoint(addr.c_str(), port.c_str()) {}

NetworkEndpoint::NetworkEndpoint(const std::string &addr, uint16_t port)
    : NetworkEndpoint(addr.c_str(), std::to_string(port)) {}

bool NetworkEndpoint::operator==(const NetworkEndpoint &other) const {
  return std::equal(std::begin(address_), std::end(address_),
                    std::begin(other.address_)) &&
         port_ == other.port_ && family_ == other.family_;
}

}  // namespace io::network
