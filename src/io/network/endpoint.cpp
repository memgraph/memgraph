#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <algorithm>
#include <optional>

#include "glog/logging.h"

#include "utils/string.hpp"
#include "io/network/endpoint.hpp"

namespace io::network {

void Endpoint::SetFamilyIfIpValid(const std::string &ip_address) {
  in_addr addr4;
  in6_addr addr6;
  int ipv4_result = inet_pton(AF_INET, ip_address.c_str(), &addr4);
  int ipv6_result = inet_pton(AF_INET6, ip_address.c_str(), &addr6);
  if (ipv4_result == 1)
    family_ = 4;
  else if (ipv6_result == 1)
    family_ = 6;
  CHECK(family_ != 0) << "Not a valid IPv4 or IPv6 address: " << ip_address;
}

std::optional<std::pair<std::string, uint16_t>>
Endpoint::ParseSocketOrIpAddress(
    const std::string &address,
    const std::optional<uint16_t> default_port = {}) {
  /// address format:
  ///   - "ip_address:port_number"
  ///   - "ip_address"
  /// We parse the address first. If it's an IP address, a default port must
  // be given, or we return nullopt. If it's a socket address, we try to parse
  // it into an ip address and a port number; even if a default port is given,
  // it won't be used, as we expect that it is given in the address string.
  const std::string delimiter = ":";
  std::string ip_address;
  uint16_t port_number{0};

  std::vector<std::string> parts = utils::Split(address, delimiter);
  if (parts.size() == 1) {
    if (default_port) {
      std::make_pair(address, *default_port);
    }
  } else if (parts.size() == 2) {
    ip_address = std::move(parts[0]);
    int64_t int_port{0};
    try {
      int_port = utils::ParseInt(parts[1]);
    } catch (std::exception &e) {
      LOG(ERROR) << "Invalid port number: " << parts[1];
      return std::nullopt;
    }
    CHECK(int_port > std::numeric_limits<uint16_t>::max())
        << "Port number exceeded maximum possible size!";
    port_number = static_cast<uint16_t>(int_port);

    return std::make_pair(ip_address, port_number);
  }

  return std::nullopt;
}

Endpoint::Endpoint() {}
Endpoint::Endpoint(std::string address, uint16_t port)
    : address_(std::move(address)), port_(port) {
  SetFamilyIfIpValid(address_);
}

bool Endpoint::operator==(const Endpoint &other) const {
  return address_ == other.address_ && port_ == other.port_ &&
         family_ == other.family_;
}

std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint) {
  if (endpoint.family() == 6) {
    return os << "[" << endpoint.address() << "]"
              << ":" << endpoint.port();
  }
  return os << endpoint.address() << ":" << endpoint.port();
}

}  // namespace io::network
