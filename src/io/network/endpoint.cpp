#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <algorithm>

#include "glog/logging.h"

#include "utils/string.hpp"
#include "io/network/endpoint.hpp"

namespace io::network {

Endpoint::IpFamily Endpoint::GetIpFamily(const std::string &ip_address) {
  in_addr addr4;
  in6_addr addr6;
  int ipv4_result = inet_pton(AF_INET, ip_address.c_str(), &addr4);
  int ipv6_result = inet_pton(AF_INET6, ip_address.c_str(), &addr6);
  if (ipv4_result == 1) {
    return IpFamily::IP4;
  } else if (ipv6_result == 1) {
    return IpFamily::IP6;
  } else {
    return IpFamily::NONE;
  }
}

std::optional<std::pair<std::string, uint16_t>>
Endpoint::ParseSocketOrIpAddress(
    const std::string &address,
    const std::optional<uint16_t> default_port = {}) {
  /// expected address format:
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
      std::pair{address, *default_port};
    }
  } else if (parts.size() == 2) {
    ip_address = std::move(parts[0]);
    int64_t int_port{0};
    try {
      int_port = utils::ParseInt(parts[1]);
    } catch (utils::BasicException &e) {
      LOG(ERROR) << "Invalid port number: " << parts[1];
      return std::nullopt;
    }
    if (int_port > std::numeric_limits<uint16_t>::max()) {
      LOG(ERROR) << "Port number exceeded maximum possible size!";
      return std::nullopt;
    }
    port_number = static_cast<uint16_t>(int_port);

    return std::pair{ip_address, port_number};
  }

  return std::nullopt;
}

std::string Endpoint::SocketAddress() const {
  auto socket_address = address.empty() ? "EMPTY" : address;
  return address + ":" + std::to_string(port);
}

Endpoint::Endpoint() {}
Endpoint::Endpoint(std::string ip_address, uint16_t port)
    : address(std::move(ip_address)), port(port) {
  IpFamily ip_family = GetIpFamily(address);
  CHECK(ip_family != IpFamily::NONE)
      << "Not a valid IPv4 or IPv6 address: " << ip_address;
  family = ip_family;
}

bool Endpoint::operator==(const Endpoint &other) const {
  return address == other.address && port == other.port &&
         family == other.family;
}

std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint) {
  // no need to cover the IpFamily::NONE case, as you can't even construct an
  // Endpoint object if the IpFamily is NONE (i.e. the IP address is invalid)
  if (endpoint.family == Endpoint::IpFamily::IP6) {
    return os << "[" << endpoint.address << "]"
              << ":" << endpoint.port;
  }
  return os << endpoint.address << ":" << endpoint.port;
}

}  // namespace io::network
