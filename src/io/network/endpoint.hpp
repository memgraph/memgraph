#pragma once

#include <netinet/in.h>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>

namespace io::network {

/**
 * This class represents a network endpoint that is used in Socket.
 * It is used when connecting to an address and to get the current
 * connection address.
 */
struct Endpoint {
  Endpoint();
  Endpoint(std::string ip_address, uint16_t port);

  enum class IpFamily : std::uint8_t { NONE, IP4, IP6 };

  std::string SocketAddress() const;

  bool operator==(const Endpoint &other) const = default;
  friend std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint);

  std::string address;
  uint16_t port{0};
  IpFamily family{IpFamily::NONE};

  static std::optional<std::pair<std::string, uint16_t>> ParseSocketOrIpAddress(
      const std::string &address, const std::optional<uint16_t> default_port);

  static IpFamily GetIpFamily(const std::string &ip_address);
};

}  // namespace io::network
