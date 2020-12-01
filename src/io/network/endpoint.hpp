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
class Endpoint {
 public:
  Endpoint();
  Endpoint(std::string ip_address, uint16_t port);
  // TODO: Remove these since members are public
  std::string address() const { return address_; }
  uint16_t port() const { return port_; }
  unsigned char family() const { return family_; }

  bool operator==(const Endpoint &other) const;
  friend std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint);

  std::string address_;
  uint16_t port_{0};
  unsigned char family_{0};

  static std::optional<std::pair<std::string, uint16_t>> ParseSocketOrIpAddress(
      const std::string &address, const std::optional<uint16_t> default_port);

 private:
  unsigned char GetIpFamily(const std::string &ip_address) const;
};

}  // namespace io::network
