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
  Endpoint(const std::string &address, uint16_t port);
  // TODO: Remove these since members are public
  std::string address() const { return address_; }
  uint16_t port() const { return port_; }
  unsigned char family() const { return family_; }

  bool operator==(const Endpoint &other) const;
  friend std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint);

  std::string address_;
  uint16_t port_{0};
  unsigned char family_{0};

 private:
  void SetFamilyIfIpValidOrThrowOtherwise(const std::string &ip_address);
};

}  // namespace io::network
