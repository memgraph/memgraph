#pragma once

#include <netinet/in.h>
#include <cstdint>
#include <string>

#include "boost/serialization/access.hpp"

#include "utils/exceptions.hpp"

namespace io::network {

/**
 * This class represents a network endpoint that is used in Socket.
 * It is used when connecting to an address and to get the current
 * connection address.
 */
class NetworkEndpoint {
 public:
  NetworkEndpoint();
  NetworkEndpoint(const std::string &addr, const std::string &port);
  NetworkEndpoint(const char *addr, const char *port);
  NetworkEndpoint(const std::string &addr, uint16_t port);

  const char *address() const { return address_; }
  const char *port_str() const { return port_str_; }
  uint16_t port() const { return port_; }
  unsigned char family() const { return family_; }

  bool operator==(const NetworkEndpoint &other) const;

 private:
  friend class boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &address_;
    ar &port_str_;
    ar &port_;
    ar &family_;
  }

  char address_[INET6_ADDRSTRLEN];
  char port_str_[6];
  uint16_t port_;
  unsigned char family_;
};

}  // namespace io::network
