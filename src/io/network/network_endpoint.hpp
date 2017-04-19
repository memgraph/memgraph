#pragma once

#include "utils/exceptions.hpp"

#include <netinet/in.h>
#include <string>

namespace io::network {

class NetworkEndpointException : public utils::BasicException {
 public:
  using utils::BasicException::BasicException;
};

/**
 * This class represents a network endpoint that is used in Socket.
 * It is used when connecting to an address and to get the current
 * connection address.
 */
class NetworkEndpoint {
 public:
  NetworkEndpoint();
  NetworkEndpoint(const char* addr, const char* port);
  NetworkEndpoint(const char* addr, unsigned short port);
  NetworkEndpoint(const std::string& addr, const std::string& port);

  const char* address();
  const char* port_str();
  unsigned short port();
  unsigned char family();

 private:
  void is_address_valid();

  char address_[INET6_ADDRSTRLEN];
  char port_str_[6];
  unsigned short port_;
  unsigned char family_;
};
}
