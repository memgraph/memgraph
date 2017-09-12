#include <netdb.h>
#include <cstring>

#include "io/network/addrinfo.hpp"

#include "io/network/network_error.hpp"

namespace io::network {

AddrInfo::AddrInfo(struct addrinfo *info) : info(info) {}

AddrInfo::~AddrInfo() { freeaddrinfo(info); }

AddrInfo AddrInfo::Get(const char *addr, const char *port) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));

  hints.ai_family = AF_UNSPEC;      // IPv4 and IPv6
  hints.ai_socktype = SOCK_STREAM;  // TCP socket
  hints.ai_flags = AI_PASSIVE;

  struct addrinfo *result;
  auto status = getaddrinfo(addr, port, &hints, &result);

  if (status != 0) throw NetworkError(gai_strerror(status));

  return AddrInfo(result);
}

AddrInfo::operator struct addrinfo *() { return info; }
}
