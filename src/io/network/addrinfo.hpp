#pragma once

namespace io::network {

/**
 * Wrapper class for getaddrinfo.
 * see: man 3 getaddrinfo
 */
class AddrInfo {
  AddrInfo(struct addrinfo* info);

 public:
  ~AddrInfo();

  static AddrInfo Get(const char* addr, const char* port);

  operator struct addrinfo*();

 private:
  struct addrinfo* info;
};
}
