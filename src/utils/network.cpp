#include "utils/network.hpp"

#include <arpa/inet.h>
#include <netdb.h>

#include <cstring>
#include <string>

#include "glog/logging.h"

namespace utils {

/// Resolves hostname to ip, if already an ip, just returns it
std::string ResolveHostname(std::string hostname) {
  addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;  // use AF_INET6 to force IPv6
  hints.ai_socktype = SOCK_STREAM;

  int addr_result;
  addrinfo *servinfo;
  CHECK((addr_result =
             getaddrinfo(hostname.c_str(), NULL, &hints, &servinfo)) == 0)
      << "Error with getaddrinfo:" << gai_strerror(addr_result);
  CHECK(servinfo) << "Could not resolve address: " << hostname;

  std::string address;
  if (servinfo->ai_family == AF_INET) {
    sockaddr_in *hipv4 = (sockaddr_in *)servinfo->ai_addr;
    char astring[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(hipv4->sin_addr), astring, INET_ADDRSTRLEN);
    address = astring;
  } else {
    sockaddr_in6 *hipv6 = (sockaddr_in6 *)servinfo->ai_addr;
    char astring[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &(hipv6->sin6_addr), astring, INET6_ADDRSTRLEN);
    address = astring;
  }

  freeaddrinfo(servinfo);
  return address;
}

};  // namespace utils
