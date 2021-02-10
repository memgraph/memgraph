#include "io/network/utils.hpp"

#include <arpa/inet.h>
#include <netdb.h>

#include <climits>
#include <cstdlib>
#include <cstring>
#include <string>

#include "io/network/socket.hpp"

#include "utils/logging.hpp"

namespace io::network {

/// Resolves hostname to ip, if already an ip, just returns it
std::string ResolveHostname(std::string hostname) {
  addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;  // use AF_INET6 to force IPv6
  hints.ai_socktype = SOCK_STREAM;

  int addr_result;
  addrinfo *servinfo;
  MG_ASSERT((addr_result =
                 getaddrinfo(hostname.c_str(), NULL, &hints, &servinfo)) == 0,
            "Error with getaddrinfo: {}", gai_strerror(addr_result));
  MG_ASSERT(servinfo, "Could not resolve address: {}", hostname);

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

/// Gets hostname
std::optional<std::string> GetHostname() {
  char hostname[HOST_NAME_MAX + 1];
  int result = gethostname(hostname, sizeof(hostname));
  if (result) return std::nullopt;
  return std::string(hostname);
}

bool CanEstablishConnection(const io::network::Endpoint &endpoint) {
  io::network::Socket client;
  return client.Connect(endpoint);
}

};  // namespace io::network
