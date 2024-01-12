// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <algorithm>

#include "endpoint.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/network_error.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/string.hpp"

namespace memgraph::io::network {

Endpoint::IpFamily Endpoint::GetIpFamily(const std::string &address) {
  in_addr addr4;
  in6_addr addr6;
  int ipv4_result = inet_pton(AF_INET, address.c_str(), &addr4);
  int ipv6_result = inet_pton(AF_INET6, address.c_str(), &addr6);
  if (ipv4_result == 1) {
    return IpFamily::IP4;
  } else if (ipv6_result == 1) {
    return IpFamily::IP6;
  } else {
    return IpFamily::NONE;
  }
}

std::optional<std::pair<std::string, uint16_t>> Endpoint::ParseSocketOrIpAddress(
    const std::string &address, const std::optional<uint16_t> default_port = {}) {
  /// expected address format:
  ///   - "ip_address:port_number"
  ///   - "ip_address"
  /// We parse the address first. If it's an IP address, a default port must
  // be given, or we return nullopt. If it's a socket address, we try to parse
  // it into an ip address and a port number; even if a default port is given,
  // it won't be used, as we expect that it is given in the address string.
  const std::string delimiter = ":";
  std::string ip_address;

  std::vector<std::string> parts = utils::Split(address, delimiter);
  if (parts.size() == 1) {
    if (default_port) {
      if (GetIpFamily(address) == IpFamily::NONE) {
        return std::nullopt;
      }
      return std::pair{address, *default_port};
    }
  } else if (parts.size() == 2) {
    ip_address = std::move(parts[0]);
    if (GetIpFamily(ip_address) == IpFamily::NONE) {
      return std::nullopt;
    }
    int64_t int_port{0};
    try {
      int_port = utils::ParseInt(parts[1]);
    } catch (utils::BasicException &e) {
      spdlog::error(utils::MessageWithLink("Invalid port number {}.", parts[1], "https://memgr.ph/ports"));
      return std::nullopt;
    }
    if (int_port < 0) {
      spdlog::error(utils::MessageWithLink("Invalid port number {}. The port number must be a positive integer.",
                                           int_port, "https://memgr.ph/ports"));
      return std::nullopt;
    }
    if (int_port > std::numeric_limits<uint16_t>::max()) {
      spdlog::error(utils::MessageWithLink("Invalid port number. The port number exceedes the maximum possible size.",
                                           "https://memgr.ph/ports"));
      return std::nullopt;
    }

    return std::pair{ip_address, static_cast<uint16_t>(int_port)};
  }

  return std::nullopt;
}

std::optional<std::pair<std::string, uint16_t>> Endpoint::ParseHostname(
    const std::string &address, const std::optional<uint16_t> default_port = {}) {
  const std::string delimiter = ":";
  std::string ip_address;
  std::vector<std::string> parts = utils::Split(address, delimiter);
  if (parts.size() == 1) {
    if (default_port) {
      if (!IsResolvableAddress(address, *default_port)) {
        return std::nullopt;
      }
      return std::pair{address, *default_port};
    }
  } else if (parts.size() == 2) {
    int64_t int_port{0};
    auto hostname = std::move(parts[0]);
    try {
      int_port = utils::ParseInt(parts[1]);
    } catch (utils::BasicException &e) {
      spdlog::error(utils::MessageWithLink("Invalid port number {}.", parts[1], "https://memgr.ph/ports"));
      return std::nullopt;
    }
    if (int_port < 0) {
      spdlog::error(utils::MessageWithLink("Invalid port number {}. The port number must be a positive integer.",
                                           int_port, "https://memgr.ph/ports"));
      return std::nullopt;
    }
    if (int_port > std::numeric_limits<uint16_t>::max()) {
      spdlog::error(utils::MessageWithLink("Invalid port number. The port number exceedes the maximum possible size.",
                                           "https://memgr.ph/ports"));
      return std::nullopt;
    }
    if (IsResolvableAddress(hostname, static_cast<uint16_t>(int_port))) {
      return std::pair{hostname, static_cast<u_int16_t>(int_port)};
    }
  }
  return std::nullopt;
}

std::string Endpoint::SocketAddress() const {
  auto ip_address = address.empty() ? "EMPTY" : address;
  return ip_address + ":" + std::to_string(port);
}

Endpoint::Endpoint(std::string ip_address, uint16_t port) : address(std::move(ip_address)), port(port) {
  IpFamily ip_family = GetIpFamily(address);
  if (ip_family == IpFamily::NONE) {
    throw NetworkError("Not a valid IPv4 or IPv6 address: {}", ip_address);
  }
  family = ip_family;
}

// NOLINTNEXTLINE
Endpoint::Endpoint(needs_resolving_t, std::string hostname, uint16_t port) : port(port) {
  address = ResolveHostnameIntoIpAddress(hostname, port);
  IpFamily ip_family = GetIpFamily(address);
  if (ip_family == IpFamily::NONE) {
    throw NetworkError("Not a valid IPv4 or IPv6 address: {}", address);
  }
  family = ip_family;
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

bool Endpoint::IsResolvableAddress(const std::string &address, uint16_t port) {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  addrinfo *info = nullptr;
  auto status = getaddrinfo(address.c_str(), std::to_string(port).c_str(), &hints, &info);
  freeaddrinfo(info);
  return status == 0;
}

std::optional<std::pair<std::string, uint16_t>> Endpoint::ParseSocketOrAddress(
    const std::string &address, const std::optional<uint16_t> default_port) {
  const std::string delimiter = ":";
  std::vector<std::string> parts = utils::Split(address, delimiter);
  if (parts.size() == 1) {
    if (GetIpFamily(address) == IpFamily::NONE) {
      return ParseHostname(address, default_port);
    }
    return ParseSocketOrIpAddress(address, default_port);
  }
  if (parts.size() == 2) {
    if (GetIpFamily(parts[0]) == IpFamily::NONE) {
      return ParseHostname(address, default_port);
    }
    return ParseSocketOrIpAddress(address, default_port);
  }
  return std::nullopt;
}

std::string Endpoint::ResolveHostnameIntoIpAddress(const std::string &address, uint16_t port) {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  addrinfo *info = nullptr;
  auto status = getaddrinfo(address.c_str(), std::to_string(port).c_str(), &hints, &info);
  if (status != 0) throw NetworkError(gai_strerror(status));

  for (auto *result = info; result != nullptr; result = result->ai_next) {
    if (result->ai_family == AF_INET) {
      char ipstr[INET_ADDRSTRLEN];
      auto *ipv4 = reinterpret_cast<struct sockaddr_in *>(result->ai_addr);
      inet_ntop(AF_INET, &(ipv4->sin_addr), ipstr, sizeof(ipstr));
      freeaddrinfo(info);
      return ipstr;
    }
    if (result->ai_family == AF_INET6) {
      char ipstr[INET6_ADDRSTRLEN];
      auto *ipv6 = reinterpret_cast<struct sockaddr_in6 *>(result->ai_addr);
      inet_ntop(AF_INET6, &(ipv6->sin6_addr), ipstr, sizeof(ipstr));
      freeaddrinfo(info);
      return ipstr;
    }
  }
  freeaddrinfo(info);
  throw NetworkError("Not a valid address: {}", address);
}

}  // namespace memgraph::io::network
