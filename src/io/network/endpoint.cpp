// Copyright 2022 Memgraph Ltd.
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

#include "io/network/endpoint.hpp"
#include "io/network/network_error.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/string.hpp"

namespace memgraph::io::network {

Endpoint::IpFamily Endpoint::GetIpFamily(const std::string &ip_address) {
  in_addr addr4;
  in6_addr addr6;
  int ipv4_result = inet_pton(AF_INET, ip_address.c_str(), &addr4);
  int ipv6_result = inet_pton(AF_INET6, ip_address.c_str(), &addr6);
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
      spdlog::error(utils::MessageWithLink("Invalid port number. The port number exceeds the maximum possible size.",
                                           "https://memgr.ph/ports"));
      return std::nullopt;
    }

    return std::pair{ip_address, static_cast<uint16_t>(int_port)};
  }

  return std::nullopt;
}

std::string Endpoint::SocketAddress() const {
  auto ip_address = address.empty() ? "EMPTY" : address;
  return ip_address + ":" + std::to_string(port);
}

Endpoint::Endpoint() {}
Endpoint::Endpoint(std::string ip_address, uint16_t port) : address(std::move(ip_address)), port(port) {
  IpFamily ip_family = GetIpFamily(address);
  if (ip_family == IpFamily::NONE) {
    throw NetworkError("Not a valid IPv4 or IPv6 address: {}", ip_address);
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

}  // namespace memgraph::io::network
