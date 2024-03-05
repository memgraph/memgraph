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

namespace {
constexpr std::string_view delimiter = ":";

}  // namespace

namespace memgraph::io::network {

Endpoint::Endpoint(std::string ip_address, uint16_t port) : address(std::move(ip_address)), port(port) {
  IpFamily ip_family = GetIpFamily(address);
  if (ip_family == IpFamily::NONE) {
    throw NetworkError("Not a valid IPv4 or IPv6 address: {}", ip_address);
  }
  family = ip_family;
}

// NOLINTNEXTLINE
Endpoint::Endpoint(needs_resolving_t, std::string hostname, uint16_t port)
    : address(std::move(hostname)), port(port), family{GetIpFamily(address)} {}

Endpoint::IpFamily Endpoint::GetIpFamily(std::string_view address) {
  in_addr addr4;
  in6_addr addr6;
  int ipv4_result = inet_pton(AF_INET, address.data(), &addr4);
  int ipv6_result = inet_pton(AF_INET6, address.data(), &addr6);
  if (ipv4_result == 1) {
    return IpFamily::IP4;
  }
  if (ipv6_result == 1) {
    return IpFamily::IP6;
  }
  return IpFamily::NONE;
}

std::optional<std::pair<std::string_view, uint16_t>> Endpoint::ParseSocketOrIpAddress(
    std::string_view address, const std::optional<uint16_t> default_port) {
  /// expected address format:
  ///   - "ip_address:port_number"
  ///   - "ip_address"
  /// We parse the address first. If it's an IP address, a default port must
  // be given, or we return nullopt. If it's a socket address, we try to parse
  // it into an ip address and a port number; even if a default port is given,
  // it won't be used, as we expect that it is given in the address string.

  auto parts = utils::SplitView(address, delimiter);

  if (parts.size() == 1 && default_port) {
    if (GetIpFamily(address) == IpFamily::NONE) {
      return std::nullopt;
    }
    return std::pair{address, *default_port};
  }

  if (parts.size() == 2) {
    std::string_view ip_address = parts[0];
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

std::optional<std::pair<std::string_view, uint16_t>> Endpoint::ParseHostname(
    std::string_view address, const std::optional<uint16_t> default_port) {
  auto parts = utils::SplitView(address, delimiter);

  if (parts.size() == 1 && default_port) {
    if (!IsResolvableAddress(address, *default_port)) {
      return std::nullopt;
    }
    return std::pair{address, *default_port};
  }

  if (parts.size() == 2) {
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

    auto hostname = parts[0];
    if (IsResolvableAddress(hostname, static_cast<uint16_t>(int_port))) {
      return std::pair{hostname, static_cast<u_int16_t>(int_port)};
    }
  }

  return std::nullopt;
}

auto Endpoint::SocketAddress() const -> std::string { return fmt::format("{}:{}", address, port); }

std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint) {
  // no need to cover the IpFamily::NONE case, as you can't even construct an
  // Endpoint object if the IpFamily is NONE (i.e. the IP address is invalid)
  // unless you use DNS hostname
  if (endpoint.family == Endpoint::IpFamily::IP6) {
    return os << "[" << endpoint.address << "]"
              << ":" << endpoint.port;
  }
  return os << endpoint.address << ":" << endpoint.port;
}

bool Endpoint::IsResolvableAddress(std::string_view address, uint16_t port) {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  addrinfo *info = nullptr;
  auto status = getaddrinfo(address.data(), std::to_string(port).c_str(), &hints, &info);
  if (info) freeaddrinfo(info);
  return status == 0;
}

std::optional<std::pair<std::string_view, uint16_t>> Endpoint::ParseSocketOrAddress(
    std::string_view address, const std::optional<uint16_t> default_port) {
  auto parts = utils::SplitView(address, delimiter);

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

}  // namespace memgraph::io::network
