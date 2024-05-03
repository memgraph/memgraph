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

// NOLINTNEXTLINE
Endpoint::Endpoint(needs_resolving_t, std::string hostname, uint16_t port)
    : address(std::move(hostname)), port(port), family{GetIpFamily(address)} {}

Endpoint::Endpoint(std::string ip_address, uint16_t port) : address(std::move(ip_address)), port(port) {
  IpFamily ip_family = GetIpFamily(address);
  if (ip_family == IpFamily::NONE) {
    throw NetworkError("Not a valid IPv4 or IPv6 address: {}", ip_address);
  }
  family = ip_family;
}

Endpoint::Endpoint(real_manual_resolving_t, std::string const &ip_address, uint16_t port) {
  auto result = TryResolveAddress(ip_address, port);
  if (!result.has_value()) {
    throw NetworkError("Couldn't resolve neither DNS, nor IP address");
  }
  // TODO add _ to name
  address = std::get<0>(*result);
  this->port = std::get<1>(*result);
  family = std::get<2>(*result);
  spdlog::trace("Resolving name: {} {}", address, this->port);
}

std::string Endpoint::SocketAddress() const { return fmt::format("{}:{}", address, port); }

Endpoint::IpFamily Endpoint::GetIpFamily(std::string_view address) {
  // Ensure null-terminated
  auto const tmp = std::string(address);
  in_addr addr4;
  in6_addr addr6;
  int ipv4_result = inet_pton(AF_INET, tmp.c_str(), &addr4);
  int ipv6_result = inet_pton(AF_INET6, tmp.c_str(), &addr6);
  if (ipv4_result == 1) {
    return IpFamily::IP4;
  }
  if (ipv6_result == 1) {
    return IpFamily::IP6;
  }
  return IpFamily::NONE;
}

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

// NOTE: Intentional copy to ensure null-terminated string
bool Endpoint::IsResolvableAddress(std::string_view address, uint16_t port) {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  addrinfo *info = nullptr;
  auto status = getaddrinfo(std::string(address).c_str(), std::to_string(port).c_str(), &hints, &info);
  if (info) freeaddrinfo(info);
  return status == 0;
}

std::optional<std::tuple<std::string, uint16_t, Endpoint::IpFamily>> Endpoint::TryResolveAddress(
    std::string_view address, uint16_t port) {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  addrinfo *info = nullptr;
  auto status = getaddrinfo(std::string(address).c_str(), std::to_string(port).c_str(), &hints, &info);
  if (status != 0) {
    throw NetworkError("Couldn't resolve address: {}", address);
  }

  char buffer[INET6_ADDRSTRLEN];
  for (auto socket_addr = info; socket_addr != nullptr; socket_addr = socket_addr->ai_next) {
    void *addr{nullptr};
    switch (socket_addr->ai_family) {
      case AF_UNSPEC:
        break;
      case AF_INET: {
        auto *socket_address_ipv4 = (struct sockaddr_in *)socket_addr->ai_addr;
        addr = &(socket_address_ipv4->sin_addr);
        inet_ntop(socket_addr->ai_family, addr, buffer, sizeof(buffer));
        return std::tuple{buffer, port, Endpoint::IpFamily::IP4};
      }
      case AF_INET6: {
        auto *socket_address_ipv6 = (struct sockaddr_in6 *)socket_addr->ai_addr;
        addr = &(socket_address_ipv6->sin6_addr);
        inet_ntop(socket_addr->ai_family, addr, buffer, sizeof(buffer));
        return std::tuple{buffer, port, Endpoint::IpFamily::IP6};
      }
      default:
        throw std::runtime_error("Can't parse");
    }

    if (nullptr == addr) {
      continue;
    }
  }
  if (info) freeaddrinfo(info);
  return std::nullopt;
}

std::optional<Endpoint> Endpoint::ParseSocketOrAddress(std::string_view address, std::optional<uint16_t> default_port) {
  auto const parts = utils::SplitView(address, delimiter);

  if (parts.size() > 2) {
    return std::nullopt;
  }

  auto const port = [default_port, &parts]() -> std::optional<uint16_t> {
    if (parts.size() == 2) {
      return static_cast<uint16_t>(utils::ParseInt(parts[1]));
    }
    return default_port;
  }();

  if (!ValidatePort(port)) {
    return std::nullopt;
  }

  auto const addr = [address, &parts]() {
    if (parts.size() == 2) {
      return parts[0];
    }
    return address;
  }();

  if (GetIpFamily(addr) == IpFamily::NONE) {
    if (IsResolvableAddress(addr, *port)) {       // NOLINT
      return Endpoint{std::string(addr), *port};  // NOLINT
    }
    return std::nullopt;
  }

  return Endpoint{std::string(addr), *port};  // NOLINT
}

std::optional<Endpoint> Endpoint::ParseSocketOrAddressManual(std::string_view address,
                                                             std::optional<uint16_t> default_port) {
  auto const parts = utils::SplitView(address, delimiter);

  if (parts.size() > 2) {
    return std::nullopt;
  }

  auto const port = [default_port, &parts]() -> std::optional<uint16_t> {
    if (parts.size() == 2) {
      return static_cast<uint16_t>(utils::ParseInt(parts[1]));
    }
    return default_port;
  }();

  if (!ValidatePort(port)) {
    return std::nullopt;
  }

  auto const addr = [address, &parts]() {
    if (parts.size() == 2) {
      return parts[0];
    }
    return address;
  }();

  return Endpoint{real_manual_resolving, std::string(addr), *port};  // NOLINT
}

auto Endpoint::ValidatePort(std::optional<uint16_t> port) -> bool {
  if (!port) {
    return false;
  }

  if (port < 0) {
    spdlog::error(utils::MessageWithLink("Invalid port number {}. The port number must be a positive integer.", *port,
                                         "https://memgr.ph/ports"));
    return false;
  }

  if (port > std::numeric_limits<uint16_t>::max()) {
    spdlog::error(utils::MessageWithLink("Invalid port number. The port number exceedes the maximum possible size.",
                                         "https://memgr.ph/ports"));
    return false;
  }

  return true;
}

void to_json(nlohmann::json &j, Endpoint const &config) {
  j = nlohmann::json{{"address", config.address}, {"port", config.port}, {"family", config.family}};
}

void from_json(nlohmann::json const &j, Endpoint &config) {
  config.address = j.at("address").get<std::string>();
  config.port = j.at("port").get<uint16_t>();
  config.family = j.at("family").get<Endpoint::IpFamily>();
}

}  // namespace memgraph::io::network
