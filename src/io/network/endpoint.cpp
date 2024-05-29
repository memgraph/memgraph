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

#include "io/network/endpoint.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>

#include "io/network/network_error.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/string.hpp"

#include <arpa/inet.h>
#include <fmt/core.h>
#include <netdb.h>
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <json/json.hpp>

namespace {
constexpr std::string_view delimiter = ":";
}  // namespace

namespace memgraph::io::network {

Endpoint::Endpoint(std::string address, uint16_t port) : address_(std::move(address)), port_(port) {}

std::string Endpoint::SocketAddress() const { return fmt::format("{}{}{}", address_, delimiter, port_); }

std::string Endpoint::GetResolvedSocketAddress() const {
  auto const result = TryResolveAddress(address_, port_);
  if (!result.has_value()) {
    throw NetworkError("Couldn't resolve neither using DNS, nor IP address!");
  }
  return fmt::format("{}{}{}", std::get<0>(*result), delimiter, std::get<1>(*result));
}

std::string Endpoint::GetResolvedIPAddress() const {
  auto const result = TryResolveAddress(address_, port_);
  if (!result.has_value()) {
    throw NetworkError("Couldn't resolve neither using DNS, nor IP address!");
  }
  return std::get<0>(*result);
}

std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint) {
  return os << endpoint.address_ << delimiter << endpoint.port_;
}

std::optional<std::tuple<std::string, uint16_t, Endpoint::IpFamily>> Endpoint::TryResolveAddress(
    std::string_view address, uint16_t port) {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,     // fill with IPv4 or IPv6
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  addrinfo *info = nullptr;
  utils::OnScopeExit const free_info{[&info]() {
    if (info) {
      freeaddrinfo(info);
    }
  }};
  auto status = getaddrinfo(std::string(address).c_str(), std::to_string(port).c_str(), &hints, &info);
  if (status != 0) {
    throw NetworkError("Couldn't resolve address: {}", address);
  }

  for (auto *socket_addr = info; socket_addr != nullptr; socket_addr = socket_addr->ai_next) {
    switch (socket_addr->ai_family) {
      case AF_UNSPEC:
        break;
      case AF_INET: {
        char buffer[INET_ADDRSTRLEN];
        auto *socket_address_ipv4 = reinterpret_cast<struct sockaddr_in *>(socket_addr->ai_addr);
        inet_ntop(socket_addr->ai_family, &(socket_address_ipv4->sin_addr), buffer, sizeof(buffer));
        return std::tuple{std::string{buffer}, port, Endpoint::IpFamily::IP4};
      }
      case AF_INET6: {
        char buffer[INET6_ADDRSTRLEN];
        auto *socket_address_ipv6 = reinterpret_cast<sockaddr_in6 *>(socket_addr->ai_addr);
        inet_ntop(socket_addr->ai_family, &(socket_address_ipv6->sin6_addr), buffer, sizeof(buffer));
        return std::tuple{std::string{buffer}, port, Endpoint::IpFamily::IP6};
      }
      default:
        throw std::runtime_error("Can't parse");
    }
  }
  return std::nullopt;
}

std::optional<Endpoint> Endpoint::ParseAndCreateSocketOrAddress(std::string_view address,
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

  return Endpoint{std::string(addr), *port};  // NOLINT
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

[[nodiscard]] auto Endpoint::GetAddress() const -> std::string const & { return address_; }
[[nodiscard]] auto Endpoint::GetAddress() -> std::string & { return address_; }

[[nodiscard]] auto Endpoint::GetPort() const -> uint16_t const & { return port_; }
[[nodiscard]] auto Endpoint::GetPort() -> uint16_t & { return port_; }

[[nodiscard]] auto Endpoint::GetIpFamily() const -> IpFamily {
  auto const result = TryResolveAddress(address_, port_);
  if (!result.has_value()) {
    throw NetworkError("Couldn't resolve neither using DNS, nor IP address!");
  }
  return std::get<2>(*result);
}

void Endpoint::SetAddress(std::string address) { address_ = std::move(address); }
void Endpoint::SetPort(uint16_t port) { port_ = port; }

void to_json(nlohmann::json &j, Endpoint const &config) {
  j = nlohmann::json{{"address", config.GetAddress()}, {"port", config.GetPort()}};
}

void from_json(nlohmann::json const &j, Endpoint &config) {
  config.SetAddress(j.at("address").get<std::string>());
  config.SetPort(j.at("port").get<uint16_t>());
}

}  // namespace memgraph::io::network
