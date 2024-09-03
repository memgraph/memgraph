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
  return os << endpoint.GetResolvedSocketAddress();
}

std::optional<std::tuple<std::string, uint16_t, Endpoint::IpFamily>> Endpoint::TryResolveAddress(
    std::string_view address, uint16_t port) {
  struct addrinfo hints;
  struct addrinfo *results{nullptr};
  char ipstr[INET6_ADDRSTRLEN];

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  std::string address_str = std::string(address);
  if (int status = getaddrinfo(address_str.c_str(), nullptr, &hints, &results) != 0) {
    spdlog::trace("Failed to resolve hostname {} with error status code {}", address_str, gai_strerror(status));
    return std::nullopt;
  }

  void *addr{nullptr};
  IpFamily family{IpFamily::IP4};

  if (results->ai_family == AF_INET) {
    auto *ipv4 = (struct sockaddr_in *)results->ai_addr;  // NOLINT
    addr = &(ipv4->sin_addr);
    family = IpFamily::IP4;
  } else {
    auto *ipv6 = (struct sockaddr_in6 *)results->ai_addr;  // NOLINT
    addr = &(ipv6->sin6_addr);
    family = IpFamily::IP6;
  }

  inet_ntop(results->ai_family, addr, ipstr, sizeof ipstr);
  freeaddrinfo(results);

  auto const ip = std::string(ipstr);
  std::string const version_str = family == IpFamily::IP4 ? "IPv4" : "IPv6";
  spdlog::trace("Resolved to ip {} ip version {}", ip, version_str);
  return std::make_tuple(ip, port, family);
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
