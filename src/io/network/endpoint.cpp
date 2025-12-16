// Copyright 2025 Memgraph Ltd.
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

#include <array>
#include <cstdint>
#include <limits>
#include <optional>
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
#include <nlohmann/json.hpp>

namespace {
constexpr std::string_view delimiter = ":";
}  // namespace

namespace memgraph::io::network {

Endpoint::Endpoint(std::string address, uint16_t port) : address_(std::move(address)), port_(port) {}

auto Endpoint::SocketAddress() const -> std::string { return fmt::format("{}{}{}", address_, delimiter, port_); }

auto Endpoint::GetResolvedSocketAddress() const -> std::string {
  auto const result = TryResolveAddress(address_, port_);
  if (!result) {
    throw NetworkError("Couldn't resolve {}:{}.", address_, port_);
  }

  auto resolved_address = std::get<0>(*result);
  auto resolved_port = std::get<1>(*result);

  return fmt::format("{}{}{}", resolved_address, delimiter, resolved_port);
}

auto Endpoint::GetResolvedIPAddress() const -> std::string {
  auto const result = TryResolveAddress(address_, port_);
  if (!result) {
    throw NetworkError("Couldn't resolve {}:{}.", address_, port_);
  }

  auto resolved_address = std::get<0>(*result);

  return resolved_address;
}

[[nodiscard]] auto Endpoint::GetIpFamily() const -> IpFamily {
  auto const result = TryResolveAddress(address_, port_);
  if (!result) {
    throw NetworkError("Couldn't resolve {}:{}.", address_, port_);
  }

  return std::get<2>(*result);
}

std::optional<Endpoint::RetValue> Endpoint::TryResolveAddress(std::string_view address, uint16_t port) {
  auto const process_ipv4_family = [address](addrinfo *socket_addr, uint16_t port) -> std::optional<RetValue> {
    std::array<char, INET_ADDRSTRLEN> buffer;  // intentionally uninitialized
    auto *socket_address_ipv4 = reinterpret_cast<struct sockaddr_in *>(socket_addr->ai_addr);
    auto const *res = inet_ntop(socket_addr->ai_family, &(socket_address_ipv4->sin_addr), buffer.data(), buffer.size());
    if (res == NULL) {    // NOLINT
      int errsv = errno;  // don't reorder, otherwise errno could get reassigned.
      spdlog::error("inet_ntop failed with errno {} when resolving {} to ipv4 address.", errsv, address);
      return std::nullopt;
    }
    return std::tuple{std::string{buffer.data()}, port, Endpoint::IpFamily::IP4};
  };

  auto const process_ipv6_family = [address](addrinfo *socket_addr, uint16_t port) -> std::optional<RetValue> {
    std::array<char, INET6_ADDRSTRLEN> buffer;  // intentionally uninitialized
    auto *socket_address_ipv6 = reinterpret_cast<sockaddr_in6 *>(socket_addr->ai_addr);
    auto const *res =
        inet_ntop(socket_addr->ai_family, &(socket_address_ipv6->sin6_addr), buffer.data(), buffer.size());
    if (res == NULL) {    // NOLINT
      int errsv = errno;  // don't reorder, otherwise errno could get reassigned.
      spdlog::error("inet_ntop failed with errno {} when resolving {} to ipv6 address.", errsv, address);
      return std::nullopt;
    }
    return std::tuple{std::string{buffer.data()}, port, Endpoint::IpFamily::IP6};
  };

  auto const parse_ip_family = [&address, &port](
                                   std::function<std::optional<RetValue>(addrinfo *, uint16_t)> const &processing_fn,
                                   auto family) -> std::optional<RetValue> {
    addrinfo const hints{
        .ai_flags = AI_PASSIVE,      // fill with IPv4 or IPv6
        .ai_family = family,         // IPv4 or IPv6
        .ai_socktype = SOCK_STREAM,  // TCP socket
        .ai_protocol = 0,            // any protocol
    };

    addrinfo *info{nullptr};
    utils::OnScopeExit const free_info{[&info]() {
      if (info) {
        freeaddrinfo(info);
      }
    }};

    auto status = getaddrinfo(std::string(address).c_str(), std::to_string(port).c_str(), &hints, &info);
    if (status != 0) {
      spdlog::error("getaddrinfo finished unsuccessfully while resolving {}:{}. Error occurred: {}", address, port,
                    gai_strerror(status));
      return std::nullopt;
    }

    auto *socket_addr = info;
    while (socket_addr != nullptr) {
      if (family != socket_addr->ai_family) {
        socket_addr = socket_addr->ai_next;
        continue;
      }
      return processing_fn(socket_addr, port);
    }

    return std::nullopt;
  };

  auto ip_v4_family = parse_ip_family(process_ipv4_family, AF_INET);
  if (ip_v4_family) {
    return std::move(*ip_v4_family);
  }
  return parse_ip_family(process_ipv6_family, AF_INET6);
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
