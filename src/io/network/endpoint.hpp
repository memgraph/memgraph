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

#pragma once

#include <netinet/in.h>
#include <cstdint>
#include <iostream>
#include <optional>
#include <string>

#include "json/json.hpp"

namespace memgraph::io::network {

class Endpoint {
 public:
  Endpoint() = default;
  Endpoint(std::string const &ip_address, uint16_t port);

  Endpoint(Endpoint const &) = default;
  Endpoint(Endpoint &&) noexcept = default;

  Endpoint &operator=(Endpoint const &) = default;
  Endpoint &operator=(Endpoint &&) noexcept = default;

  ~Endpoint() = default;

  enum class IpFamily : std::uint8_t { NONE, IP4, IP6 };

  static std::optional<Endpoint> ParseAndCreateSocketOrAddress(std::string_view address,
                                                               std::optional<uint16_t> default_port = {});

  [[nodiscard]] auto GetAddress() const -> std::string const &;
  [[nodiscard]] auto GetPort() const -> uint16_t const &;
  [[nodiscard]] auto GetIpFamily() const -> IpFamily const &;
  [[nodiscard]] auto GetAddress() -> std::string &;
  [[nodiscard]] auto GetPort() -> uint16_t &;
  [[nodiscard]] auto GetIpFamily() -> IpFamily &;

  [[nodiscard]] std::string SocketAddress() const;

  bool operator==(const Endpoint &other) const = default;
  friend std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint);

 private:
  static std::optional<std::tuple<std::string, uint16_t, Endpoint::IpFamily>> TryResolveAddress(
      std::string_view address, uint16_t port);

  static auto ValidatePort(std::optional<uint16_t> port) -> bool;

  std::string address_;
  uint16_t port_{0};
  IpFamily ip_family_{IpFamily::NONE};
};

void to_json(nlohmann::json &j, Endpoint const &config);
void from_json(nlohmann::json const &j, Endpoint &config);

}  // namespace memgraph::io::network
