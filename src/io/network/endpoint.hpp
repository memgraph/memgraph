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
#include <iosfwd>
#include <optional>
#include <string>

#include "json/json.hpp"

namespace memgraph::io::network {

struct Endpoint {
  static const struct needs_resolving_t {
  } needs_resolving;

  Endpoint() = default;
  Endpoint(std::string ip_address, uint16_t port);
  Endpoint(needs_resolving_t, std::string hostname, uint16_t port);

  Endpoint(Endpoint const &) = default;
  Endpoint(Endpoint &&) noexcept = default;

  Endpoint &operator=(Endpoint const &) = default;
  Endpoint &operator=(Endpoint &&) noexcept = default;

  ~Endpoint() = default;

  enum class IpFamily : std::uint8_t { NONE, IP4, IP6 };

  static std::optional<Endpoint> ParseSocketOrAddress(std::string_view address,
                                                      std::optional<uint16_t> default_port = {});

  std::string SocketAddress() const;

  std::string address;
  uint16_t port{0};
  IpFamily family{IpFamily::NONE};

  bool operator==(const Endpoint &other) const = default;
  friend std::ostream &operator<<(std::ostream &os, const Endpoint &endpoint);

 private:
  static IpFamily GetIpFamily(std::string_view address);

  static bool IsResolvableAddress(std::string_view address, uint16_t port);

  static auto ValidatePort(std::optional<uint16_t> port) -> bool;
};

void to_json(nlohmann::json &j, Endpoint const &config);
void from_json(nlohmann::json const &j, Endpoint &config);

}  // namespace memgraph::io::network
