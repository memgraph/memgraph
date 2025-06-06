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

#pragma once

#include <uuid/uuid.h>

#include "fmt/format.h"

#include <array>
// NOLINTNEXTLINE
#include <nlohmann/json_fwd.hpp>
#include <string>

namespace memgraph::utils {
struct UUID;
}  // namespace memgraph::utils

namespace memgraph::slk {
class Reader;
class Builder;
void Save(const ::memgraph::utils::UUID &self, Builder *builder);
void Load(::memgraph::utils::UUID *self, Reader *reader);
}  // namespace memgraph::slk

namespace memgraph::utils {

/**
 * This function generates an UUID and returns it.
 */
std::string GenerateUUID();

struct UUID {
  using arr_t = std::array<unsigned char, 16>;

  UUID() { uuid_generate(uuid.data()); }
  explicit operator std::string() const {
    // Note not using UUID_STR_LEN so we can build with older libuuid
    auto decoded = std::array<char, 37 /*UUID_STR_LEN*/>{};
    uuid_unparse(uuid.data(), decoded.data());
    return std::string{decoded.data(), 37 /*UUID_STR_LEN*/ - 1};
  }

  void set(std::string_view uuid_str) {
    if (uuid_str.length() != 36) {
      throw std::invalid_argument(
          fmt::format("Invalid UUID argument length. Length is {} and expected to be 36.", uuid_str.length()));
    }
    if (uuid_parse(uuid_str.data(), uuid.data()) != 0) {
      throw std::invalid_argument("Invalid UUID formatwhen setting new uuid string.");
    }
  }

  explicit operator arr_t() const { return uuid; }

  friend bool operator==(UUID const &, UUID const &) = default;
  friend bool operator==(UUID const &lhs, std::string_view rhs) { return std::string(lhs) == rhs; };

 private:
  friend void to_json(nlohmann::json &j, const UUID &uuid);
  friend void from_json(const nlohmann::json &j, UUID &uuid);
  friend void ::memgraph::slk::Load(UUID *self, slk::Reader *reader);
  explicit UUID(arr_t const &arr) : uuid(arr) {}

  arr_t uuid;
};

}  // namespace memgraph::utils
