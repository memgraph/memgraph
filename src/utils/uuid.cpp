// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/uuid.hpp"

#include <array>
#include <cstddef>
#include <nlohmann/json.hpp>

namespace memgraph::utils {

void to_json(nlohmann::json &j, const UUID &uuid) { j = nlohmann::json(uuid.uuid); }

void from_json(const nlohmann::json &j, UUID &uuid) {
  auto arr = UUID::arr_t{};
  j.get_to(arr);
  uuid = UUID(arr);
}

std::string GenerateUUID() {
  uuid_t uuid;
  constexpr size_t kUuidStringLength = 36;          // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
  std::array<char, kUuidStringLength + 1> decoded;  // +1 for null terminator written by uuid_unparse
  uuid_generate(uuid);
  uuid_unparse(uuid, decoded.data());
  return {decoded.data(), kUuidStringLength};
}

}  // namespace memgraph::utils
