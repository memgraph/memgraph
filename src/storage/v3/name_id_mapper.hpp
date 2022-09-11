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

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include "utils/logging.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage::v3 {

class NameIdMapper final {
 public:
  NameIdMapper() = default;

  explicit NameIdMapper(std::unordered_map<uint64_t, std::string> id_to_name) : id_to_name_{std::move(id_to_name)} {}

  void StoreMapping(std::unordered_map<uint64_t, std::string> id_to_name) { id_to_name_ = std::move(id_to_name); }

  std::optional<uint64_t> NameToId(std::string_view name) const {
    auto it =
        std::ranges::find_if(id_to_name_, [name](const auto &name_id_pair) { return name_id_pair.second == name; });
    if (it == id_to_name_.end()) {
      return std::nullopt;
    }
    return it->first;
  }

  const std::string &IdToName(const uint64_t id) const {
    auto it = std::ranges::find_if(id_to_name_, [id](const auto &name_id_pair) { return name_id_pair.first == id; });
    MG_ASSERT(it != id_to_name_.end(), "Id not know in mapper!");
    return it->second;
  }

 private:
  std::unordered_map<uint64_t, std::string> id_to_name_;
};
}  // namespace memgraph::storage::v3
