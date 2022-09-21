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
  static constexpr uint64_t kUnmappedId{0};
  NameIdMapper() = default;

  explicit NameIdMapper(std::unordered_map<uint64_t, std::string> id_to_name) : id_to_name_{std::move(id_to_name)} {
    for (const auto &[id, name] : id_to_name_) {
      name_to_id_.emplace(name, id);
    }
  }

  void StoreMapping(std::unordered_map<uint64_t, std::string> id_to_name) {
    id_to_name_ = std::move(id_to_name);
    for (const auto &[id, name] : id_to_name_) {
      name_to_id_.emplace(name, id);
    }
  }

  uint64_t NameToId(std::string_view name) const {
    if (auto it = name_to_id_.find(name); it != name_to_id_.end()) {
      return it->second;
    }
    return kUnmappedId;
  }

  const std::string &IdToName(const uint64_t id) const {
    auto it = id_to_name_.find(id);
    MG_ASSERT(it != id_to_name_.end(), "Id not know in mapper!");
    return it->second;
  }

 private:
  // Necessary for comparison with string_view nad string
  // https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2018/p0919r1.html
  // https://www.cppstories.com/2021/heterogeneous-access-cpp20/
  struct StringHash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(const char *txt) const { return std::hash<std::string_view>{}(txt); }
    [[nodiscard]] size_t operator()(std::string_view txt) const { return std::hash<std::string_view>{}(txt); }
    [[nodiscard]] size_t operator()(const std::string &txt) const { return std::hash<std::string>{}(txt); }
  };
  std::unordered_map<uint64_t, std::string> id_to_name_;
  std::unordered_map<std::string, uint64_t, StringHash, std::equal_to<>> name_to_id_;
};
}  // namespace memgraph::storage::v3
