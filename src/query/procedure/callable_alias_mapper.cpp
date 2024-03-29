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

#include "callable_alias_mapper.hpp"

#include <algorithm>
#include <array>
#include <filesystem>
#include <fstream>

#include <spdlog/spdlog.h>
#include <json/json.hpp>

#include "utils/logging.hpp"

namespace memgraph::query::procedure {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
CallableAliasMapper gCallableAliasMapper;

void CallableAliasMapper::LoadMapping(const std::filesystem::path &path) {
  using json = nlohmann::json;
  if (path.empty()) {
    spdlog::info("Path to callable mappings was not set.");
    return;
  }

  if (std::filesystem::exists(path)) {
    const bool is_regular_file = std::filesystem::is_regular_file(path);
    const bool has_json_extension = (path.extension() == ".json");
    if (is_regular_file && has_json_extension) {
      std::ifstream mapping_file(path);
      try {
        json mapping_data = json::parse(mapping_file);
        mapping_ = mapping_data.get<std::unordered_map<std::string, std::string>>();
      } catch (...) {
        MG_ASSERT(false, "Parsing callable mapping was unsuccesful. Make sure it is in correct json format.");
      }
    } else {
      MG_ASSERT(false, "Path to callable mappings is not a regular file or does not have .json extension.");
    }
  } else {
    MG_ASSERT(false, "Path to callable mappings was set, but the path does not exist.");
  }
}

std::optional<std::string_view> CallableAliasMapper::FindAlias(const std::string &name) const noexcept {
  if (!mapping_.contains(name)) {
    return std::nullopt;
  }
  return mapping_.at(name);
}

}  // namespace memgraph::query::procedure
