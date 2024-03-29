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

#include <array>
#include <cstring>
#include <filesystem>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

namespace memgraph::query::procedure {

class CallableAliasMapper final {
 public:
  CallableAliasMapper() = default;
  CallableAliasMapper(const CallableAliasMapper &) = delete;
  CallableAliasMapper &operator=(const CallableAliasMapper &) = delete;
  CallableAliasMapper(CallableAliasMapper &&) = delete;
  CallableAliasMapper &operator=(CallableAliasMapper &&) = delete;
  ~CallableAliasMapper() = default;

  void LoadMapping(const std::filesystem::path &);
  [[nodiscard]] std::optional<std::string_view> FindAlias(const std::string &) const noexcept;

 private:
  std::unordered_map<std::string, std::string> mapping_;
};

/// Single, global alias mapper.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
extern CallableAliasMapper gCallableAliasMapper;

}  // namespace memgraph::query::procedure
