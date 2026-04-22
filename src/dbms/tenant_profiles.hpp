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

#pragma once

#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "kvstore/kvstore.hpp"

namespace memgraph::dbms {

class TenantProfiles {
 public:
  static constexpr std::string_view kPrefix = "tenant_profile:";
  static constexpr std::string_view kVersionKey = "tenant_profiles_version";
  static constexpr std::string_view kVersion = "V1";

  struct Profile {
    std::string name;
    int64_t memory_limit{0};  // bytes, 0 = unlimited
    std::unordered_set<std::string> databases;
  };

  explicit TenantProfiles(kvstore::KVStore &durability);

  enum class DropResult : uint8_t { SUCCESS, NOT_FOUND, HAS_ATTACHED_DATABASES, DURABILITY_ERROR };

  bool Create(std::string_view name, int64_t memory_limit);
  std::optional<std::unordered_set<std::string>> Alter(std::string_view name, int64_t memory_limit);
  DropResult Drop(std::string_view name);

  std::optional<Profile> Get(std::string_view name) const;
  std::vector<Profile> GetAll() const;

  std::optional<int64_t> AttachToDatabase(std::string_view profile_name, std::string_view db_name);
  bool DetachFromDatabase(std::string_view db_name);
  bool RenameDatabase(std::string_view old_name, std::string_view new_name);
  std::optional<std::string> GetProfileForDatabase(std::string_view db_name) const;

 private:
  static std::string ProfileToJson(const Profile &profile);

  mutable std::shared_mutex mutex_;
  kvstore::KVStore *durability_;
  std::unordered_map<std::string, Profile> profiles_;
  std::unordered_map<std::string, std::string> db_to_profile_;  // db_name → profile_name
};

}  // namespace memgraph::dbms
