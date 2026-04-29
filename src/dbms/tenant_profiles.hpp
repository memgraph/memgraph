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

#ifdef MG_ENTERPRISE

#include <cstdint>
#include <expected>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <nlohmann/json_fwd.hpp>

#include "kvstore/kvstore.hpp"

namespace memgraph::dbms {

class TenantProfiles {
 public:
  static constexpr std::string_view kPrefix = "tenant_profile:";
  static constexpr std::string_view kDbMappingPrefix = "db_tenant_profile:";
  static constexpr std::string_view kVersionKey = "tenant_profiles_version";
  static constexpr std::string_view kVersion = "V1";

  struct Profile {
    std::string name;
    int64_t memory_limit{0};  // bytes, 0 = unlimited
    std::unordered_set<std::string> databases;
  };

  explicit TenantProfiles(kvstore::KVStore &durability);

  enum class CreateError : uint8_t { ALREADY_EXISTS, DURABILITY_ERROR };
  enum class AlterError : uint8_t { NOT_FOUND, DURABILITY_ERROR };
  enum class DropError : uint8_t { NOT_FOUND, HAS_ATTACHED_DATABASES, DURABILITY_ERROR };
  enum class AttachError : uint8_t { PROFILE_NOT_FOUND, DURABILITY_ERROR };
  enum class DetachError : uint8_t { NOT_ATTACHED, DURABILITY_ERROR };
  enum class RenameError : uint8_t { NOT_ATTACHED, DURABILITY_ERROR };

  std::expected<void, CreateError> Create(std::string_view name, int64_t memory_limit);
  std::expected<std::unordered_set<std::string>, AlterError> Alter(std::string_view name, int64_t memory_limit);
  std::expected<void, DropError> Drop(std::string_view name);

  std::optional<Profile> Get(std::string_view name) const;
  std::vector<Profile> GetAll() const;

  std::expected<int64_t, AttachError> AttachToDatabase(std::string_view profile_name, std::string_view db_name);
  std::expected<void, DetachError> DetachFromDatabase(std::string_view db_name);
  std::expected<void, RenameError> RenameDatabase(std::string_view old_name, std::string_view new_name);
  std::optional<std::string> GetProfileForDatabase(std::string_view db_name) const;

 private:
  static nlohmann::json ProfileToJson(const Profile &profile);

  mutable std::shared_mutex mutex_;
  kvstore::KVStore *durability_;
};

}  // namespace memgraph::dbms

#endif
