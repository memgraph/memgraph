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

#include "dbms/tenant_profiles.hpp"

#ifdef MG_ENTERPRISE

#include <iterator>
#include <map>
#include <set>

#include <nlohmann/json.hpp>

#include "spdlog/spdlog.h"

namespace memgraph::dbms {

namespace {

auto ProfileKey(std::string_view name) -> std::string {
  return std::string{TenantProfiles::kPrefix} + std::string{name};
}

auto DbMappingKey(std::string_view db) -> std::string {
  return std::string{TenantProfiles::kDbMappingPrefix} + std::string{db};
}

auto FromJson(const nlohmann::json &json, std::string_view name) -> TenantProfiles::Profile {
  TenantProfiles::Profile profile;
  profile.name = name;
  if (json.contains("memory_limit")) profile.memory_limit = json["memory_limit"].get<int64_t>();
  if (json.contains("databases")) profile.databases = json["databases"].get<std::unordered_set<std::string>>();
  return profile;
}

}  // namespace

nlohmann::json TenantProfiles::ProfileToJson(const Profile &profile) {
  nlohmann::json json;
  json["memory_limit"] = profile.memory_limit;
  json["databases"] = profile.databases;
  return json;
}

TenantProfiles::TenantProfiles(kvstore::KVStore &durability) : durability_{&durability} {
  auto existing_version = durability_->Get(kVersionKey);
  if (!existing_version || *existing_version != kVersion) {
    durability_->Put(kVersionKey, kVersion);
  }
}

std::expected<void, TenantProfiles::CreateError> TenantProfiles::Create(std::string_view name, int64_t memory_limit) {
  const std::unique_lock lock{mutex_};
  if (durability_->Get(ProfileKey(name))) return std::unexpected{CreateError::ALREADY_EXISTS};

  const Profile profile{.name = std::string{name}, .memory_limit = memory_limit};
  if (!durability_->Put(ProfileKey(profile.name), ProfileToJson(profile).dump())) {
    return std::unexpected{CreateError::DURABILITY_ERROR};
  }
  return {};
}

std::expected<std::unordered_set<std::string>, TenantProfiles::AlterError> TenantProfiles::Alter(std::string_view name,
                                                                                                 int64_t memory_limit) {
  const std::unique_lock lock{mutex_};
  auto stored = durability_->Get(ProfileKey(name));
  if (!stored) return std::unexpected{AlterError::NOT_FOUND};

  Profile profile = FromJson(nlohmann::json::parse(*stored), name);
  profile.memory_limit = memory_limit;
  if (!durability_->Put(ProfileKey(profile.name), ProfileToJson(profile).dump())) {
    return std::unexpected{AlterError::DURABILITY_ERROR};
  }
  return profile.databases;
}

std::expected<void, TenantProfiles::DropError> TenantProfiles::Drop(std::string_view name) {
  const std::unique_lock lock{mutex_};
  auto stored = durability_->Get(ProfileKey(name));
  if (!stored) return std::unexpected{DropError::NOT_FOUND};

  const Profile profile = FromJson(nlohmann::json::parse(*stored), name);
  if (!profile.databases.empty()) return std::unexpected{DropError::HAS_ATTACHED_DATABASES};

  if (!durability_->Delete(ProfileKey(name))) return std::unexpected{DropError::DURABILITY_ERROR};
  return {};
}

std::optional<TenantProfiles::Profile> TenantProfiles::Get(std::string_view name) const {
  const std::shared_lock lock{mutex_};
  auto stored = durability_->Get(ProfileKey(name));
  if (!stored) return std::nullopt;
  try {
    return FromJson(nlohmann::json::parse(*stored), name);
  } catch (const nlohmann::json::parse_error &e) {
    spdlog::warn("Failed to parse tenant profile '{}': {}", name, e.what());
    return std::nullopt;
  }
}

std::vector<TenantProfiles::Profile> TenantProfiles::GetAll() const {
  const std::shared_lock lock{mutex_};
  std::vector<Profile> result;
  for (auto it = durability_->begin(std::string{kPrefix}); it != durability_->end(std::string{kPrefix}); ++it) {
    const auto &[key, value] = *it;
    auto name = key.substr(kPrefix.size());
    try {
      result.push_back(FromJson(nlohmann::json::parse(value), name));
    } catch (const nlohmann::json::parse_error &e) {
      spdlog::warn("Failed to parse tenant profile '{}': {}", name, e.what());
    }
  }
  return result;
}

std::expected<int64_t, TenantProfiles::AttachError> TenantProfiles::AttachToDatabase(std::string_view profile_name,
                                                                                     std::string_view db_name) {
  const std::unique_lock lock{mutex_};
  auto new_stored = durability_->Get(ProfileKey(profile_name));
  if (!new_stored) return std::unexpected{AttachError::PROFILE_NOT_FOUND};
  Profile new_profile = FromJson(nlohmann::json::parse(*new_stored), profile_name);

  std::map<std::string, std::string> to_put;
  if (const auto old_profile_name = durability_->Get(DbMappingKey(db_name));
      old_profile_name && *old_profile_name != profile_name) {
    if (auto old_stored = durability_->Get(ProfileKey(*old_profile_name))) {
      Profile old_profile = FromJson(nlohmann::json::parse(*old_stored), *old_profile_name);
      old_profile.databases.erase(std::string{db_name});
      to_put.emplace(ProfileKey(old_profile.name), ProfileToJson(old_profile).dump());
    }
  }
  new_profile.databases.insert(std::string{db_name});
  to_put.emplace(ProfileKey(new_profile.name), ProfileToJson(new_profile).dump());
  to_put.emplace(DbMappingKey(db_name), profile_name);

  if (!durability_->PutMultiple(to_put)) return std::unexpected{AttachError::DURABILITY_ERROR};
  return new_profile.memory_limit;
}

std::expected<void, TenantProfiles::DetachError> TenantProfiles::DetachFromDatabase(
    std::string_view db_name, std::vector<std::string> extra_keys_to_delete) {
  const std::unique_lock lock{mutex_};
  auto profile_name = durability_->Get(DbMappingKey(db_name));
  if (!profile_name) return std::unexpected{DetachError::NOT_ATTACHED};

  auto profile_stored = durability_->Get(ProfileKey(*profile_name));
  if (!profile_stored) return std::unexpected{DetachError::DURABILITY_ERROR};

  std::map<std::string, std::string> to_put;
  try {
    Profile profile = FromJson(nlohmann::json::parse(*profile_stored), *profile_name);
    profile.databases.erase(std::string{db_name});
    to_put.emplace(ProfileKey(profile.name), ProfileToJson(profile).dump());
  } catch (const nlohmann::json::exception &e) {
    // Corrupt reads the same as unreadable to the caller, so reuse DURABILITY_ERROR; the mapping key is
    // left behind on purpose -- the next boot's PruneDatabases collects it once the database key is gone.
    spdlog::warn("Tenant profile '{}' durable entry is corrupt ({}); failed to detach database '{}'.",
                 *profile_name,
                 e.what(),
                 db_name);
    return std::unexpected{DetachError::DURABILITY_ERROR};
  }

  std::vector<std::string> to_delete{DbMappingKey(db_name)};
  to_delete.insert(to_delete.end(),
                   std::make_move_iterator(extra_keys_to_delete.begin()),
                   std::make_move_iterator(extra_keys_to_delete.end()));
  if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) return std::unexpected{DetachError::DURABILITY_ERROR};
  return {};
}

std::size_t TenantProfiles::PruneDatabases(const std::set<std::string> &live_db_names) {
  const std::unique_lock lock{mutex_};

  std::vector<std::string> to_delete;
  // Group stale db names by profile so a profile attached to two stale databases is read and rewritten
  // once: to_put is keyed by ProfileKey, so a duplicate emplace is dropped and one erase would be lost.
  std::map<std::string, std::set<std::string>> stale_by_profile;
  const auto mapping_end = durability_->end(std::string{kDbMappingPrefix});
  for (auto it = durability_->begin(std::string{kDbMappingPrefix}); it != mapping_end; ++it) {
    const auto &[key, profile_name] = *it;
    auto db_name = key.substr(kDbMappingPrefix.size());
    if (live_db_names.contains(db_name)) continue;
    to_delete.push_back(key);
    stale_by_profile[profile_name].insert(std::move(db_name));
  }
  if (to_delete.empty()) return 0;

  std::map<std::string, std::string> to_put;
  for (const auto &[profile_name, stale_dbs] : stale_by_profile) {
    auto stored = durability_->Get(ProfileKey(profile_name));
    if (!stored) continue;
    try {
      Profile profile = FromJson(nlohmann::json::parse(*stored), profile_name);
      for (const auto &db_name : stale_dbs) profile.databases.erase(db_name);
      to_put.emplace(ProfileKey(profile.name), ProfileToJson(profile).dump());
    } catch (const nlohmann::json::exception &e) {
      // The mapping key is deleted regardless -- its database already failed the live_db_names check,
      // so it is garbage either way. Catching only json::exception keeps std::bad_alloc propagating.
      spdlog::warn("Tenant profile '{}' durable entry is corrupt ({}); pruning its stale database mapping(s) anyway.",
                   profile_name,
                   e.what());
    }
  }

  if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) {
    spdlog::error(
        "Failed to durably prune {} stale tenant profile database attachment(s); they survive this boot and "
        "reconciliation will be retried on the next one.",
        to_delete.size());
    return 0;
  }
  return to_delete.size();
}

std::expected<void, TenantProfiles::RenameError> TenantProfiles::RenameDatabase(std::string_view old_name,
                                                                                std::string_view new_name) {
  const std::unique_lock lock{mutex_};
  auto profile_name = durability_->Get(DbMappingKey(old_name));
  if (!profile_name) return std::unexpected{RenameError::NOT_ATTACHED};

  auto profile_stored = durability_->Get(ProfileKey(*profile_name));
  if (!profile_stored) return std::unexpected{RenameError::DURABILITY_ERROR};

  Profile profile = FromJson(nlohmann::json::parse(*profile_stored), *profile_name);
  profile.databases.erase(std::string{old_name});
  profile.databases.insert(std::string{new_name});

  const std::map<std::string, std::string> to_put{
      {ProfileKey(profile.name), ProfileToJson(profile).dump()},
      {DbMappingKey(new_name), *profile_name},
  };
  const std::vector<std::string> to_delete{DbMappingKey(old_name)};
  if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) return std::unexpected{RenameError::DURABILITY_ERROR};
  return {};
}

std::optional<std::string> TenantProfiles::GetProfileForDatabase(std::string_view db_name) const {
  const std::shared_lock lock{mutex_};
  return durability_->Get(DbMappingKey(db_name));
}

}  // namespace memgraph::dbms

#endif
