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

#include <map>
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
  std::unique_lock lock{mutex_};
  if (durability_->Get(ProfileKey(name))) return std::unexpected{CreateError::ALREADY_EXISTS};

  Profile profile{.name = std::string{name}, .memory_limit = memory_limit};
  if (!durability_->Put(ProfileKey(profile.name), ProfileToJson(profile).dump())) {
    return std::unexpected{CreateError::DURABILITY_ERROR};
  }
  return {};
}

std::expected<std::unordered_set<std::string>, TenantProfiles::AlterError> TenantProfiles::Alter(std::string_view name,
                                                                                                 int64_t memory_limit) {
  std::unique_lock lock{mutex_};
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
  std::unique_lock lock{mutex_};
  auto stored = durability_->Get(ProfileKey(name));
  if (!stored) return std::unexpected{DropError::NOT_FOUND};

  Profile profile = FromJson(nlohmann::json::parse(*stored), name);
  if (!profile.databases.empty()) return std::unexpected{DropError::HAS_ATTACHED_DATABASES};

  if (!durability_->Delete(ProfileKey(name))) return std::unexpected{DropError::DURABILITY_ERROR};
  return {};
}

std::optional<TenantProfiles::Profile> TenantProfiles::Get(std::string_view name) const {
  std::shared_lock lock{mutex_};
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
  std::shared_lock lock{mutex_};
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
  std::unique_lock lock{mutex_};
  auto new_stored = durability_->Get(ProfileKey(profile_name));
  if (!new_stored) return std::unexpected{AttachError::PROFILE_NOT_FOUND};
  Profile new_profile = FromJson(nlohmann::json::parse(*new_stored), profile_name);

  std::map<std::string, std::string> to_put;
  if (auto old_profile_name = durability_->Get(DbMappingKey(db_name));
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

std::expected<void, TenantProfiles::DetachError> TenantProfiles::DetachFromDatabase(std::string_view db_name) {
  std::unique_lock lock{mutex_};
  auto profile_name = durability_->Get(DbMappingKey(db_name));
  if (!profile_name) return std::unexpected{DetachError::NOT_ATTACHED};

  auto profile_stored = durability_->Get(ProfileKey(*profile_name));
  if (!profile_stored) return std::unexpected{DetachError::DURABILITY_ERROR};

  Profile profile = FromJson(nlohmann::json::parse(*profile_stored), *profile_name);
  profile.databases.erase(std::string{db_name});

  std::map<std::string, std::string> to_put{{ProfileKey(profile.name), ProfileToJson(profile).dump()}};
  std::vector<std::string> to_delete{DbMappingKey(db_name)};
  if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) return std::unexpected{DetachError::DURABILITY_ERROR};
  return {};
}

std::expected<void, TenantProfiles::RenameError> TenantProfiles::RenameDatabase(std::string_view old_name,
                                                                                std::string_view new_name) {
  std::unique_lock lock{mutex_};
  auto profile_name = durability_->Get(DbMappingKey(old_name));
  if (!profile_name) return std::unexpected{RenameError::NOT_ATTACHED};

  auto profile_stored = durability_->Get(ProfileKey(*profile_name));
  if (!profile_stored) return std::unexpected{RenameError::DURABILITY_ERROR};

  Profile profile = FromJson(nlohmann::json::parse(*profile_stored), *profile_name);
  profile.databases.erase(std::string{old_name});
  profile.databases.insert(std::string{new_name});

  std::map<std::string, std::string> to_put{
      {ProfileKey(profile.name), ProfileToJson(profile).dump()},
      {DbMappingKey(new_name), *profile_name},
  };
  std::vector<std::string> to_delete{DbMappingKey(old_name)};
  if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) return std::unexpected{RenameError::DURABILITY_ERROR};
  return {};
}

std::optional<std::string> TenantProfiles::GetProfileForDatabase(std::string_view db_name) const {
  std::shared_lock lock{mutex_};
  return durability_->Get(DbMappingKey(db_name));
}

}  // namespace memgraph::dbms

#endif
