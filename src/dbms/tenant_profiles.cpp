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

std::string TenantProfiles::ProfileToJson(const Profile &profile) {
  nlohmann::json json;
  json["memory_limit"] = profile.memory_limit;
  json["databases"] = profile.databases;
  return json.dump();
}

TenantProfiles::TenantProfiles(kvstore::KVStore &durability) : durability_{&durability} {
  auto existing_version = durability_->Get(kVersionKey);
  if (!existing_version || *existing_version != kVersion) {
    durability_->Put(kVersionKey, kVersion);
  }

  for (auto it = durability_->begin(std::string{kPrefix}); it != durability_->end(std::string{kPrefix}); ++it) {
    const auto &[key, value] = *it;
    auto name = key.substr(kPrefix.size());
    try {
      auto profile = FromJson(nlohmann::json::parse(value), name);
      for (const auto &db : profile.databases) {
        db_to_profile_[db] = profile.name;
      }
      profiles_.emplace(std::move(name), std::move(profile));
    } catch (const nlohmann::json::parse_error &e) {
      spdlog::warn("Failed to parse tenant profile '{}': {}", name, e.what());
    }
  }

  spdlog::info("Restored {} tenant profile(s)", profiles_.size());
}

bool TenantProfiles::Create(std::string_view name, int64_t memory_limit) {
  std::unique_lock lock{mutex_};
  if (profiles_.contains(std::string{name})) return false;

  Profile profile{.name = std::string{name}, .memory_limit = memory_limit};
  if (!durability_->Put(ProfileKey(profile.name), ProfileToJson(profile))) return false;
  auto key = profile.name;
  profiles_.emplace(std::move(key), std::move(profile));
  return true;
}

std::optional<std::unordered_set<std::string>> TenantProfiles::Alter(std::string_view name, int64_t memory_limit) {
  std::unique_lock lock{mutex_};
  auto it = profiles_.find(std::string{name});
  if (it == profiles_.end()) return std::nullopt;

  auto updated = it->second;
  updated.memory_limit = memory_limit;
  if (!durability_->Put(ProfileKey(updated.name), ProfileToJson(updated))) return std::nullopt;

  it->second.memory_limit = memory_limit;
  return it->second.databases;
}

TenantProfiles::DropResult TenantProfiles::Drop(std::string_view name) {
  std::unique_lock lock{mutex_};
  auto it = profiles_.find(std::string{name});
  if (it == profiles_.end()) return DropResult::NOT_FOUND;

  if (!it->second.databases.empty()) return DropResult::HAS_ATTACHED_DATABASES;

  if (!durability_->Delete(ProfileKey(name))) return DropResult::NOT_FOUND;
  profiles_.erase(it);
  return DropResult::SUCCESS;
}

std::optional<TenantProfiles::Profile> TenantProfiles::Get(std::string_view name) const {
  std::shared_lock lock{mutex_};
  auto it = profiles_.find(std::string{name});
  if (it == profiles_.end()) return std::nullopt;
  return it->second;
}

std::vector<TenantProfiles::Profile> TenantProfiles::GetAll() const {
  std::shared_lock lock{mutex_};
  std::vector<Profile> result;
  result.reserve(profiles_.size());
  for (const auto &[_, profile] : profiles_) {
    result.push_back(profile);
  }
  return result;
}

std::optional<int64_t> TenantProfiles::AttachToDatabase(std::string_view profile_name, std::string_view db_name) {
  std::unique_lock lock{mutex_};
  auto pit = profiles_.find(std::string{profile_name});
  if (pit == profiles_.end()) return std::nullopt;

  // Build atomic write batch: update old profile (if any), update new profile, update db mapping.
  std::map<std::string, std::string> to_put;
  Profile *old_profile = nullptr;
  if (auto dit = db_to_profile_.find(std::string{db_name}); dit != db_to_profile_.end()) {
    old_profile = &profiles_.at(dit->second);
    auto updated_old = *old_profile;
    updated_old.databases.erase(std::string{db_name});
    to_put.emplace(ProfileKey(updated_old.name), ProfileToJson(updated_old));
  }
  auto updated_new = pit->second;
  updated_new.databases.insert(std::string{db_name});
  to_put.emplace(ProfileKey(updated_new.name), ProfileToJson(updated_new));
  to_put.emplace(DbMappingKey(db_name), std::string{profile_name});

  if (!durability_->PutMultiple(to_put)) return std::nullopt;

  // Persist succeeded — apply to in-memory state.
  if (old_profile) {
    old_profile->databases.erase(std::string{db_name});
  }
  pit->second.databases.insert(std::string{db_name});
  db_to_profile_[std::string{db_name}] = std::string{profile_name};
  return pit->second.memory_limit;
}

bool TenantProfiles::DetachFromDatabase(std::string_view db_name) {
  std::unique_lock lock{mutex_};
  auto dit = db_to_profile_.find(std::string{db_name});
  if (dit == db_to_profile_.end()) return false;

  auto &profile = profiles_.at(dit->second);
  auto updated = profile;
  updated.databases.erase(std::string{db_name});

  std::map<std::string, std::string> to_put{{ProfileKey(updated.name), ProfileToJson(updated)}};
  std::vector<std::string> to_delete{DbMappingKey(db_name)};
  if (!durability_->PutAndDeleteMultiple(to_put, to_delete)) return false;

  profile.databases.erase(std::string{db_name});
  db_to_profile_.erase(dit);
  return true;
}

std::optional<std::string> TenantProfiles::GetProfileForDatabase(std::string_view db_name) const {
  std::shared_lock lock{mutex_};
  auto it = db_to_profile_.find(std::string{db_name});
  if (it == db_to_profile_.end()) return std::nullopt;
  return it->second;
}

}  // namespace memgraph::dbms
