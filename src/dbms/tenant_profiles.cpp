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

void ToJson(nlohmann::json &j, const TenantProfiles::Profile &p) {
  j["memory_limit"] = p.memory_limit;
  j["databases"] = p.databases;
}

auto FromJson(const nlohmann::json &j, std::string_view name) -> TenantProfiles::Profile {
  TenantProfiles::Profile p;
  p.name = name;
  if (j.contains("memory_limit")) p.memory_limit = j["memory_limit"].get<int64_t>();
  if (j.contains("databases")) p.databases = j["databases"].get<std::unordered_set<std::string>>();
  return p;
}

}  // namespace

TenantProfiles::TenantProfiles(kvstore::KVStore &durability) : durability_{&durability} {
  auto existing_version = durability_->Get(std::string{kVersionKey});
  if (!existing_version || *existing_version != kVersion) {
    durability_->Put(std::string{kVersionKey}, std::string{kVersion});
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
  auto lock = std::unique_lock{mtx_};
  if (profiles_.contains(std::string{name})) return false;

  Profile p{.name = std::string{name}, .memory_limit = memory_limit};
  Save(p);
  auto key = p.name;
  profiles_.emplace(std::move(key), std::move(p));
  return true;
}

std::optional<std::unordered_set<std::string>> TenantProfiles::Alter(std::string_view name, int64_t memory_limit) {
  auto lock = std::unique_lock{mtx_};
  auto it = profiles_.find(std::string{name});
  if (it == profiles_.end()) return std::nullopt;

  it->second.memory_limit = memory_limit;
  Save(it->second);
  return it->second.databases;
}

bool TenantProfiles::Drop(std::string_view name) {
  auto lock = std::unique_lock{mtx_};
  auto it = profiles_.find(std::string{name});
  if (it == profiles_.end()) return false;

  if (!it->second.databases.empty()) return false;

  DeleteProfile(name);
  profiles_.erase(it);
  return true;
}

std::optional<TenantProfiles::Profile> TenantProfiles::Get(std::string_view name) const {
  auto lock = std::shared_lock{mtx_};
  auto it = profiles_.find(std::string{name});
  if (it == profiles_.end()) return std::nullopt;
  return it->second;
}

std::vector<TenantProfiles::Profile> TenantProfiles::GetAll() const {
  auto lock = std::shared_lock{mtx_};
  std::vector<Profile> result;
  result.reserve(profiles_.size());
  for (const auto &[_, p] : profiles_) {
    result.push_back(p);
  }
  return result;
}

std::optional<int64_t> TenantProfiles::AttachToDatabase(std::string_view profile_name, std::string_view db_name) {
  auto lock = std::unique_lock{mtx_};

  auto pit = profiles_.find(std::string{profile_name});
  if (pit == profiles_.end()) return std::nullopt;

  if (auto dit = db_to_profile_.find(std::string{db_name}); dit != db_to_profile_.end()) {
    auto &old_profile = profiles_.at(dit->second);
    old_profile.databases.erase(std::string{db_name});
    Save(old_profile);
    db_to_profile_.erase(dit);
  }

  pit->second.databases.insert(std::string{db_name});
  Save(pit->second);
  SaveDbMapping(db_name, profile_name);
  db_to_profile_[std::string{db_name}] = std::string{profile_name};
  return pit->second.memory_limit;
}

bool TenantProfiles::DetachFromDatabase(std::string_view db_name) {
  auto lock = std::unique_lock{mtx_};
  auto dit = db_to_profile_.find(std::string{db_name});
  if (dit == db_to_profile_.end()) return false;

  auto &profile = profiles_.at(dit->second);
  profile.databases.erase(std::string{db_name});
  Save(profile);
  DeleteDbMapping(db_name);
  db_to_profile_.erase(dit);
  return true;
}

std::optional<std::string> TenantProfiles::GetProfileForDatabase(std::string_view db_name) const {
  auto lock = std::shared_lock{mtx_};
  auto it = db_to_profile_.find(std::string{db_name});
  if (it == db_to_profile_.end()) return std::nullopt;
  return it->second;
}

void TenantProfiles::Save(const Profile &profile) {
  nlohmann::json j;
  ToJson(j, profile);
  durability_->Put(ProfileKey(profile.name), j.dump());
}

void TenantProfiles::SaveDbMapping(std::string_view db_name, std::string_view profile_name) {
  durability_->Put(DbMappingKey(db_name), std::string{profile_name});
}

void TenantProfiles::DeleteDbMapping(std::string_view db_name) { durability_->Delete(DbMappingKey(db_name)); }

void TenantProfiles::DeleteProfile(std::string_view name) { durability_->Delete(ProfileKey(name)); }

}  // namespace memgraph::dbms
