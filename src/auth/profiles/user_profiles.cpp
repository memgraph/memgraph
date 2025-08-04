// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "auth/profiles/user_profiles.hpp"

#include <mutex>
#include <shared_mutex>
#include <string>

#include "nlohmann/json.hpp"
#include "spdlog/spdlog.h"

#include "auth/rpc.hpp"

namespace memgraph::auth {

void to_json(nlohmann::json &data, const memgraph::auth::UserProfiles::limits_t &limits) {
  using up = memgraph::auth::UserProfiles;
  for (const auto &[key, val] : limits) {
    if (std::holds_alternative<up::unlimitted_t>(val)) {
      data.emplace(up::kLimits[(size_t)key], nlohmann::json(/*null*/));
    } else {
      data.emplace(up::kLimits[(size_t)key], std::get<uint64_t>(val));
    }
  }
}

void from_json(const nlohmann::json &data, memgraph::auth::UserProfiles::limits_t &limits) {
  using up = memgraph::auth::UserProfiles;
  if (!data.is_object()) return;
  for (const auto &[key, val] : data.items()) {
    const auto *key_it = std::ranges::find(up::kLimits, key);
    if (key_it == up::kLimits.end()) continue;  // unkown key
    const auto key_idx = std::distance(up::kLimits.begin(), key_it);
    if (val.is_null()) {
      limits[(up::Limits)key_idx] = up::unlimitted_t{};
    } else {
      limits[(up::Limits)key_idx] = val.get<uint64_t>();
    }
  }
}

void to_json(nlohmann::json &data, const memgraph::auth::UserProfiles::Profile &profile) {
  data["limits"] = profile.limits;
  data["usernames"] = profile.usernames;
}

void from_json(const nlohmann::json &data, memgraph::auth::UserProfiles::Profile &profile) {
  if (data.contains("limits")) profile.limits = data["limits"].get<memgraph::auth::UserProfiles::limits_t>();
  if (data.contains("usernames")) profile.usernames = data["usernames"].get<std::unordered_set<std::string>>();
}

UserProfiles::UserProfiles(kvstore::KVStore &durability) : durability_{&durability} {
  // No migration at the moment
  durability_->Put(kUserProfilesVersionkey, kUserProfilesVersion);

  // Populate local storage
  for (auto it = durability_->begin(kUserProfilesPrefix.data()); it != durability_->end(kUserProfilesPrefix.data());
       ++it) {
    const auto &key = it->first;
    const auto &value = it->second;
    const auto name = key.substr(kUserProfilesPrefix.size());
    try {
      auto profile = nlohmann::json::parse(value).get<memgraph::auth::UserProfiles::Profile>();
      profile.name = name;
      profiles_.emplace(std::move(profile));
    } catch (const nlohmann::json::parse_error &) {
      spdlog::warn("Failed to parse user profile {}", name);
    }
  }
};

bool UserProfiles::Create(std::string_view name, limits_t defined_limits,
                          const std::unordered_set<std::string> &usernames) {
  auto l = std::unique_lock{mtx_};
  if (profiles_.contains(name)) {
    return false;
  }

  // Create profile with initial usernames
  Profile profile{std::string{name}, std::move(defined_limits), usernames};

  // Remove usernames from other profiles if they exist
  for (const auto &username : usernames) {
    // Check if username is already in another profile and remove it
    for (auto &existing_profile : profiles_) {  // NOLINT
      if (existing_profile.usernames.contains(username)) {
        existing_profile.usernames.erase(username);
        // Update the other profile in durability
        const nlohmann::json json = existing_profile;
        durability_->Put(kUserProfilesPrefix.data() + existing_profile.name, json.dump());
      }
    }
  }

  const nlohmann::json json = profile;
  const auto [it, succ] = profiles_.emplace(std::move(profile));
  if (!succ) {
    return false;
  }
  if (!durability_->Put(kUserProfilesPrefix.data() + std::string{name}, json.dump())) {
    // Remove new profile
    profiles_.erase(it);
    return false;
  }
  return true;
}

std::optional<UserProfiles::Profile> UserProfiles::Update(std::string_view name, const limits_t &updated_limits) {
  auto l = std::unique_lock{mtx_};
  auto profile_it = profiles_.find(name);
  if (profile_it == profiles_.end()) {
    return std::nullopt;
  }
  auto old_limits = profile_it->limits;  // copy
  // Update local storage
  for (const auto &[key, val] : updated_limits) {
    profile_it->limits[key] = val;
  }
  // Update durability
  const nlohmann::json json = *profile_it;
  if (!durability_->Put(kUserProfilesPrefix.data() + std::string{name}, json.dump())) {
    // Revert to old profile
    profile_it->limits = std::move(old_limits);
    return std::nullopt;
  }
  return *profile_it;  // Return updated profile
}

bool UserProfiles::Drop(std::string_view name) {
  auto l = std::unique_lock{mtx_};
  auto profile_it = profiles_.find(name);
  if (profile_it == profiles_.end()) {
    return false;
  }
  auto old_profile = *profile_it;  // copy
  profiles_.erase(profile_it);
  if (!durability_->Delete(kUserProfilesPrefix.data() + std::string{name})) {
    // Revert to old profile
    profiles_.emplace(std::move(old_profile));
    return false;
  }
  return true;
}

std::optional<UserProfiles::Profile> UserProfiles::Get(std::string_view name) const {
  auto l = std::shared_lock{mtx_};
  auto profile_it = profiles_.find(name);
  if (profile_it == profiles_.end()) {
    return std::nullopt;
  }
  return *profile_it;
}

std::vector<UserProfiles::Profile> UserProfiles::GetAll() const {
  std::vector<UserProfiles::Profile> profiles;
  profiles.reserve(profiles_.size());
  auto l = std::shared_lock{mtx_};
  for (const auto &profile : profiles_) {
    profiles.emplace_back(profile);
  }
  return profiles;
}

bool UserProfiles::AddUsername(std::string_view profile_name, std::string_view username) {
  auto l = std::unique_lock{mtx_};
  auto profile_it = profiles_.find(profile_name);
  if (profile_it == profiles_.end()) {
    return false;
  }

  // Check if username is already in another profile and remove it
  for (auto &profile : profiles_) {  // NOLINT
    if (profile.usernames.contains(std::string{username})) {
      profile.usernames.erase(std::string{username});
      // Update the other profile in durability
      const nlohmann::json json = profile;
      durability_->Put(kUserProfilesPrefix.data() + profile.name, json.dump());
    }
  }

  // Add username to the target profile
  profile_it->usernames.insert(std::string{username});

  // Update durability
  const nlohmann::json json = *profile_it;
  if (!durability_->Put(kUserProfilesPrefix.data() + std::string{profile_name}, json.dump())) {
    // Revert changes
    profile_it->usernames.erase(std::string{username});
    return false;
  }

  return true;
}

bool UserProfiles::RemoveUsername(std::string_view profile_name, std::string_view username) {
  auto l = std::unique_lock{mtx_};
  auto profile_it = profiles_.find(profile_name);
  if (profile_it == profiles_.end()) {
    return false;
  }

  auto username_it = profile_it->usernames.find(std::string{username});
  if (username_it == profile_it->usernames.end()) {
    return false;
  }

  profile_it->usernames.erase(username_it);

  // Update durability
  const nlohmann::json json = *profile_it;
  if (!durability_->Put(kUserProfilesPrefix.data() + std::string{profile_name}, json.dump())) {
    // Revert changes
    profile_it->usernames.insert(std::string{username});
    return false;
  }

  return true;
}

std::unordered_set<std::string> UserProfiles::GetUsernames(std::string_view profile_name) const {
  auto l = std::shared_lock{mtx_};
  auto profile_it = profiles_.find(profile_name);
  if (profile_it == profiles_.end()) {
    return {};
  }

  return profile_it->usernames;
}

std::optional<std::string> UserProfiles::GetProfileForUsername(std::string_view username) const {
  auto l = std::shared_lock{mtx_};
  for (const auto &profile : profiles_) {
    if (profile.usernames.contains(std::string{username})) {
      return profile.name;
    }
  }
  return std::nullopt;
}

}  // namespace memgraph::auth

namespace memgraph::slk {
// JSON needs to/from in the same namespace
void to_json(nlohmann::json &data, const memgraph::auth::UserProfiles::limits_t &limits) {
  memgraph::auth::to_json(data, limits);
}
void from_json(const nlohmann::json &data, memgraph::auth::UserProfiles::limits_t &limits) {
  memgraph::auth::from_json(data, limits);
}

// Serialize code for auth::UserProfiles::Profile
void Save(const auth::UserProfiles::Profile &self, memgraph::slk::Builder *builder) {
  nlohmann::json json;
  json[self.name] = self;
  memgraph::slk::Save(json.dump(), builder);
}
// Deserialize code for auth::UserProfiles::Profile
void Load(auth::UserProfiles::Profile *self, memgraph::slk::Reader *reader) {
  std::string tmp;
  memgraph::slk::Load(&tmp, reader);
  const auto json = nlohmann::json::parse(tmp);
  const auto name = json.begin().key();
  auto profile = json.begin().value().get<auth::UserProfiles::Profile>();
  profile.name = name;
  *self = std::move(profile);
}
}  // namespace memgraph::slk
