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

#include "auth/profiles/user_profiles.hpp"

#include <map>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "nlohmann/json.hpp"
#include "slk/serialization.hpp"
#include "spdlog/spdlog.h"

#include "auth/rpc.hpp"
#include "utils/string.hpp"

namespace {

std::string FoldUsername(std::string_view username) { return memgraph::utils::ToLowerCase(username); }

}  // namespace

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
  if (data.contains("usernames")) {
    // Fold on load so a raw spelling persisted by a pre-fix binary stays matchable without a durable rewrite.
    std::unordered_set<std::string> usernames;
    for (const auto &username : data["usernames"].get<std::unordered_set<std::string>>()) {
      usernames.insert(FoldUsername(username));
    }
    profile.usernames = std::move(usernames);
  }
}

UserProfiles::UserProfiles(Repository &durability) : durability_{&durability} {
  // No migration at the moment
  if (!durability_->Put(kUserProfilesVersionKey, kUserProfilesVersion)) {
    spdlog::error("Failed to put user profiles version");
  }

  // Raw (pre-fold) spellings per folded user, as (spelling, profile name).
  std::map<std::string, std::vector<std::pair<std::string, std::string>>> spellings;

  // Populate local storage
  durability_->ForEachProfile([this, &spellings](auto const &name, auto const &value) {
    try {
      const auto data = nlohmann::json::parse(value);
      if (data.contains("usernames")) {
        for (auto &raw : data["usernames"].template get<std::vector<std::string>>()) {
          spellings[FoldUsername(raw)].emplace_back(std::move(raw), name);
        }
      }
      auto profile = data.template get<memgraph::auth::UserProfiles::Profile>();
      profile.name = name;
      profiles_.emplace(std::move(profile));
    } catch (const nlohmann::json::parse_error &) {
      spdlog::warn("Failed to parse user profile {}", name);
    }
  });

  // Data written before usernames were folded can place spellings of one user in several profiles. Keep one in
  // memory (the largest raw spelling, i.e. the exact-lowercase one older binaries enforced) and leave durable as is.
  for (const auto &[user, entries] : spellings) {
    std::set<std::string> names;
    const std::pair<std::string, std::string> *winner = nullptr;
    for (const auto &entry : entries) {
      names.insert(entry.second);
      if (!winner || entry.first > winner->first || (entry.first == winner->first && entry.second < winner->second)) {
        winner = &entry;
      }
    }
    if (names.size() < 2) {
      continue;
    }
    for (const auto &name : names) {
      if (name != winner->second) {
        profiles_.find(name)->usernames.erase(user);
      }
    }
    ambiguous_.insert(user);
    spdlog::error(
        "User '{}' is assigned to profiles {} under different spellings; applying profile '{}'. Run SET PROFILE FOR {} "
        "TO <profile> to choose a profile, or CLEAR PROFILE FOR {} to remove it.",
        user,
        utils::Join(names, ", "),
        winner->second,
        user,
        user);
  }
};

bool UserProfiles::Persist(const Profile &profile) {
  nlohmann::json json = profile;
  if (!ambiguous_.empty()) {
    if (const auto stored = durability_->Get(Repository::ProfileKey(profile.name))) {
      const auto stored_json = nlohmann::json::parse(*stored, nullptr, false);
      if (stored_json.is_object() && stored_json.contains("usernames") && stored_json["usernames"].is_array()) {
        for (const auto &raw : stored_json["usernames"]) {
          if (!raw.is_string()) {
            continue;
          }
          const auto folded = FoldUsername(raw.get<std::string>());
          if (ambiguous_.contains(folded) && !profile.usernames.contains(folded)) {
            json["usernames"].push_back(raw);
          }
        }
      }
    }
  }
  return durability_->Put(Repository::ProfileKey(profile.name), json.dump());
}

bool UserProfiles::ResolveAmbiguity(const std::string &folded) {
  if (!ambiguous_.contains(folded)) {
    return true;
  }
  // Erase first so Persist stops carrying this user's stale spellings.
  ambiguous_.erase(folded);
  for (const auto &profile : profiles_) {
    if (!Persist(profile)) {
      ambiguous_.insert(folded);
      spdlog::warn("Failed to clear stale profile membership of user '{}'; it stays ambiguous.", folded);
      return false;
    }
  }
  return true;
}

bool UserProfiles::Create(std::string_view name, limits_t defined_limits,
                          const std::unordered_set<std::string> &usernames) {
  auto l = std::unique_lock{mtx_};
  if (profiles_.contains(name)) {
    return false;
  }

  // Create profile with initial usernames
  std::unordered_set<std::string> folded_usernames;
  folded_usernames.reserve(usernames.size());
  for (const auto &username : usernames) {
    folded_usernames.insert(FoldUsername(username));
  }
  Profile profile{std::string{name}, std::move(defined_limits), std::move(folded_usernames)};

  // Remove usernames from other profiles if they exist
  for (const auto &username : profile.usernames) {
    // Check if username is already in another profile and remove it
    for (auto &existing_profile : profiles_) {  // NOLINT
      if (existing_profile.usernames.contains(username)) {
        existing_profile.usernames.erase(username);
        // Update the other profile in durability
        Persist(existing_profile);
      }
    }
  }

  const auto [it, succ] = profiles_.emplace(std::move(profile));
  if (!succ) {
    return false;
  }
  if (!Persist(*it)) {
    // Remove new profile
    profiles_.erase(it);
    return false;
  }
  for (const auto &username : it->usernames) {
    ResolveAmbiguity(username);
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
  if (!Persist(*profile_it)) {
    // Revert to old profile
    profile_it->limits = std::move(old_limits);
    return std::nullopt;
  }
  return *profile_it;  // Return updated profile
}

UserProfiles::DropResult UserProfiles::Drop(std::string_view name) {
  auto l = std::unique_lock{mtx_};
  auto profile_it = profiles_.find(name);
  if (profile_it == profiles_.end()) {
    return DropResult::kAbsent;
  }
  auto old_profile = *profile_it;  // copy
  profiles_.erase(profile_it);
  if (!durability_->Delete(Repository::ProfileKey(name))) {
    // Revert to old profile
    profiles_.emplace(std::move(old_profile));
    return DropResult::kFailed;
  }
  for (const auto &username : old_profile.usernames) {
    ResolveAmbiguity(username);
  }
  return DropResult::kDropped;
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
  auto l = std::shared_lock{mtx_};
  std::vector<UserProfiles::Profile> profiles;
  profiles.reserve(profiles_.size());
  for (const auto &profile : profiles_) {
    profiles.emplace_back(profile);
  }
  return profiles;
}

std::optional<UserProfiles::Profile> UserProfiles::AddUsername(std::string_view profile_name,
                                                               std::string_view username) {
  auto l = std::unique_lock{mtx_};
  auto profile_it = profiles_.find(profile_name);
  if (profile_it == profiles_.end()) {
    return std::nullopt;
  }

  const auto folded_username = FoldUsername(username);

  // Check if username is already in another profile and remove it
  for (auto &profile : profiles_) {  // NOLINT
    if (profile.usernames.contains(folded_username)) {
      profile.usernames.erase(folded_username);
      // Update the other profile in durability
      Persist(profile);
    }
  }

  // Add username to the target profile
  profile_it->usernames.insert(folded_username);

  // Update durability
  if (!Persist(*profile_it)) {
    // Revert changes
    profile_it->usernames.erase(folded_username);
    return std::nullopt;
  }

  ResolveAmbiguity(folded_username);
  return *profile_it;
}

UserProfiles::MembershipResult UserProfiles::RemoveUsername(std::string_view profile_name, std::string_view username) {
  auto l = std::unique_lock{mtx_};
  auto profile_it = profiles_.find(profile_name);
  if (profile_it == profiles_.end()) {
    return MembershipResult::kAbsent;
  }

  const auto folded_username = FoldUsername(username);
  auto username_it = profile_it->usernames.find(folded_username);
  if (username_it == profile_it->usernames.end()) {
    return MembershipResult::kAbsent;
  }

  profile_it->usernames.erase(username_it);

  // Update durability
  if (!Persist(*profile_it)) {
    // Revert changes
    profile_it->usernames.insert(folded_username);
    return MembershipResult::kFailed;
  }

  ResolveAmbiguity(folded_username);
  return MembershipResult::kChanged;
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
  const auto folded_username = FoldUsername(username);
  for (const auto &profile : profiles_) {
    if (profile.usernames.contains(folded_username)) {
      return profile.name;
    }
  }
  return std::nullopt;
}

bool UserProfiles::IsAmbiguous(std::string_view username) const {
  auto l = std::shared_lock{mtx_};
  return ambiguous_.contains(FoldUsername(username));
}

std::optional<UserProfiles::Profile> UserProfiles::Merge(const std::optional<Profile> &user,
                                                         const std::optional<Profile> &role) {
  if (!user && !role) return std::nullopt;
  if (!user) return role;
  if (!role) return user;
  Profile merged = *user;
  merged.name = fmt::format("{}\n{}", user->name, role->name);
  for (const auto &[limit, value] : role->limits) {
    const auto [it, succ] = merged.limits.try_emplace(limit, value);
    if (!succ) {
      // Limit already present, merge (take the lower value)
      if (std::holds_alternative<unlimitted_t>(it->second)) {
        // If the existing limit is unlimited, we overwrite it with the role's limit
        it->second = value;
      } else if (std::holds_alternative<unlimitted_t>(value)) {
        // If the role's limit is unlimited, we keep the existing one
        continue;
      } else {
        // Otherwise, we keep the lower value
        it->second = std::min(std::get<uint64_t>(it->second), std::get<uint64_t>(value));
      }
    }
  }
  return merged;
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
