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

#pragma once

#include <fmt/core.h>
#include <functional>
#include <unordered_set>
#include <variant>

#include "kvstore/kvstore.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::auth {

class UserProfiles {
 public:
  enum class Limits : uint8_t { kSessions = 0, kTransactionsMemory };
  static constexpr std::array<std::string_view, 2> kLimits = {"sessions", "transactions_memory"};
  static_assert(kLimits.size() == (int)Limits::kTransactionsMemory + 1, "kLimits size mismatch");
  static auto AllLimits() { return fmt::format("{}, {}", kLimits[0], kLimits[1]); }

  static constexpr std::string_view kUserProfilesPrefix = "user_profile:";
  static constexpr std::string_view kUserProfilesVersionkey = "user_profile_version";
  static constexpr std::string_view kUserProfilesV1 = "V1";
  static constexpr std::string_view kUserProfilesVersion = kUserProfilesV1;

  using unlimitted_t = std::monostate;
  using limit_t = std::variant<unlimitted_t, uint64_t>;  // For now uint64_t is sufficient, might change in the future
  using limits_t = std::unordered_map<Limits, limit_t>;

  struct Profile {
    std::string name;
    mutable limits_t limits;                            // mutable to allow update in sets
    mutable std::unordered_set<std::string> usernames;  // mutable to allow update in sets

    Profile() = default;
    Profile(std::string name, limits_t limits, std::unordered_set<std::string> usernames = {})
        : name(std::move(name)), limits(std::move(limits)), usernames(std::move(usernames)) {}
  };

  explicit UserProfiles(kvstore::KVStore &durability);

  bool Create(std::string_view name, limits_t defined_limits, const std::unordered_set<std::string> &usernames = {});
  std::optional<Profile> Update(std::string_view name, const limits_t &updated_limits);
  bool Drop(std::string_view name);
  std::optional<Profile> Get(std::string_view name) const;
  std::vector<Profile> GetAll() const;

  // New methods for username management
  bool AddUsername(std::string_view profile_name, std::string_view username);
  bool RemoveUsername(std::string_view profile_name, std::string_view username);
  std::unordered_set<std::string> GetUsernames(std::string_view profile_name) const;
  std::optional<std::string> GetProfileForUsername(std::string_view username) const;

  static std::optional<Profile> Merge(const std::optional<Profile> &user, const std::optional<Profile> &role) {
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

 private:
  struct profile_hash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(const Profile &profile) const {
      return std::hash<std::string_view>{}(profile.name);
    }
    [[nodiscard]] size_t operator()(const char *s) const { return std::hash<std::string_view>{}(s); }
    [[nodiscard]] size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }
    [[nodiscard]] size_t operator()(const std::string &s) const { return std::hash<std::string>{}(s); }
  };

  struct profile_equal {
    using is_transparent = void;
    [[nodiscard]] bool operator()(const Profile &lhs, const Profile &rhs) const { return lhs.name == rhs.name; }
    [[nodiscard]] bool operator()(const char *lhs, const Profile &rhs) const { return lhs == rhs.name; }
    [[nodiscard]] bool operator()(std::string_view lhs, const Profile &rhs) const { return lhs == rhs.name; }
    [[nodiscard]] bool operator()(const std::string &lhs, const Profile &rhs) const { return lhs == rhs.name; }
  };

  mutable utils::RWSpinLock mtx_;
  kvstore::KVStore *durability_;                                       // Reuse auth's durability
  std::unordered_set<Profile, profile_hash, profile_equal> profiles_;  // Local storage
};

}  // namespace memgraph::auth
