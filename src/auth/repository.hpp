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

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "auth/atomic_auth_overlay.hpp"
#include "kvstore/kvstore.hpp"

namespace memgraph::auth {

/// The store auth reads and writes: either the durable KVStore or a transaction's overlay.
///
/// This is the seam the rules sit on. A rules call is handed a repository and works in terms of keys and bytes,
/// so it cannot tell whether its writes are landing on disk or buffering in a transaction.
///
/// `kvstore::KVStore` is final and has no virtual methods, so the two cannot share a base class. This dispatches
/// over both without touching kvstore.
class Repository {
 public:
  explicit Repository(kvstore::KVStore &base) : target_{&base} {}

  explicit Repository(AtomicAuthOverlay &overlay) : target_{&overlay} {}

  std::optional<std::string> Get(std::string_view key) const {
    return std::visit([key](auto *target) { return target->Get(key); }, target_);
  }

  bool Put(std::string_view key, std::string_view value) {
    return std::visit([key, value](auto *target) { return AsBool([&] { return target->Put(key, value); }); }, target_);
  }

  bool Delete(std::string_view key) {
    return std::visit([key](auto *target) { return AsBool([&] { return target->Delete(key); }); }, target_);
  }

  bool PutMultiple(std::map<std::string, std::string> const &items) {
    return std::visit([&items](auto *target) { return target->PutMultiple(items); }, target_);
  }

  bool DeleteMultiple(std::vector<std::string> const &keys) {
    return std::visit([&keys](auto *target) { return target->DeleteMultiple(keys); }, target_);
  }

  bool PutAndDeleteMultiple(std::map<std::string, std::string> const &puts, std::vector<std::string> const &deletes) {
    return std::visit(
        [&puts, &deletes](auto *target) { return AsBool([&] { return target->PutAndDeleteMultiple(puts, deletes); }); },
        target_);
  }

  /// The key schema. Callers name an entity; only the repository knows how that becomes a key.
  static std::string UserKey(std::string_view username) { return Key(kUserPrefix, username); }

  static std::string RoleKey(std::string_view rolename) { return Key(kRolePrefix, rolename); }

  static std::string RoleLinkKey(std::string_view username) { return Key(kRoleLinkPrefix, username); }

  static std::string MtLinkKey(std::string_view username) { return Key(kMtLinkPrefix, username); }

  static std::string ProfileKey(std::string_view profile_name) { return Key(kProfilePrefix, profile_name); }

  /// Entity scans. Fn receives (name, value): the name has the prefix already stripped, so no caller needs to
  /// know the key format to take one apart.
  template <typename Fn>
  void ForEachUser(Fn &&fn) const {
    ForEachEntity(kUserPrefix, std::forward<Fn>(fn));
  }

  template <typename Fn>
  void ForEachRole(Fn &&fn) const {
    ForEachEntity(kRolePrefix, std::forward<Fn>(fn));
  }

  template <typename Fn>
  void ForEachRoleLink(Fn &&fn) const {
    ForEachEntity(kRoleLinkPrefix, std::forward<Fn>(fn));
  }

  template <typename Fn>
  void ForEachMtLink(Fn &&fn) const {
    ForEachEntity(kMtLinkPrefix, std::forward<Fn>(fn));
  }

  template <typename Fn>
  void ForEachProfile(Fn &&fn) const {
    ForEachEntity(kProfilePrefix, std::forward<Fn>(fn));
  }

  bool HasAnyUser() const { return HasAny(kUserPrefix); }

  bool HasAnyRole() const { return HasAny(kRolePrefix); }

 private:
  static constexpr std::string_view kUserPrefix = "user:";
  static constexpr std::string_view kRolePrefix = "role:";
  static constexpr std::string_view kRoleLinkPrefix = "link:";
  static constexpr std::string_view kMtLinkPrefix = "mtlink:";
  static constexpr std::string_view kProfilePrefix = "user_profile:";

  static std::string Key(std::string_view prefix, std::string_view name) { return std::string{prefix}.append(name); }

  /// Fn receives (name, value), with `prefix` stripped from the key. The name is owned: callers store it, move
  /// from it, and pass it to interfaces taking `std::string const &`.
  template <typename Fn>
  void ForEachEntity(std::string_view prefix, Fn &&fn) const {
    AnyOf(prefix, [&fn, prefix](auto const &entry) {
      fn(entry.first.substr(prefix.size()), entry.second);
      return false;
    });
  }

  /// Returns true on the first match; short-circuits.
  template <typename Pred>
  bool AnyOf(std::string_view prefix, Pred &&pred) const {
    auto const prefix_str = std::string{prefix};
    return std::visit(
        [&prefix_str, &pred](auto *target) {
          for (auto it = target->begin(prefix_str); it != target->end(prefix_str); ++it) {
            if (pred(*it)) return true;
          }
          return false;
        },
        target_);
  }

  bool HasAny(std::string_view prefix) const {
    return AnyOf(prefix, [](auto const &) { return true; });
  }

  /// The overlay's mutating operations return void where KVStore returns bool; they cannot fail, since a write only
  /// buffers into the write-set and the base is untouched until Flush.
  template <typename Fn>
  static bool AsBool(Fn &&fn) {
    if constexpr (std::is_void_v<std::invoke_result_t<Fn>>) {
      std::forward<Fn>(fn)();
      return true;
    } else {
      return std::forward<Fn>(fn)();
    }
  }

  std::variant<kvstore::KVStore *, AtomicAuthOverlay *> target_;
};

}  // namespace memgraph::auth
