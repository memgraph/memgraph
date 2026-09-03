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

/// A handle over the storage auth works against: either the durable KVStore or a transaction's overlay.
///
/// `kvstore::KVStore` is final and has no virtual methods, so the two cannot share a base class. This adapter
/// dispatches over both without touching kvstore, letting Auth stay unaware of which one it holds.
class AuthStorage {
 public:
  explicit AuthStorage(kvstore::KVStore &base) : target_{&base} {}

  explicit AuthStorage(AtomicAuthOverlay &overlay) : target_{&overlay} {}

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

  size_t Size(std::string const &prefix = "") const {
    return std::visit([&prefix](auto *target) { return target->Size(prefix); }, target_);
  }

  /// Fn receives std::pair<std::string, std::string> const &.
  template <typename Fn>
  void ForEach(std::string const &prefix, Fn &&fn) {
    AnyOf(prefix, [&fn](auto const &entry) {
      fn(entry);
      return false;
    });
  }

  /// Returns true on the first match; short-circuits.
  template <typename Pred>
  bool AnyOf(std::string const &prefix, Pred &&pred) {
    return std::visit(
        [&prefix, &pred](auto *target) {
          for (auto it = target->begin(prefix); it != target->end(prefix); ++it) {
            if (pred(*it)) return true;
          }
          return false;
        },
        target_);
  }

  bool HasAny(std::string const &prefix) {
    return AnyOf(prefix, [](auto const &) { return true; });
  }

 private:
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
