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

#include <list>
#include <memory>
#include <optional>
#include <utility>

#include "auth/atomic_auth_overlay.hpp"
#include "auth/auth.hpp"
#include "auth/auth_storage.hpp"
#include "system/transaction.hpp"

namespace memgraph::auth {

/// Owns the transaction concept that sits above Auth.
///
/// Outside a transaction this is a pass-through to the locked Auth. Inside one it points Auth at the transaction's
/// overlay for the duration of each locked call, so writes buffer instead of landing on disk, and it collects the
/// replication actions that would otherwise need a system transaction held open for the transaction's whole life.
///
/// Auth itself stays unaware of any of this: it sees only the storage handle it was given.
class AuthLayer {
 public:
  /// One auth transaction's buffered state. The interpreter owns one from BEGIN to COMMIT or ROLLBACK.
  ///
  /// The overlay is created lazily, on the first locked call, because it needs the base store and that is only
  /// reachable under the lock.
  class Transaction {
   public:
    Transaction() = default;
    Transaction(Transaction const &) = delete;
    Transaction &operator=(Transaction const &) = delete;

    PendingActions &pending_actions() { return pending_actions_; }

   private:
    friend class AuthLayer;

    std::optional<AtomicAuthOverlay> overlay_;
    PendingActions pending_actions_;
  };

  explicit AuthLayer(SynchedAuth &auth) : auth_{&auth} {}

  /// Locked access outside a transaction: Auth works against durable storage, exactly as before.
  auto Lock() { return auth_->Lock(); }

  auto ReadLock() const { return auth_->ReadLock(); }

  /// Locked access inside a transaction. Auth is pointed at the transaction's overlay while the returned guard is
  /// alive, and restored when it dies, so no other session can ever observe the buffered storage.
  auto Lock(Transaction &tx) {
    auto locked = auth_->Lock();
    if (!tx.overlay_) tx.overlay_.emplace(locked->durability());
    return ScopedOverlay{std::move(locked), *tx.overlay_};
  }

  /// Flush the transaction under the write lock. Returns false on conflict, leaving durable storage untouched.
  /// On success the epoch moves once, invalidating every session's cached permissions.
  [[nodiscard]] bool Commit(Transaction &tx) {
    auto locked = auth_->Lock();
    if (tx.overlay_ && !tx.overlay_->Flush()) return false;
    locked->UpdateEpoch();
    return true;
  }

 private:
  /// Retargets Auth's storage at an overlay for as long as it lives. Holds the lock, so the swap is never visible to
  /// another session.
  using LockedAuth = decltype(std::declval<SynchedAuth &>().Lock());

  class ScopedOverlay {
   public:
    ScopedOverlay(LockedAuth locked, AtomicAuthOverlay &overlay)
        : locked_{std::move(locked)}, epoch_{locked_->epoch()} {
      previous_.emplace(locked_->storage());
      locked_->storage() = AuthStorage{overlay};
    }

    /// Restores both the storage and the epoch. Nothing this call wrote is durable yet, so the epoch must not move:
    /// bumping it would invalidate every session's permission cache against uncommitted state, and spend the
    /// invalidation that Commit owes them once the flush lands.
    ~ScopedOverlay() {
      locked_->storage() = *previous_;
      locked_->epoch() = epoch_;
    }

    ScopedOverlay(ScopedOverlay const &) = delete;
    ScopedOverlay &operator=(ScopedOverlay const &) = delete;

    Auth *operator->() { return &*locked_; }

    Auth &operator*() { return *locked_; }

   private:
    LockedAuth locked_;
    Auth::Epoch epoch_;
    std::optional<AuthStorage> previous_;
  };

  SynchedAuth *auth_;
};

}  // namespace memgraph::auth
