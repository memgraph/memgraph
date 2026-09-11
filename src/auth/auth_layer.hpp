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
#include <variant>

#include "auth/atomic_auth_overlay.hpp"
#include "auth/auth.hpp"
#include "auth/repository.hpp"
#include "system/transaction.hpp"

namespace memgraph::auth {

/// One auth transaction's buffered state. The interpreter owns one from BEGIN to COMMIT or ROLLBACK, and the auth
/// query handler holds a pointer to it for the duration of a single query.
///
/// The overlay is created lazily, on the first locked call, because it needs the base store and that is only
/// reachable under the lock.
class AuthTransaction {
 public:
  AuthTransaction() = default;
  AuthTransaction(AuthTransaction const &) = delete;
  AuthTransaction &operator=(AuthTransaction const &) = delete;

  PendingActions &pending_actions() { return pending_actions_; }

 private:
  friend class AuthLayer;

  std::optional<AtomicAuthOverlay> overlay_;
  PendingActions pending_actions_;
};

/// Owns the transaction concept that sits above Auth.
///
/// Outside a transaction this is a pass-through to the locked Auth. Inside one it points Auth at the transaction's
/// overlay for the duration of each locked call, so writes buffer instead of landing on disk, and it collects the
/// replication actions that would otherwise need a system transaction held open for the transaction's whole life.
///
/// Auth itself stays unaware of any of this: it sees only the storage handle it was given.
class AuthLayer {
 public:
  using LockedAuth = decltype(std::declval<SynchedAuth &>().Lock());
  using ReadLockedAuth = decltype(std::declval<SynchedAuth const &>().ReadLock());

  /// Retargets Auth's storage at an overlay for as long as it lives, or leaves it alone when there is no
  /// transaction. Holds the lock either way, so the swap is never visible to another session.
  class ScopedOverlay {
   public:
    ScopedOverlay(LockedAuth locked, AtomicAuthOverlay *overlay) : locked_{std::move(locked)} {
      if (!overlay) return;
      epoch_.emplace(locked_->epoch());
      previous_.emplace(locked_->storage());
      locked_->storage() = Repository{*overlay};
    }

    /// Restores both the storage and the epoch. Nothing this call wrote is durable yet, so the epoch must not move:
    /// bumping it would invalidate every session's permission cache against uncommitted state, and spend the
    /// invalidation that Commit owes them once the flush lands.
    ~ScopedOverlay() {
      if (!previous_) return;
      locked_->storage() = *previous_;
      locked_->epoch() = *epoch_;
    }

    ScopedOverlay(ScopedOverlay const &) = delete;
    ScopedOverlay &operator=(ScopedOverlay const &) = delete;
    ScopedOverlay(ScopedOverlay &&) = default;
    ScopedOverlay &operator=(ScopedOverlay &&) = delete;

    Auth *operator->() const { return &*locked_; }

    Auth &operator*() const { return *locked_; }

   private:
    // Mutable because Synchronized::LockedPtr's own accessors are non-const. Const here means the guard is not
    // being modified, not that the Auth behind it is read-only.
    mutable LockedAuth locked_;
    std::optional<Auth::Epoch> epoch_;
    std::optional<Repository> previous_;
  };

  /// A read guard: a shared lock outside a transaction, or the transaction's exclusive overlay guard inside one.
  /// Reads inside a transaction must be exclusive because installing the overlay mutates Auth's storage handle;
  /// outside one they stay shared, so logins and permission checks are not serialised by SHOW USERS.
  class SharedOrOverlay {
   public:
    explicit SharedOrOverlay(ReadLockedAuth locked) : guard_{std::move(locked)} {}

    explicit SharedOrOverlay(ScopedOverlay locked) : guard_{std::move(locked)} {}

    Auth const *operator->() const {
      return std::visit([](auto const &g) -> Auth const * { return &*g; }, guard_);
    }

    Auth const &operator*() const { return *operator->(); }

   private:
    std::variant<ReadLockedAuth, ScopedOverlay> guard_;
  };

  explicit AuthLayer(SynchedAuth &auth) : auth_{&auth} {}

  /// Locked access. Outside a transaction (`tx` null) Auth works against durable storage exactly as before. Inside
  /// one, Auth is pointed at the transaction's overlay while the returned guard is alive and restored when it dies,
  /// so no other session can ever observe the buffered storage.
  ScopedOverlay Lock(AuthTransaction *tx = nullptr) {
    auto locked = auth_->Lock();
    if (!tx) return ScopedOverlay{std::move(locked), nullptr};
    if (!tx->overlay_) tx->overlay_.emplace(locked->durability());
    return ScopedOverlay{std::move(locked), &*tx->overlay_};
  }

  /// Read access: shared outside a transaction, exclusive through the overlay inside one.
  SharedOrOverlay ReadLock(AuthTransaction *tx = nullptr) {
    if (!tx) return SharedOrOverlay{auth_->ReadLock()};
    return SharedOrOverlay{Lock(tx)};
  }

  /// Flush the transaction under the write lock. Returns false on conflict, leaving durable storage untouched.
  /// On success the epoch moves once, invalidating every session's cached permissions.
  [[nodiscard]] bool Commit(AuthTransaction &tx) {
    auto locked = auth_->Lock();
    if (tx.overlay_ && !tx.overlay_->Flush()) return false;
    locked->UpdateEpoch();
    return true;
  }

 private:
  SynchedAuth *auth_;
};

}  // namespace memgraph::auth
