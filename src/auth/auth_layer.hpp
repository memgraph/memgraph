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
#include <string>
#include <utility>
#include <variant>
#include <vector>

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

#ifdef MG_ENTERPRISE
  std::vector<std::string> const &dropped_users() const { return dropped_users_; }
#endif

 private:
  friend class AuthLayer;

  std::optional<AtomicAuthOverlay> overlay_;
  PendingActions pending_actions_;
#ifdef MG_ENTERPRISE
  // Users whose live resource limits are released at COMMIT. ResourceMonitoring is process-wide and has no
  // rollback, so dropping them while the transaction is still open would outlive an abort.
  std::vector<std::string> dropped_users_;
#endif
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
    ScopedOverlay(LockedAuth locked, AtomicAuthOverlay *overlay, PendingActions *sink,
                  std::vector<std::string> *dropped_users)
        : locked_{std::move(locked)} {
      if (!overlay) return;
      previous_.emplace(locked_->storage());
      locked_->storage() = Repository{*overlay};
      locked_->sink() = sink;
#ifdef MG_ENTERPRISE
      locked_->dropped_users() = dropped_users;
#endif
    }

    ~ScopedOverlay() {
      if (!previous_) return;
      locked_->storage() = *previous_;
      locked_->sink() = nullptr;
#ifdef MG_ENTERPRISE
      locked_->dropped_users() = nullptr;
#endif
    }

    ScopedOverlay(ScopedOverlay const &) = delete;
    ScopedOverlay &operator=(ScopedOverlay const &) = delete;

    /// Moving transfers the restore duty. A defaulted move would leave the source's `previous_` engaged, since
    /// moving an optional leaves it so, and the source's destructor would then restore the durable storage while
    /// the moved-to guard is still using the overlay.
    ScopedOverlay(ScopedOverlay &&other) noexcept
        : locked_{std::move(other.locked_)}, previous_{std::exchange(other.previous_, std::nullopt)} {}

    ScopedOverlay &operator=(ScopedOverlay &&) = delete;

    Auth *operator->() const { return &*locked_; }

    Auth &operator*() const { return *locked_; }

   private:
    // Mutable because Synchronized::LockedPtr's own accessors are non-const. Const here means the guard is not
    // being modified, not that the Auth behind it is read-only.
    mutable LockedAuth locked_;
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
    if (!tx) return ScopedOverlay{std::move(locked), nullptr, nullptr, nullptr};
    if (!tx->overlay_) tx->overlay_.emplace(locked->durability());
#ifdef MG_ENTERPRISE
    return ScopedOverlay{std::move(locked), &*tx->overlay_, &tx->pending_actions_, &tx->dropped_users_};
#else
    return ScopedOverlay{std::move(locked), &*tx->overlay_, &tx->pending_actions_, nullptr};
#endif
  }

  /// Read access: shared outside a transaction, exclusive through the overlay inside one.
  SharedOrOverlay ReadLock(AuthTransaction *tx = nullptr) {
    if (!tx) return SharedOrOverlay{auth_->ReadLock()};
    return SharedOrOverlay{Lock(tx)};
  }

  /// Flush the transaction under the write lock. Returns false on conflict, leaving durable storage untouched and
  /// `system_tx` empty for the caller to abort. On success the epoch moves once, invalidating every session's
  /// cached permissions, and the collected replication actions move into `system_tx`.
  ///
  /// The caller owns `system_tx`: creating it here would mean holding the system mutex for the transaction's whole
  /// life, which is what the overlay exists to avoid, and committing it needs a replication handler this layer has
  /// no business knowing.
  [[nodiscard]] bool Commit(AuthTransaction &tx, system::Transaction *system_tx) {
    auto locked = auth_->Lock();
    if (tx.overlay_ && !tx.overlay_->Flush()) return false;
    locked->UpdateEpoch();
    if (system_tx) {
      for (auto &action : tx.pending_actions_) system_tx->AddAction(std::move(action));
    }
    tx.pending_actions_.clear();
#ifdef MG_ENTERPRISE
    for (auto const &username : tx.dropped_users_) locked->ReleaseUserResources(username);
    tx.dropped_users_.clear();
#endif
    return true;
  }

 private:
  SynchedAuth *auth_;
};

}  // namespace memgraph::auth
