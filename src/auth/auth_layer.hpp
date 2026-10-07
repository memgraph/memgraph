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

#include <memory>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "auth/atomic_auth_overlay.hpp"
#include "auth/auth.hpp"
#include "auth/profiles/user_profiles.hpp"
#include "auth/repository.hpp"
#include "auth/rpc.hpp"
#include "system/transaction.hpp"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

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

  bool HasWrites() const { return overlay_ && overlay_->HasWrites(); }

#ifdef MG_ENTERPRISE
  std::vector<std::string> const &dropped_users() const { return dropped_users_; }

  /// Databases a statement checked exist. The write lands at COMMIT, which checks them again.
  void NameDatabase(std::string name) { named_databases_.insert(std::move(name)); }

  std::set<std::string> const &named_databases() const { return named_databases_; }
#endif

 private:
  friend class AuthLayer;

  std::optional<AtomicAuthOverlay> overlay_;
  PendingActions pending_actions_;
#ifdef MG_ENTERPRISE
  // Users whose live resource limits are released at COMMIT. ResourceMonitoring is process-wide and has no
  // rollback, so dropping them while the transaction is still open would outlive an abort.
  std::vector<std::string> dropped_users_;
  std::set<std::string> named_databases_;
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

    ~ScopedOverlay() { Restore(); }

    /// Restores the durable storage and hands the lock back, so the caller can go on without letting go of it.
    LockedAuth Release() && {
      Restore();
      return std::move(locked_);
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
    void Restore() {
      if (!previous_) return;
      locked_->storage() = *std::exchange(previous_, std::nullopt);
      locked_->sink() = nullptr;
#ifdef MG_ENTERPRISE
      locked_->dropped_users() = nullptr;
#endif
    }

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

#ifdef MG_ENTERPRISE
  /// Apply a replicated batch as one write. A replica gets a whole auth transaction or none of it: the users
  /// and roles run against an overlay in the order the main made them, and a single flush puts them in the
  /// store. Anything throwing part-way discards the overlay, leaving them as they were, and the false return
  /// tells the caller to refuse the request so the main re-sends a full snapshot. A profile is not in that set:
  /// it applies durably as it is read, for the reason below. The main never puts a profile in a batch with
  /// anything else, but nothing here enforces that, so a mixed batch that fails after its profile keeps the
  /// profile change.
  ///
  /// A drop naming nothing is not a failure. The main may have created and dropped a record between snapshots,
  /// so a removal that finds nothing is the state the main asked for. A removal that fails is a different
  /// matter and refuses the batch.
  [[nodiscard]] bool ApplyBatch(std::vector<replication::AuthOp> const &ops) {
    // Users and roles go through the overlay and land in one flush, so a replica holds all of them or none.
    //
    // Profiles do not, and must not: `UserProfiles` answers from an in-memory cache beside the store and writes
    // to both as it goes, so putting it behind the overlay would let a failed flush leave the cache holding a
    // change the store never took. Profiles are not transactional on the main either, which is why a profile
    // write is refused inside a transaction there -- so a batch carrying one always carries only that one, and
    // there is nothing for it to be atomic across.
    try {
      {
        auto locked = auth_->Lock();
        for (auto const &op : ops) {
          if (auto const *update = std::get_if<replication::AuthUpdateOp>(&op); update && update->profile) {
            if (!locked->CreateOrUpdateProfile(
                    update->profile->name, update->profile->limits, update->profile->usernames)) {
              throw AuthException("Couldn't create or update profile '{}'", update->profile->name);
            }
          } else if (auto const *drop = std::get_if<replication::AuthDropOp>(&op);
                     drop && drop->type == replication::AuthDataType::PROFILE) {
            // A profile that is not there was already dropped, which is the state the main asked for. A delete
            // that failed is not: the store still holds a profile the main removed, so refuse the batch and let
            // the main send a snapshot rather than acking a replica that has diverged.
            if (locked->DropProfile(drop->name) == UserProfiles::DropResult::kFailed) {
              throw AuthException("Couldn't drop profile '{}'", drop->name);
            }
          }
        }
      }

      AuthTransaction tx;
      auto locked = Lock(&tx);
      {
        for (auto const &op : ops) {
          std::visit(utils::Overloaded{[&](replication::AuthUpdateOp const &update) {
                                         // The main never builds an update that names nothing, so one arriving
                                         // here is a corrupt request rather than a no-op to skip.
                                         if (!update.user && !update.role && !update.profile) {
                                           throw AuthException("Received an auth update naming no record");
                                         }
                                         if (update.user) locked->SaveUser(*update.user);
                                         if (update.role) locked->SaveRole(*update.role);
                                       },
                                       [&](replication::AuthDropOp const &drop) {
                                         switch (drop.type) {
                                           using enum replication::AuthDataType;
                                           case USER:
                                             locked->RemoveUser(drop.name);
                                             break;
                                           case ROLE:
                                             locked->RemoveRole(drop.name, /*force=*/true);
                                             break;
                                           case PROFILE:
                                             break;  // applied above, outside the overlay
                                           case N:
                                             throw AuthException("Received an auth drop of no known kind");
                                         }
                                       }},
                     op);
        }
      }

      // The rest buffered cleanly; one flush puts the whole of it in the store. It runs under the lock the batch
      // was applied with, so no local write, such as a login upgrading a password hash, can land in between and
      // make the flush conflict.
      auto held = std::move(locked).Release();
      if (!Commit(held, tx, nullptr)) {
        spdlog::warn("Applying an auth batch of {} operation(s) conflicted with a local write", ops.size());
        return false;
      }
      return true;
    } catch (AuthException const &e) {
      spdlog::warn("Applying an auth batch of {} operation(s) failed: {}", ops.size(), e.what());
      return false;
    } catch (...) {
      // A refused batch is an expected outcome and says so above. Anything else reaching here is not, so it is
      // reported at a level that says so. Returning false either way is what makes the docstring's promise
      // true: the overlay is already discarded and the lock released by the time this runs, so the main is
      // told to re-send rather than left waiting on a response this replica will never produce.
      spdlog::error("Applying an auth batch of {} operation(s) failed unexpectedly", ops.size());
      return false;
    }
  }
#endif

  /// Flush the transaction under the write lock. Returns false on conflict, leaving durable storage untouched and
  /// `system_tx` empty for the caller to abort. On success with writes the epoch moves once, invalidating every
  /// session's cached permissions, and the collected replication actions move into `system_tx`.
  ///
  /// The caller owns `system_tx`: creating it here would mean holding the system mutex for the transaction's whole
  /// life, which is what the overlay exists to avoid, and committing it needs a replication handler this layer has
  /// no business knowing.
  [[nodiscard]] bool Commit(AuthTransaction &tx, system::Transaction *system_tx) {
    auto locked = auth_->Lock();
    return Commit(locked, tx, system_tx);
  }

 private:
  [[nodiscard]] bool Commit(LockedAuth &locked, AuthTransaction &tx, system::Transaction *system_tx) {
    if (tx.overlay_ && !tx.overlay_->Flush()) return false;
    // A read-only transaction is still validated above, because what it read can still have been invalidated. It
    // has nothing to publish though, so it must not spend the epoch: bumping it invalidates every session's
    // cached permissions, and nothing changed for them to re-read.
    auto const has_writes = tx.HasWrites();
    auto nothing_to_publish = tx.pending_actions_.empty();
#ifdef MG_ENTERPRISE
    nothing_to_publish = nothing_to_publish && tx.dropped_users_.empty();
#endif
    // Anything to publish or release comes from a write, so the epoch always moves with it. Publishing without
    // one would send replicas a change this instance never made durable, and leave every session's cached
    // permissions unrefreshed, neither of which anything downstream detects.
    MG_ASSERT(has_writes || nothing_to_publish,
              "An auth transaction has something to publish but never wrote to the auth store. Replicas may "
              "have received a change this instance did not keep. Compare the users, roles and profiles here "
              "against every replica before resuming writes.");
    if (has_writes) locked->UpdateEpoch();
#ifdef MG_ENTERPRISE
    // Skip a user the transaction recreated. Dropping a user does not remove it from its profile's username
    // set, so the recreated user is still a member and the limits held here are that live membership's, not a
    // dead user's residue. Releasing would erase the entry, and nothing re-applies a profile on user creation,
    // so the user would run unlimited while its profile still lists it. The erase also orphans the entry rather
    // than destroying it: open sessions hold their own shared_ptr and keep counting against it, while a later
    // login would build a fresh one, splitting the accounting for a limit that is meant to be per user.
    for (auto const &username : tx.dropped_users_) {
      if (!locked->HasUser(username)) locked->ReleaseUserResources(username);
    }
    tx.dropped_users_.clear();
#endif
    // One action for the whole transaction, so a replica applies all of it or none. An empty batch is never
    // sent: a read-only transaction has nothing to publish, and a zero-operation request would only cost a round
    // trip.
#ifdef MG_ENTERPRISE
    // A null `system_tx` means the operations are deliberately not forwarded: that is how ApplyBatch commits a
    // batch a replica has just received, since a replica must not replicate onward. A session on a main always
    // has one by here, because the interpreter creates it exactly when there is something to replicate and
    // refuses the commit if it cannot. Either way the clear below is what ends their life.
    if (system_tx && !tx.pending_actions_.empty()) {
      system_tx->AddAction(std::make_unique<BatchedAuthAction>(std::move(tx.pending_actions_)));
    }
#endif
    tx.pending_actions_.clear();
    return true;
  }

  SynchedAuth *auth_;
};

}  // namespace memgraph::auth
