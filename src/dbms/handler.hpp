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

#include <spdlog/spdlog.h>
#include <chrono>
#include <expected>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "global.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "metrics/scoped_gauge.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/scheduler.hpp"

namespace memgraph::dbms {

/**
 * @brief Generic multi-database content handler.
 *
 * @tparam T
 */
template <typename T>
class Handler {
 public:
  struct string_hash {
    using is_transparent = void;

    [[nodiscard]] size_t operator()(const char *s) const { return std::hash<std::string_view>{}(s); }

    [[nodiscard]] size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }

    [[nodiscard]] size_t operator()(const std::string &s) const { return std::hash<std::string>{}(s); }
  };

  using container_type = std::unordered_map<std::string, utils::Gatekeeper<T>, string_hash, std::equal_to<>>;
  using value_type = typename container_type::value_type;
  using reference = typename container_type::reference;
  using const_reference = typename container_type::const_reference;
  using iterator = typename container_type::iterator;
  using const_iterator = typename container_type::const_iterator;
  using difference_type = typename container_type::difference_type;
  using size_type = typename container_type::size_type;
  using NewResult = std::expected<typename utils::Gatekeeper<T>::Accessor, NewError>;

  /**
   * @brief Handler constructor.
   *
   * Starts the single deferred-destruction reschedule worker, paused (nothing to drain yet). Every
   * later deferred destruction rides this one worker instead of a thread of its own: on each tick it
   * trylocks each pending tenant and destroys the ones whose last accessor has been released, leaving
   * the still-held ones for the next tick (round-robin). See DeferDelete / DrainDeferred_.
   */
  Handler() {
    defer_scheduler_.Run("defer-delete", [this] { DrainDeferred_(); });
    defer_scheduler_.Pause();
  }

  // Defaulted: defer_scheduler_ is the LAST member, so ~Scheduler (which Stop()s and JOINS the tick
  // worker) runs FIRST, before pending_/items_ are torn down -- the tick references both. Any tenant
  // still pending after the join is destroyed by pending_'s own teardown (blocking ~Gatekeeper),
  // exactly as before: a genuinely un-drainable tenant still holds up shutdown, by design.
  virtual ~Handler() = default;

  /**
   * @brief Generate a new context and corresponding configuration.
   *
   * @tparam Args Variadic template of constructor arguments of T
   * @param name Name associated with the new T
   * @param args Arguments passed to the constructor of T
   * @return NewResult
   */
  template <typename... Args>
  NewResult New(std::piecewise_construct_t /* marker */, std::string_view name, Args &&...args) {
    // Make sure the emplace will succeed, since we don't want to create temporary objects that could break something
    if (!Has(name)) {
      auto [itr, _] = items_.emplace(
          std::piecewise_construct, std::forward_as_tuple(name), std::forward_as_tuple(std::forward<Args>(args)...));
      auto db_acc = itr->second.access();
      if (db_acc) return std::move(*db_acc);
      return std::unexpected{NewError::DEFUNCT};
    }
    spdlog::info("Item with name \"{}\" already exists.", name);
    return std::unexpected{NewError::EXISTS};
  }

  /**
   * @brief Emplace a no-value COLD shell gatekeeper for @p name.
   *
   * Hot/cold restart recovery: a COLD (suspended) tenant has a durable metadata entry but no
   * in-memory storage. This inserts the no-value shell (state COLD) so a later resume can
   * move-assign a fresh HOT gatekeeper over it, exactly as a runtime SUSPEND leaves the in-map
   * gatekeeper. The cold_shell_t ctor builds the shell with no value (access() == nullopt).
   *
   * @param name Name to associate with the COLD shell
   * @return the in-map gatekeeper pointer, or nullptr if @p name is already present
   */
  utils::Gatekeeper<T> *EmplaceColdShell(std::string_view name) {
    if (Has(name)) return nullptr;
    auto [itr, _] =
        items_.emplace(std::piecewise_construct, std::forward_as_tuple(name), std::forward_as_tuple(utils::cold_shell));
    return &itr->second;
  }

  /**
   * @brief Erase a COLD-shell entry (no live value) directly.
   *
   * Unlike TryDelete (which needs a live HOT accessor and throws for an unknown name),
   * this removes a suspended tenant's gatekeeper by name. Safe ONLY when the gatekeeper
   * is strictly in the COLD state: count==0 and no transition in flight. Callers MUST
   * ensure the tenant is COLD before calling (DeleteCold_ does the state check under
   * lock_ so by the time EraseColdShell is reached the invariant already holds).
   *
   * Defense-in-depth: refuse to erase anything that is not strictly COLD. A HOT tenant
   * (state HOT) takes the wrong path; a SUSPENDING/RESUMING tenant mid-transition would
   * make ~Gatekeeper block forever waiting for a terminal state while the caller holds
   * lock_ — deadlock. This check is the backstop that prevents that scenario even if the
   * caller's own state check is bypassed or races.
   *
   * @param name Name associated with the COLD shell to erase
   * @return true if erased, false if absent or if the entry is not in the COLD state
   */
  bool EraseColdShell(std::string_view name) {
    auto itr = items_.find(name);
    if (itr == items_.end()) return false;
    // Refuse anything not strictly COLD: a HOT value, or a SUSPENDING/RESUMING shell
    // mid-transition. Erasing a RESUMING/SUSPENDING gatekeeper would block ~Gatekeeper
    // forever (it waits for a terminal state) while the caller holds lock_ -> deadlock.
    if (itr->second.state() != utils::GatekeeperState::COLD) return false;
    items_.erase(itr);
    return true;
  }

  /**
   * @brief Get pointer to context.
   *
   * @param name Name associated with the wanted context
   * @return std::optional<typename utils::Gatekeeper<T>::Accessor>
   */
  std::optional<typename utils::Gatekeeper<T>::Accessor> Get(std::string_view name) {
    if (auto search = items_.find(name); search != items_.end()) {
      return search->second.access();
    }
    return std::nullopt;
  }

  /**
   * @brief Get a raw (non-owning) pointer to the in-map gatekeeper by name.
   *
   * The pointer is stable across insert/erase of OTHER entries (std::unordered_map
   * node stability) and is used to drive suspend/resume state transitions on an
   * in-map (possibly COLD) gatekeeper. Caller must ensure the entry is not erased
   * while using the pointer.
   *
   * @param name Name associated with the wanted Gatekeeper
   * @return utils::Gatekeeper<T> * (nullptr if absent)
   */
  utils::Gatekeeper<T> *GetGatekeeper(std::string_view name) {
    auto itr = items_.find(name);
    if (itr == items_.end()) return nullptr;
    return &itr->second;
  }

  /**
   * @brief Delete the context associated with the name.
   *
   * @param name Name associated with the context to delete
   * @return true on success
   * @throw BasicException
   */
  bool TryDelete(std::string_view name) {
    if (auto itr = items_.find(name); itr != items_.end()) {
      // Deliberately the plain, drain-gated access() -- NOT utils::drain_bypass. This does not
      // participate in the drain protocol at all: its only caller (DbmsHandler::TryDelete) already
      // fails earlier at its own GetConfig lookup for a tenant under drop. More importantly, a
      // bypassed mint here could win Accessor::try_delete() behind an in-flight FORCE drop's back --
      // during that drop's off-lock phase its own accessor is released, so count_ can be exactly 1,
      // letting this path destroy value_ and erase the entry out from under the drop, which would
      // then falsely promote its registry row and roll back a drain on a gatekeeper that no longer
      // exists.
      auto db_acc = itr->second.access();
      if (db_acc && db_acc->try_delete()) {
        db_acc->reset();
        items_.erase(itr);
        return true;
      }
      return false;
    }
    // TODO: Change to return enum
    throw utils::BasicException("Unknown item \"{}\".", name);
  }

  /**
   * @brief Delete or defunct the context associated with the name.
   *
   * @param name Name associated with the context to delete
   * @param post_delete_func What to do after deletion has happened
   */
  template <typename Func>
  void DeferDelete(std::string_view name, Func &&post_delete_func) {
    auto itr = items_.find(name);
    if (itr == items_.end()) return;

    // utils::drain_bypass is mandatory here, not a convenience: DeferDelete is the drop path itself,
    // so it owns whatever drain it (or its caller) already declared on this gatekeeper -- and
    // items_.erase(itr) below is unconditional, reached whether the accessor mints or not. A
    // drain-gated access() failing here would `return` before that erase, stranding the tenant in
    // items_ forever: unreachable by name (its own access() refuses), never destroyed, its name
    // permanently unusable.
    auto db_acc = itr->second.access(utils::drain_bypass);
    if (!db_acc) return;

    if (db_acc->try_delete()) {
      // Delete the database now
      db_acc->reset();
      post_delete_func();
    } else {
      // Defer deletion
      db_acc->reset();
      auto guard = std::lock_guard{defer_lock_};
      // `gk` is the LAST-evaluated member of the aggregate below: designated initializers run in
      // member-declaration order (post_delete_func, then the ScopedGauge, then gk), so if an earlier
      // one throws -- post_delete_func's move, or the gauge's non-noexcept Increment() -- `itr->second`
      // is never moved from. Nothing is left half-destroyed and the unconditional items_.erase(itr)
      // below still removes a fully-intact entry. On the happy path the node now owns the Gatekeeper.
      pending_.emplace_back(
          PendingDestruction{.post_delete_func = std::forward<Func>(post_delete_func),
                             .pending = metrics::ScopedGauge{metrics::Metrics().global.pending_tenant_destructions},
                             .gk = std::move(itr->second)});
      // Guarded and swallowed: the node above already owns the only handle to the Gatekeeper, so if the
      // counter Increment, the throwing-capable spdlog::warn, or a Scheduler wake escaped, it would skip
      // the unconditional items_.erase(itr) below and strand a moved-from husk under a live name. Silent
      // on purpose -- logging is one of the things being guarded against. Resume() clears any pause left
      // by a previous fully-drained tick; SetIntervalAndWake re-arms the tick period and wakes the
      // (possibly parked) worker so the first retry is prompt, not up-to-one-interval late.
      try {
        metrics::Metrics().global.deferred_tenant_destructions->Increment();
        spdlog::warn(
            "Destruction of dropped database \"{}\" is deferred because it is still in use; its memory "
            "stays accounted for until the last accessor is released ({} tenant destruction(s) pending).",
            name,
            pending_.size());
        defer_scheduler_.Resume();
        defer_scheduler_.SetIntervalAndWake(kDeferRetryInterval);
      } catch (...) {  // NOLINT(bugprone-empty-catch)
      }
    }
    // In any case remove from handled map
    items_.erase(itr);
  }

  /**
   * @brief Check if a name is already used.
   *
   * @param name Name to check
   * @return true if a T is already associated with the name
   */
  bool Has(std::string_view name) const { return items_.contains(name); }

  /**
   * @brief Rename the context associated with the name.
   *
   * @param old_name Name associated with the context to rename
   * @param new_name New name for the context
   * @return true on success, false if new_name already exists or context is in use
   */
  std::expected<void, RenameError> Rename(std::string_view old_name, std::string_view new_name) {
    auto old_itr = items_.find(old_name);
    if (old_itr == items_.end()) {
      return std::unexpected{RenameError::NON_EXISTENT};
    }

    auto new_itr = items_.find(new_name);
    if (new_itr != items_.end()) {
      return std::unexpected{RenameError::ALREADY_EXISTS};
    }

    // Move the gatekeeper to the new name
    auto gatekeeper = std::move(old_itr->second);
    items_.erase(old_itr);
    items_.emplace(new_name, std::move(gatekeeper));
    return {};
  }

  iterator begin() noexcept { return items_.begin(); }

  iterator end() noexcept { return items_.end(); }

  const_iterator begin() const noexcept { return items_.begin(); }

  const_iterator end() const noexcept { return items_.end(); }

  const_iterator cbegin() const noexcept { return items_.cbegin(); }

  const_iterator cend() const noexcept { return items_.cend(); }

  [[nodiscard]] size_type size() const noexcept { return items_.size(); }

  [[nodiscard]] bool empty() const noexcept { return items_.empty(); }

 private:
  static constexpr auto kDeferTryTimeout = std::chrono::milliseconds{0};      //!< pure trylock; no blocking
  static constexpr auto kDeferRetryInterval = std::chrono::milliseconds{50};  //!< round-robin tick cadence

  // A tenant dropped while an accessor is still held: its Gatekeeper is moved out of items_ into one of
  // these nodes and destroyed later, on the reschedule worker, once the last accessor is released.
  struct PendingDestruction {
    // Members are declared in the order DeferDelete's designated-init aggregate evaluates them; `gk` is
    // LAST so a throw while building an earlier member never leaves the source Gatekeeper half-moved
    // (see DeferDelete). Destruction (reverse order) tears `gk` down first: on a completed entry that is
    // a moved-from no-op, on a shutdown-surviving entry it is the blocking ~Gatekeeper drain.
    std::move_only_function<void()> post_delete_func;  //!< runs once, OFF defer_lock_, after teardown
    metrics::ScopedGauge pending;                      //!< holds the pending-destructions gauge up while queued
    utils::Gatekeeper<T> gk;                           //!< the tenant awaiting its last accessor's release

    // Non-blocking trylock, called OFF defer_lock_ (so the value teardown never runs under the list
    // mutex). Mints an accessor and try_delete()s it with a zero timeout: succeeds only if this is the
    // sole live accessor RIGHT NOW (all external holders
    // released), in which case the managed value is destroyed here and true is returned so the tick
    // splices this node out. Otherwise the accessor is released and false leaves the node for the next
    // tick -- a tenant nobody releases is retried forever but never blocks the others (round-robin).
    // Mints with utils::drain_bypass, exactly like DeferDelete's own access() above: a dropped tenant
    // has been begin_drain()'d, and plain access() refuses a draining tenant. Bypassing is mandatory,
    // not a convenience -- plain access() would return nullopt for every pending (hence draining)
    // tenant, so `if (!acc) return true` would splice a still-live tenant out and RunCallback()'s
    // blocking ~Gatekeeper would wait, under this single worker, for that tenant's last accessor. One
    // pinned tenant would then head-of-line-block every other tenant's deferred destruction -- the
    // exact starvation this round-robin worker exists to prevent. The nullopt branch is now only a
    // dead-state backstop: value_ already gone (state no longer HOT), which try_delete() reports as
    // done.
    bool TryReserve() {
      auto acc = gk.access(utils::drain_bypass);
      if (!acc) return true;
      if (!acc->try_delete(kDeferTryTimeout)) {
        acc->reset();
        return false;
      }
      acc->reset();
      return true;
    }

    // Runs after a successful TryReserve(), OFF defer_lock_ -- matching the old per-tenant worker's
    // lock-free context, so a callback that re-enters the Handler cannot self-deadlock on defer_lock_.
    // Tears the (already value-less) Gatekeeper down first -- non-blocking, its wait predicate (HOT +
    // count 0) is already satisfied -- then fires the callback, preserving the old destroy-then-notify
    // order the detached-tenant registry's ForgetDetached_ depends on.
    void RunCallback() {
      {
        auto dying = std::move(gk);
      }
      if (post_delete_func) post_delete_func();
    }
  };

  using PendingList = std::list<PendingDestruction>;

  // The reschedule tick: one pass over the pending tenants. Trylock each OFF defer_lock_ (so neither the
  // ~Gatekeeper teardown nor a re-entrant callback runs while the list mutex is held), splice out the
  // ones that drained, run their callbacks, and pause the worker once nothing is left to retry.
  void DrainDeferred_() {
    // Snapshot the nodes present now, under the lock, briefly. std::list nodes are stable and only this
    // (single) worker ever erases them, so each iterator stays valid until we splice it out below; a
    // concurrent DeferDelete only appends, and those newcomers are simply picked up on the next tick.
    std::vector<typename PendingList::iterator> snapshot;
    {
      auto guard = std::lock_guard{defer_lock_};
      snapshot.reserve(pending_.size());
      for (auto it = pending_.begin(); it != pending_.end(); ++it) snapshot.push_back(it);
    }

    // Trylock + value teardown OFF the lock.
    std::vector<typename PendingList::iterator> completed;
    for (auto it : snapshot) {
      if (it->TryReserve()) completed.push_back(it);
    }

    // Splice the drained nodes out under the lock, then decide whether to park the worker.
    PendingList ready;
    {
      auto guard = std::lock_guard{defer_lock_};
      for (auto it : completed) ready.splice(ready.end(), pending_, it);
      // Pausing here is atomic with the empty check under defer_lock_: a DeferDelete that raced in a new
      // entry either ran before this pass (its node is in pending_, so we don't pause) or takes
      // defer_lock_ after us and Resume()s -- no lost wakeup either way.
      if (pending_.empty()) defer_scheduler_.Pause();
    }

    // Callbacks OFF the lock; `ready` then destructs -- moved-from gks are no-ops, ScopedGauges decrement.
    for (auto &entry : ready) entry.RunCallback();
  }

  // Declaration order is LOAD-BEARING for shutdown (members destruct in reverse):
  //   defer_scheduler_ FIRST -> ~Scheduler Stop()s+JOINs the tick worker, so no tick touches pending_/
  //     items_ after this point;
  //   pending_ NEXT           -> ~Gatekeeper drains any tenant still held (blocking, as before);
  //   items_ LAST             -> the live gatekeepers outlive every tick that could reach into them.
  // `items_` before `pending_` is future-proofing: today's only caller (DbmsHandler) passes a callback
  // that touches neither items_ nor this Handler, but a future caller's could, so the join stays ahead
  // of items_'s teardown.
  container_type items_;   //!< map to all active items
  std::mutex defer_lock_;  //!< guards pending_; taken by DeferDelete and the tick, never held across a callback
  PendingList pending_;    //!< node-stable queue of tenants awaiting their last accessor's release
  utils::Scheduler defer_scheduler_;  //!< single round-robin worker; declared LAST so it stops+joins first
};

}  // namespace memgraph::dbms
