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
   * Starts the single deferred-destruction worker, paused. Each tick trylocks all pending tenants and
   * destroys those whose last accessor was released, leaving still-held ones for the next tick.
   * See DeferDelete / DrainDeferred_.
   */
  Handler() {
    defer_scheduler_.SetInterval(kDeferRetryInterval);
    // Self-pacing worker: DrainDeferred_ returns Pause when nothing is left, so this Handler never
    // juggles Pause()/Resume() itself -- DeferDelete just Wake()s it. Starts paused (nothing to drain).
    defer_scheduler_.RunSelfPaced("defer-delete", [this] { return DrainDeferred_(); });
    defer_scheduler_.Pause();
  }

  // Stop the worker, then drain any still-pending drops. Two reasons this cannot be defaulted:
  //  (1) Correctness: with the teardown deferred to the worker, a pending drop whose stop-step has not
  //      run yet still has streams/triggers holding accessors. Default member teardown of pending_ would
  //      call ~Gatekeeper, which waits for count_==0 BEFORE it destroys the Database that stops those
  //      tasks -- a deadlock. TryReserve() below runs the stop-step first, so the accessors are released.
  //  (2) Cleanup: RunCallback() fires post_delete_func (on-disk data-dir cleanup + detached-row forget),
  //      which default teardown would skip, leaking the dir and the detached_ row.
  // An orphaned (never-draining) tenant still blocks shutdown here, by design.
  virtual ~Handler() {
    defer_scheduler_.Stop();  // idempotent; the later ~Scheduler is then a no-op
    for (auto &entry : pending_) {
      try {
        entry.TryReserve();   // one-time stop-step (releases background accessors); may also destroy
        entry.RunCallback();  // destroy if still live (count_ now only external holders) + fire callback
      } catch (...) {         // NOLINT(bugprone-empty-catch): a destructor must not propagate; drain best-effort
      }
    }
    pending_.clear();
  }

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

    auto db_acc = itr->second.access();
    if (!db_acc) return;

    // Always defer destruction to the worker. Delete_ no longer stops the tenant's background tasks
    // under lock_, so ~Database -- which joins them -- must run off-lock; even a sole-held tenant is
    // reclaimed on the worker's next tick (TryReserve stops the tasks, then destroys). This keeps
    // lock_ off every bounded thread join, and keeps the tenant's DETACHED row visible for the whole
    // drain instead of forgetting it inline.
    db_acc->reset();
    // `name` may alias itr->first (Delete(uuid) passes the map key); own it for the log() below,
    // which runs after items_.erase() frees that node. Copy before the pd move so a throw here
    // strands nothing.
    const std::string name_copy{name};
    // pd owns the gatekeeper (and thus the Database) once constructed; build it OFF defer_lock_ so a
    // ~Gatekeeper it may run never happens under that lock. gk is declared last so a throw building the
    // earlier members never half-moves the source.
    PendingDestruction pd{.post_delete_func = std::forward<Func>(post_delete_func),
                          .pending = metrics::ScopedGauge{metrics::Metrics().global.pending_tenant_destructions},
                          .gk = std::move(itr->second)};
    items_.erase(itr);
    std::size_t pending_count = 0;
    try {
      auto guard = std::lock_guard{defer_lock_};
      pending_.push_back(std::move(pd));
      pending_count = pending_.size();
    } catch (...) {
      // The only throwing step is the list-node allocation, and push_back gives the strong guarantee, so
      // pd still owns the tenant. The node never reached pending_, so the worker will never reclaim it --
      // do it inline in RunCallback's order (destroy the Database, THEN run post_delete_func) so a
      // bad_alloc here cannot orphan the on-disk data directory. defer_lock_ is already released by the
      // guard's unwind, so ~Gatekeeper does not run under it.
      pd.RunCallback();
      throw;
    }
    // Wake() OUTSIDE defer_lock_ and OUTSIDE the try/catch. Outside the lock: formatting/flushing the
    // log must not stall the drain worker (which contends on defer_lock_). Outside the try: a throw
    // from Increment()/trace() must not skip Wake() -- that would leave this just-pushed node unreclaimed
    // until the next DeferDelete, parking the worker in the meantime.
    try {
      metrics::Metrics().global.deferred_tenant_destructions->Increment();
      spdlog::trace(
          "Destruction of dropped database \"{}\" deferred to the background worker; its memory stays "
          "accounted for until it is reclaimed ({} tenant destruction(s) pending).",
          name_copy,
          pending_count);
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
    defer_scheduler_.Wake();
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
    // Set once, on the worker, after this tenant's background tasks have been stopped (see TryReserve).
    // Guards the one-time StopAllBackgroundTasks/DropAll so it is not re-run on every poll tick. Leading
    // + defaulted so DeferDelete's designated-initializer (which omits it) still constructs the node.
    bool stopped_ = false;
    // `gk` is declared LAST: a throw during earlier members never half-moves the source Gatekeeper
    // (see DeferDelete). Reverse-order destruction tears gk first: no-op if completed, blocking if live.
    std::move_only_function<void()> post_delete_func;  //!< runs once, OFF defer_lock_, after teardown
    metrics::ScopedGauge pending;                      //!< holds the pending-destructions gauge up while queued
    utils::Gatekeeper<T> gk;                           //!< the tenant awaiting its last accessor's release

    // Called OFF defer_lock_ (value teardown must not run under the list mutex). Zero-timeout try_delete:
    // succeeds only if sole holder (value destroyed → true). Nullopt from access() is a dead-state backstop.
    //
    // Stop-then-drain: on the first tick that reaches a live value, stop the tenant's background tasks
    // (bounded thread joins) so they release their own accessors, THEN try_delete on this same `acc`.
    // Reusing the one accessor is deliberate: minting a second for the stop would leave count_ >= 2 and
    // wedge try_delete forever. The joins run here, on the worker, off lock_ -- one slow drop delays
    // other pending drops (bounded head-of-line), never the instance.
    bool TryReserve() {
      auto acc = gk.access();
      if (!acc) return true;
      if (!stopped_) {
        // Runs on the defer worker, whose scheduler does NOT guard its callback (Scheduler::ThreadRun
        // calls f() unguarded), so an escaping throw here would std::terminate the process. On the old
        // path these joins ran on the query thread where a throw was a recoverable query error; keep that.
        // `if constexpr (requires ...)`: Handler<T> is generic -- a unit test instantiates it with a probe
        // type that has no background tasks, so the stop-step must compile away for such T. For T == Database
        // this stops streams + after-commit triggers etc.
        if constexpr (requires(T &db) {
                        db.StopAllBackgroundTasks();
                        db.streams()->DropAll();
                      }) {
          try {
            auto *database = acc->get();
            database->StopAllBackgroundTasks();
            database->streams()->DropAll();
            // Latch stopped_ only on success. A throw (e.g. a ConsumerStopped TOCTOU) would otherwise leave
            // still-running tasks holding accessors while every later tick skips the stop -- try_delete never
            // passes and ~Gatekeeper waits unbounded at shutdown. Leaving it false lets a later tick, or the
            // ~Handler drain, re-attempt the (idempotent) stop; a fast-throwing persistent failure just
            // retries per 50ms tick.
            stopped_ = true;
          } catch (...) {  // NOLINT(bugprone-empty-catch)
            spdlog::error(
                "Deferred teardown of a dropped database could not stop its background tasks "
                "cleanly; will retry on the next tick.");
          }
        } else {
          stopped_ = true;  // nothing to stop for this T
        }
      }
      if (!acc->try_delete(kDeferTryTimeout)) {
        acc->reset();
        return false;
      }
      acc->reset();
      return true;
    }

    // Called OFF defer_lock_, so a re-entrant callback cannot self-deadlock. Destroys gk first
    // (non-blocking: value already gone, count 0), then fires post_delete_func (destroy-then-notify order).
    void RunCallback() {
      {
        auto dying = std::move(gk);
      }
      if (post_delete_func) post_delete_func();
    }
  };

  using PendingList = std::list<PendingDestruction>;

  // One tick: snapshot the list under defer_lock_, trylock each tenant OFF the lock (so ~Gatekeeper and
  // re-entrant callbacks don't run under the list mutex), splice drained nodes, park if empty.
  utils::SchedulerResult DrainDeferred_() {
    // Scheduler::ThreadRun calls this callback UNGUARDED, so an escaping throw std::terminate()s the
    // process. The per-tick vector allocations below can throw bad_alloc; keep the whole tick nothrow and,
    // on a throw, drop this tick and keep running so the next one retries.
    try {
      // Brief lock: stable std::list iterators stay valid until we splice below (only this worker erases);
      // a concurrent DeferDelete can only append — newcomers are picked up on the next tick.
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

      PendingList ready;
      bool drained_empty = false;
      {
        auto guard = std::lock_guard{defer_lock_};
        for (auto it : completed) ready.splice(ready.end(), pending_, it);
        drained_empty = pending_.empty();
      }

      // Callbacks OFF the lock; `ready` then destructs -- moved-from gks are no-ops, ScopedGauges decrement.
      for (auto &entry : ready) entry.RunCallback();

      // Park iff still empty: a racing DeferDelete either appended before the check (drained_empty = false)
      // or its Wake() after return cancels the pause (scheduler skips pause when a wake landed mid-tick).
      return drained_empty ? utils::SchedulerResult::Pause : utils::SchedulerResult::KeepRunning;
    } catch (...) {  // NOLINT(bugprone-empty-catch)
      spdlog::error("A deferred-tenant-destruction tick could not complete; it will be retried next tick.");
      return utils::SchedulerResult::KeepRunning;
    }
  }

  // Declaration order is LOAD-BEARING (members destruct in reverse declaration order):
  //   defer_scheduler_ LAST  (destructs FIRST)  → Stop+join; no tick runs after.
  //   pending_               (destructs SECOND)  → blocking ~Gatekeeper drain; callbacks finish before items_.
  //   items_                 (destructs LAST)    → live gatekeepers outlive all ticks and pending drains.
  container_type items_;
  std::mutex defer_lock_;  //!< guards pending_; taken by DeferDelete and the tick, never held across a callback
  PendingList pending_;    //!< node-stable queue of tenants awaiting their last accessor's release
  utils::Scheduler defer_scheduler_;  //!< single round-robin worker; declared LAST so it stops+joins first
};

}  // namespace memgraph::dbms
