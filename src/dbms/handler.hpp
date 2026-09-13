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
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
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
   * @brief Empty Handler constructor.
   *
   */
  Handler() = default;

  virtual ~Handler() {
    // FIX A: signal destruction under the lock so a concurrent DeferDelete (racing
    // with ~Handler) will not start a new background worker after we stop ours.
    {
      auto lock = std::unique_lock{pending_mutex_};
      destroying_ = true;
    }

    // Stop the background worker before touching pending_ so the worker and
    // this drain loop cannot race on the list structure or node contents.
    defer_worker_.Stop();

    // Drain under the lock: worker is stopped, but a concurrent DeferDelete call
    // could still splice into pending_ before seeing ~Handler — guard against that.
    std::list<PendingDeletion> remaining;
    {
      auto lock = std::unique_lock{pending_mutex_};
      remaining.splice(remaining.end(), pending_);
    }

    for (auto &node : remaining) {
      try {
        if (auto a = node.gk.access()) {
          if (!node.stopped) node.stop_step(*a->get());
        }
      } catch (...) {  // NOLINT(bugprone-empty-catch) — best-effort shutdown drain; one stop_step failure must not
                       // stall the rest
      }

      // Non-blocking try: if we are the sole accessor, value is destroyed here;
      // if not, the blocking ~Gatekeeper below waits for all accessors to drain.
      try {
        if (auto a = node.gk.access()) {
          (void)a->try_delete(std::chrono::milliseconds(0));
        }
      } catch (...) {  // NOLINT(bugprone-empty-catch) — best-effort shutdown drain; one try_delete failure must not
                       // stall the rest
      }

      // Blocking ~Gatekeeper: count == 0 + terminal state → returns immediately
      // in the normal case; blocks if another thread is still holding an accessor.
      {
        auto dying = std::move(node.gk);
      }

      try {
        node.post_delete_step();
      } catch (...) {  // NOLINT(bugprone-empty-catch) — best-effort shutdown drain; one post_delete failure must not
                       // stall the rest
      }
    }
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
   * @brief Defer teardown of the context associated with @p name to a shared background worker.
   *
   * @param name           Name of the item to tear down (erased from items_ unconditionally).
   * @param stop_step      One-time stop work (e.g. StopAllBackgroundTasks). Receives a T& ref.
   * @param post_delete_step  Cleanup after value destruction (e.g. remove_all(dir)).
   */
  void DeferDelete(std::string_view name, std::move_only_function<void(T &)> stop_step,
                   std::move_only_function<void()> post_delete_step) {
    auto itr = items_.find(name);
    if (itr == items_.end()) return;

    // FIX C: only a HOT gatekeeper may be deferred. A SUSPENDING/RESUMING one mid-transition
    // would make ~Gatekeeper block indefinitely waiting for a terminal state, causing a hang.
    MG_ASSERT(
        itr->second.state() == utils::GatekeeperState::HOT, "DeferDelete requires a HOT gatekeeper (name='{}')", name);

    auto gk = std::move(itr->second);
    items_.erase(itr);

    // splice is noexcept, so the transfer to pending_ cannot fail once the node is built.
    // On emplace_back bad_alloc, gk is still valid — fall through to inline teardown.
    std::list<PendingDeletion> node;
    try {
      node.emplace_back(std::move(gk), std::move(stop_step), std::move(post_delete_step));
    } catch (...) {
      // Allocation failed before gk was moved into the list; it is still valid.
      // Run best-effort inline teardown so the data directory is not orphaned.
      try {
        if (auto a = gk.access()) stop_step(*a->get());
      } catch (
          ...) {  // NOLINT(bugprone-empty-catch) — OOM fallback teardown; secondary stop_step failure is suppressed
      }
      {
        auto dying = std::move(gk);
      }
      try {
        post_delete_step();
      } catch (
          ...) {  // NOLINT(bugprone-empty-catch) — OOM fallback teardown; secondary post_delete failure is suppressed
      }
      return;
    }

    {
      auto lock = std::unique_lock{pending_mutex_};
      pending_.splice(pending_.end(), node);
    }

    // FIX B: EnsureWorkerStarted_ may throw std::system_error (pthread_create EAGAIN) after the
    // node is already spliced into pending_. Do not propagate — the logical drop has already
    // succeeded (gatekeeper moved out of items_). std::call_once does NOT consume its flag on a
    // throw, so the next DeferDelete will retry starting the worker; ~Handler drains pending_
    // unconditionally regardless of whether the worker ever ran.
    try {
      EnsureWorkerStarted_();
    } catch (...) {
      spdlog::warn(
          "DeferDelete: background teardown worker could not start (thread creation failed). "
          "Teardown will be retried on the next deferred drop or completed at shutdown.");
    }
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
  // Explicit constructor: lets std::list::emplace_back forward the three args directly.
  struct PendingDeletion {
    utils::Gatekeeper<T> gk;
    std::move_only_function<void(T &)> stop_step;
    std::move_only_function<void()> post_delete_step;
    bool stopped = false;

    PendingDeletion(utils::Gatekeeper<T> gk_, std::move_only_function<void(T &)> stop_step_,
                    std::move_only_function<void()> post_delete_step_)
        : gk{std::move(gk_)}, stop_step{std::move(stop_step_)}, post_delete_step{std::move(post_delete_step_)} {}
  };

  // Lazy: worker starts only on the first DeferDelete call, not at construction,
  // so unused Handlers pay zero thread overhead.
  void EnsureWorkerStarted_() {
    std::call_once(worker_started_, [this] {
      // FIX A: if ~Handler already set destroying_ we are racing with shutdown — do
      // not start a worker that will immediately be stopped (or worse, never be stopped).
      {
        auto lock = std::unique_lock{pending_mutex_};
        if (destroying_) return;
      }
      // Install the cadence BEFORE Run() so the worker's first loop reads the real 50 ms schedule
      // instead of the Scheduler's default (which waits until time_point::max()); a plain SetInterval
      // after Run() would not wake a worker already parked on that default wait.
      defer_worker_.SetInterval(std::chrono::milliseconds(50));
      defer_worker_.Run("defer-delete", [this] { Tick_(); });
    });
  }

  // Must be noexcept: Scheduler calls f() without a try/catch (scheduler.cpp:188),
  // so an uncaught exception would call std::terminate on the jthread.
  void Tick_() noexcept {
    try {
      // Snapshot iterators under the lock; heavy per-node work runs lock-free.
      // Only structural mutations (erase) re-acquire pending_mutex_ below.
      std::vector<typename std::list<PendingDeletion>::iterator> its;
      {
        auto lock = std::unique_lock{pending_mutex_};
        for (auto it = pending_.begin(); it != pending_.end(); ++it) its.push_back(it);
      }

      for (auto it : its) {
        try {
          auto &node = *it;

          auto acc = node.gk.access();

          if (!acc) {
            // Value already gone (defensive path — normally we are the only ones
            // with an accessor and try_delete is the one that clears value_).
            try {
              node.post_delete_step();
            } catch (...) {  // NOLINT(bugprone-empty-catch) — best-effort cleanup; one post_delete failure must not
                             // stall the tick
            }
            auto lock = std::unique_lock{pending_mutex_};
            pending_.erase(it);
            continue;
          }

          if (!node.stopped) {
            // stop_step may throw; stopped is latched only on success so the next
            // tick retries without re-running it on a partially-stopped value.
            node.stop_step(*acc->get());
            node.stopped = true;
          }

          if (acc->try_delete(std::chrono::milliseconds(0))) {
            // Release before post_delete_step — value is already gone, count: 1 → 0.
            acc.reset();
            try {
              node.post_delete_step();
            } catch (...) {  // NOLINT(bugprone-empty-catch) — best-effort cleanup; one post_delete failure must not
                             // stall the tick
            }
            // ~Gatekeeper on list node destruction: count == 0 + HOT (terminal)
            // → returns immediately without blocking.
            auto lock = std::unique_lock{pending_mutex_};
            pending_.erase(it);
          }
        } catch (...) {  // NOLINT(bugprone-empty-catch) — per-node isolation; one failing node must not stall the rest
                         // of the list
          // Per-node catch: one failing node does not stall the rest of the list.
        }
      }
    } catch (...) {
      spdlog::error("Deferred-deletion worker tick failed; will retry.");
    }
  }

  // items_ first (destroyed last) as a conservative failsafe: if ~Handler's drain
  // is bypassed, live gatekeepers in items_ outlive the worker-side structures.
  container_type items_;  //!< map of all active items

  // pending_mutex_ guards structural changes to pending_ (splice/erase) AND destroying_.
  // Node contents (stopped, stop_step) are mutated by the single worker thread only.
  std::mutex pending_mutex_;
  bool destroying_ = false;             //!< set by ~Handler before Stop(); blocks late worker starts
  std::list<PendingDeletion> pending_;  //!< nodes awaiting deferred teardown
  utils::Scheduler defer_worker_;       //!< single shared background worker (~50 ms cadence)
  std::once_flag worker_started_;       //!< ensures EnsureWorkerStarted_ runs at most once
};

}  // namespace memgraph::dbms
