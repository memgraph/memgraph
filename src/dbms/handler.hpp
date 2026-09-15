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
#include <string>
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
   * @brief Start the deferred-teardown background worker.
   * @param defer_interval Retry cadence; default is coarse (teardown is non-urgent); inject a short value in tests.
   */
  explicit Handler(std::chrono::milliseconds defer_interval = std::chrono::seconds{10}) {
    // SetInterval before Run: Scheduler default wait is time_point::max(); a post-Run SetInterval
    // cannot wake a parked worker. A construction-time tick is safe: pending_ is empty until DeferDelete.
    defer_worker_.SetInterval(defer_interval);
    defer_worker_.Run("defer-delete", [this] { Tick_(); });
  }

  virtual ~Handler() {
    // Set shutting_down_ before Stop() so a concurrent DeferDelete tears down inline
    // rather than splicing into a pending_ we are about to drain.
    {
      auto lock = std::unique_lock{pending_mutex_};
      shutting_down_ = true;
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

    // Unbounded: ~Gatekeeper blocks until accessor count hits zero; Bolt session reaping must precede ~Handler.
    for (auto &node : remaining) {
      TeardownNode_(node);
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
   * @brief Defer teardown of @p name (erased from items_) to the background worker.
   * @param id Opaque caller tag (e.g. UUID) surfaced by PendingItems().
   * @param stop_step Before gatekeeper destruction (e.g. StopAllBackgroundTasks); @param post_delete_step after.
   */
  void DeferDelete(std::string_view name, std::string id, std::move_only_function<void(T &)> stop_step,
                   std::move_only_function<void()> post_delete_step) {
    auto itr = items_.find(name);
    if (itr == items_.end()) return;

    // Only a HOT gatekeeper may be deferred: a SUSPENDING/RESUMING one mid-transition would
    // make ~Gatekeeper block indefinitely waiting for a terminal state.
    DMG_ASSERT(
        itr->second.state() == utils::GatekeeperState::HOT, "DeferDelete requires a HOT gatekeeper (name='{}')", name);

    auto gk = std::move(itr->second);
    items_.erase(itr);

    // splice is noexcept; if emplace_back throws (OOM), node allocation fails before any arg is moved,
    // so gk and id are intact for the fallback.
    std::list<PendingDeletion> node;
    try {
      node.emplace_back(
          std::move(gk), std::string{name}, std::move(id), std::move(stop_step), std::move(post_delete_step));
    } catch (...) {
      // OOM in emplace_back; gk is intact — run the same teardown sequence.
      PendingDeletion fallback{
          std::move(gk), std::string{name}, std::move(id), std::move(stop_step), std::move(post_delete_step)};
      TeardownNode_(fallback);
      return;
    }

    {
      auto lock = std::unique_lock{pending_mutex_};
      if (shutting_down_) {
        // ~Handler has already drained pending_; splicing would orphan this node.
        // Release the lock before the potentially-blocking ~Gatekeeper and user callbacks.
        lock.unlock();
        TeardownNode_(node.front());
        return;
      }
      pending_.splice(pending_.end(), node);
    }
  }

  // Optional per-tick hook: the background worker invokes it for each draining husk node (passing the
  // node's opaque id) at the start of each tick, BEFORE try_delete. Supplied by a higher layer (kept
  // type-erased so this generic handler has no dependency on it). The hook must release AND destroy any
  // external accessors pinning the husk before returning, so the gatekeeper's count is up to date when
  // try_delete runs. Must not block and must not re-enter this Handler.
  void SetDrainHook(std::function<void(std::string_view id)> hook) {
    auto lock = std::unique_lock{pending_mutex_};
    drain_hook_ = std::move(hook);
  }

  // Clears the hook. MUST be called during shutdown before whatever the hook closes over is destroyed,
  // otherwise a late tick would invoke a dangling closure (use-after-free).
  void ClearDrainHook() {
    auto lock = std::unique_lock{pending_mutex_};
    drain_hook_ = nullptr;
  }

  std::vector<std::pair<std::string, std::string>> PendingItems() const {
    auto lock = std::unique_lock{pending_mutex_};
    std::vector<std::pair<std::string, std::string>> out;
    out.reserve(pending_.size());
    for (auto const &node : pending_) out.emplace_back(node.name, node.id);
    return out;
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
  // Explicit constructor: lets std::list::emplace_back forward the five args directly.
  struct PendingDeletion {
    utils::Gatekeeper<T> gk;
    std::string name;
    std::string id;  // opaque caller tag (e.g. UUID string) — used by PendingItems()
    std::move_only_function<void(T &)> stop_step;
    std::move_only_function<void()> post_delete_step;
    bool stopped = false;
    std::chrono::steady_clock::time_point enqueued_at;
    bool warned = false;

    PendingDeletion(utils::Gatekeeper<T> gk_, std::string name_, std::string id_,
                    std::move_only_function<void(T &)> stop_step_, std::move_only_function<void()> post_delete_step_)
        : gk{std::move(gk_)},
          name{std::move(name_)},
          id{std::move(id_)},
          stop_step{std::move(stop_step_)},
          post_delete_step{std::move(post_delete_step_)},
          enqueued_at{std::chrono::steady_clock::now()} {}
  };

  // MUST be called with no Handler lock held: ~Gatekeeper and post_delete_step may block.
  // Each step is independently caught — a throwing stop_step does not skip teardown.
  void TeardownNode_(PendingDeletion &node) {
    if (!node.stopped) {
      try {
        if (auto a = node.gk.access()) node.stop_step(*a->get());
      } catch (...) {  // NOLINT(bugprone-empty-catch) best-effort teardown; stop_step failure must not skip the rest
      }
    }
    {
      auto dying = std::move(node.gk);
    }
    try {
      node.post_delete_step();
    } catch (...) {  // NOLINT(bugprone-empty-catch) best-effort teardown; post_delete failure is suppressed
    }
  }

  // Must be noexcept: Scheduler calls f() without a try/catch (scheduler.cpp:188),
  // so an uncaught exception would call std::terminate on the jthread.
  void Tick_() noexcept {
    try {
      // Snapshot iterators under the lock; heavy per-node work runs lock-free.
      // Only structural mutations (erase) re-acquire pending_mutex_ below.
      std::vector<typename std::list<PendingDeletion>::iterator> its;
      std::function<void(std::string_view)> hook;
      {
        auto lock = std::unique_lock{pending_mutex_};
        for (auto it = pending_.begin(); it != pending_.end(); ++it) its.push_back(it);
        hook = drain_hook_;  // copy while holding lock; called below WITHOUT the lock to avoid lock inversion
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
            } catch (...) {  // NOLINT(bugprone-empty-catch) best-effort; post_delete failure must not stall the tick
            }
            auto lock = std::unique_lock{pending_mutex_};
            pending_.erase(it);
            continue;
          }

          if (!node.stopped) {
            // stop_step must be idempotent: stopped is latched only on success, so a
            // throwing invocation will be re-invoked on the next tick until it succeeds.
            node.stop_step(*acc->get());
            node.stopped = true;
          }

          if (hook) hook(node.id);  // release external pins for this husk before attempting teardown

          if (acc->try_delete(kDeferTryTimeout)) {
            acc.reset();
            try {
              node.post_delete_step();
            } catch (...) {  // NOLINT(bugprone-empty-catch) best-effort; post_delete failure must not stall the tick
            }
            // ~Gatekeeper on node destruction: count == 0 + HOT → returns without blocking.
            auto lock = std::unique_lock{pending_mutex_};
            pending_.erase(it);
          } else if (!node.warned && std::chrono::steady_clock::now() - node.enqueued_at > kStuckWarnAfter) {
            // One-shot warning (warned latched). GatekeeperLabelFor<T> returns "" when the
            // type does not declare gatekeeper_label(), producing the unlabeled message variant.
            auto const label = utils::GatekeeperLabelFor<T>::get(*acc->get());
            if (label.empty()) {
              spdlog::warn(
                  "Deferred teardown has been pending for over 5 minutes; "
                  "a holder has not released the resource.");
            } else {
              spdlog::warn(
                  "Deferred teardown has been pending for over 5 minutes; "
                  "a holder has not released the resource '{}'.",
                  label);
            }
            node.warned = true;
          }
        } catch (...) {  // NOLINT(bugprone-empty-catch) per-node isolation; one failing node must not stall the rest
        }
      }
    } catch (...) {
      spdlog::error("Deferred-deletion worker tick failed; will retry.");
    }
  }

  //!< Absorbs a transient accessor; limits per-node cost to ~10 ms. Worst-case tick latency is
  //!< N * kDeferTryTimeout; a new unpinned drop is serviced within defer_interval + N * kDeferTryTimeout.
  static constexpr auto kDeferTryTimeout = std::chrono::milliseconds{10};
  static constexpr auto kStuckWarnAfter = std::chrono::minutes{5};

  // items_ first (destroyed last) as a conservative failsafe: if ~Handler's drain
  // is bypassed, live gatekeepers in items_ outlive the worker-side structures.
  container_type items_;

  // pending_mutex_ guards structural changes to pending_ (splice/erase) AND shutting_down_.
  // Node contents (stopped, stop_step) are mutated by the single worker thread only; no lock needed.
  mutable std::mutex pending_mutex_;
  bool shutting_down_ = false;  //!< set by ~Handler before Stop(); guards the DeferDelete/drain race
  std::list<PendingDeletion> pending_;
  std::function<void(std::string_view id)> drain_hook_;  //!< guarded by pending_mutex_; see SetDrainHook
  utils::Scheduler defer_worker_;  //!< background teardown worker; cadence default 10 s, injectable via constructor
};

}  // namespace memgraph::dbms
