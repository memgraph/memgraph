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
#include <expected>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "global.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/thread_pool.hpp"

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

    if (db_acc->try_delete()) {
      // Delete the database now
      db_acc->reset();
      post_delete_func();
    } else {
      // Defer deletion
      db_acc->reset();
      // TODO: Make sure this shuts down correctly
      auto task = [gk = std::move(itr->second), post_delete_func = std::forward<Func>(post_delete_func)]() mutable {
        // Destroy the gatekeeper exactly once, via natural scope — NOT an explicit gk.~Gatekeeper<T>()
        // followed by the captured gk being destructed again when this lambda is destroyed (that is a
        // double-destruction: [basic.life] UB, reading a destroyed object's pimpl_). Moving into a
        // block-scoped local runs the blocking ~Gatekeeper once here; the moved-from capture then
        // destroys cleanly (null pimpl_) with the lambda.
        {
          auto dying = std::move(gk);
        }
        post_delete_func();
      };
      defer_pool_.AddTask(std::move(task));
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
  // Declaration order is LOAD-BEARING for shutdown: members destruct in reverse declaration order, so
  // `defer_pool_` (declared last) destructs FIRST — its ~ThreadPool joins the defer thread and drains
  // queued deferred-delete tasks (each owning a moved-out Gatekeeper) BEFORE `items_` is destroyed.
  // Reordering these would let `items_` (and the live gatekeepers) be torn down while a deferred
  // ~Gatekeeper task is still running/queued -> hang or use-after-free. Keep items_ before defer_pool_.
  container_type items_;  //!< map to all active items
  utils::ThreadPool defer_pool_{1};
};

}  // namespace memgraph::dbms
