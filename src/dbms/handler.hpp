// Copyright 2024 Memgraph Ltd.
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

#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "global.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/result.hpp"
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
  using NewResult = utils::BasicResult<NewError, typename utils::Gatekeeper<T>::Accessor>;

  /**
   * @brief Empty Handler constructor.
   *
   */
  Handler() = default;

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
      auto [itr, _] = items_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                     std::forward_as_tuple(std::forward<Args>(args)...));
      auto db_acc = itr->second.access();
      if (db_acc) return std::move(*db_acc);
      return NewError::DEFUNCT;
    }
    spdlog::info("Item with name \"{}\" already exists.", name);
    return NewError::EXISTS;
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
        gk.~Gatekeeper<T>();
        post_delete_func();
      };
      defer_pool_.AddTask(utils::CopyMovableFunctionWrapper{std::move(task)});
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
  bool Has(std::string_view name) const { return items_.find(name) != items_.end(); }

  auto begin() { return items_.begin(); }
  auto end() { return items_.end(); }
  auto begin() const { return items_.begin(); }
  auto end() const { return items_.end(); }
  auto cbegin() const { return items_.cbegin(); }
  auto cend() const { return items_.cend(); }

  auto size() const { return items_.size(); }

  struct string_hash {
    using is_transparent = void;
    [[nodiscard]] size_t operator()(const char *s) const { return std::hash<std::string_view>{}(s); }
    [[nodiscard]] size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }
    [[nodiscard]] size_t operator()(const std::string &s) const { return std::hash<std::string>{}(s); }
  };

 private:
  std::unordered_map<std::string, utils::Gatekeeper<T>, string_hash, std::equal_to<>>
      items_;  //!< map to all active items
  utils::ThreadPool defer_pool_{1};
};

}  // namespace memgraph::dbms
