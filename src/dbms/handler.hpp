// Copyright 2023 Memgraph Ltd.
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
#include "utils/result.hpp"
#include "utils/sync_ptr.hpp"

namespace memgraph::dbms {

/**
 * @brief Generic multi-database content handler.
 *
 * @tparam TContext
 * @tparam TConfig
 */
template <typename TContext, typename TConfig>
class Handler {
 public:
  using NewResult = utils::BasicResult<NewError, std::shared_ptr<TContext>>;

  /**
   * @brief Empty Handler constructor.
   *
   */
  Handler() {}

  /**
   * @brief Generate a new context and corresponding configuration.
   *
   * @tparam T1 Variadic template of context constructor arguments
   * @tparam T2 Variadic template of config constructor arguments
   * @param name Name associated with the new context/config pair
   * @param args1 Arguments passed (as a tuple) to the context constructor
   * @param args2 Arguments passed (as a tuple) to the config constructor
   * @return NewResult
   */
  template <typename... T1, typename... T2>
  NewResult New(std::string name, std::tuple<T1...> args1, std::tuple<T2...> args2) {
    return New_(name, args1, args2, std::make_index_sequence<sizeof...(T1)>{},
                std::make_index_sequence<sizeof...(T2)>{});
  }

  /**
   * @brief Get pointer to context.
   *
   * @param name Name associated with the wanted context
   * @return std::optional<std::shared_ptr<TContext>>
   */
  std::optional<std::shared_ptr<TContext>> Get(const std::string &name) {
    if (auto search = items_.find(name); search != items_.end()) {
      return search->second.get();
    }
    return {};
  }

  /**
   * @brief Get the config.
   *
   * @param name Name associated with the wanted config
   * @return std::optional<TConfig>
   */
  std::optional<TConfig> GetConfig(const std::string &name) const {
    if (auto search = items_.find(name); search != items_.end()) {
      return search->second.config();
    }
    return {};
  }

  /**
   * @brief Delete the context/config pair associated with the name.
   *
   * @param name Name associated with the context/config pair to delete
   * @return true on success
   */
  bool Delete(const std::string &name) {
    if (auto itr = items_.find(name); itr != items_.end()) {
      itr->second.DestroyAndSync();
      items_.erase(itr);
      return true;
    }
    return false;
  }

  /**
   * @brief Check if a name is already used.
   *
   * @param name Name to check
   * @return true if a context/config pair is already associated with the name
   */
  bool Has(const std::string &name) const { return items_.find(name) != items_.end(); }

  auto begin() { return items_.begin(); }
  auto end() { return items_.end(); }
  auto begin() const { return items_.begin(); }
  auto end() const { return items_.end(); }
  auto cbegin() const { return items_.cbegin(); }
  auto cend() const { return items_.cend(); }

 private:
  /**
   * @brief Lower level handler that hides some ugly code.
   *
   * @tparam T1 Variadic template of context constructor arguments
   * @tparam T2 Variadic template of config constructor arguments
   * @tparam I1 List of indexes associated with the first tuple
   * @tparam I2 List of indexes associated with the second tuple
   */
  template <typename... T1, typename... T2, std::size_t... I1, std::size_t... I2>
  NewResult New_(std::string name, std::tuple<T1...> &args1, std::tuple<T2...> &args2,
                 std::integer_sequence<std::size_t, I1...> /*not-used*/,
                 std::integer_sequence<std::size_t, I2...> /*not-used*/) {
    // Make sure the emplace will succeed, since we don't want to create temporary objects that could break something
    if (!Has(name)) {
      auto [itr, _] = items_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                     std::forward_as_tuple(TConfig{std::forward<T1>(std::get<I1>(args1))...},
                                                           std::forward<T2>(std::get<I2>(args2))...));
      return itr->second.get();
    }
    spdlog::info("Item with name \"{}\" already exists.", name);
    return NewError::EXISTS;
  }

  std::unordered_map<std::string, utils::SyncPtr<TContext, TConfig>> items_;  //!< map to all active items
};

}  // namespace memgraph::dbms
