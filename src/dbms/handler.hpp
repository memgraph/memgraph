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
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/result.hpp"

namespace memgraph::dbms {

/**
 * @brief Generic multi-database content handler.
 *
 * @tparam T
 */
template <typename T>
class Handler {
 public:
  using NewResult = utils::BasicResult<NewError, typename utils::Gatekeeper<T>::access>;

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
  template <typename... Args>
  NewResult New(std::piecewise_construct_t /* marker */, std::string name, Args... args) {
    // Make sure the emplace will succeed, since we don't want to create temporary objects that could break something
    if (!Has(name)) {
      auto [itr, _] = items_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                     std::forward_as_tuple(std::forward<Args>(args)...));
      auto [item, ok] = itr->second.Access();
      if (ok) return std::move(item);
      return NewError::DEFUNCT;
    }
    spdlog::info("Item with name \"{}\" already exists.", name);
    return NewError::EXISTS;
  }

  /**
   * @brief Get pointer to context.
   *
   * @param name Name associated with the wanted context
   * @return std::optional<std::shared_ptr<T>>
   */
  std::optional<typename utils::Gatekeeper<T>::access> Get(std::string_view name) {
    if (auto search = items_.find(name.data()); search != items_.end()) {
      auto [item, ok] = search->second.Access();
      if (ok) return item;
    }
    return std::nullopt;
  }

  /**
   * @brief Delete the context associated with the name.
   *
   * @param name Name associated with the context to delete
   * @return true on success
   */
  bool Delete(const std::string &name) {
    if (auto itr = items_.find(name); itr != items_.end()) {
      auto [item, ok] = itr->second.Access();
      if (ok && item.try_delete()) {
        item.reset();
        items_.erase(itr);
        return true;
      }
      return false;
    }
    throw utils::BasicException("Unknown item \"{}\".", name);
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
  std::unordered_map<std::string, utils::Gatekeeper<T>> items_;  //!< map to all active items
};

}  // namespace memgraph::dbms
