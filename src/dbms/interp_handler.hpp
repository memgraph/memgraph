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

#ifdef MG_ENTERPRISE

#include <filesystem>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "global.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"
#include "utils/sync_ptr.hpp"

namespace memgraph::dbms {

/**
 * @brief Multi-tenancy handler of interpreter context.
 *
 * @tparam TContext
 * @tparam TConfig
 */
template <typename TContext = query::InterpreterContext, typename TConfig = query::InterpreterConfig>
class InterpContextHandler {
 public:
  using NewResult = utils::BasicResult<NewError, std::shared_ptr<TContext>>;

  InterpContextHandler() {}

  /**
   * @brief Create a new interpreter context associated to the "name" database.
   *
   * @param name name of the database
   * @param db database storage pointer
   * @param config interpreter configuration
   * @param data_directory underlying RocksDB directory
   * @param ah associated auth handler (@note: glue)
   * @param ac associated auth checker (@note: glue)
   * @return NewResult pointer to context on success, error on failure
   */
  NewResult New(std::string_view name, storage::Storage &db, const TConfig &config,
                const std::filesystem::path &data_directory, query::AuthQueryHandler &ah, query::AuthChecker &ac) {
    // Control that no one is using the same data directory or Storage
    // if (std::any_of(interp_.begin(), interp_.end(), [&](const auto &elem) {
    //       return elem.second.config().storage_dir == data_directory || elem.second.get()->db == &db;
    //     })) {
    //   // LOG
    //   return NewError::EXISTS;
    // }
    auto [itr, success] = interp_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                          std::forward_as_tuple(config, &db, config, data_directory, &ah, &ac));
    if (success) {
      return itr->second.get();
    }
    return NewError::EXISTS;
  }

  template <typename... T1, typename... T2, std::size_t... I1, std::size_t... I2>
  NewResult New(std::string name, std::tuple<T1...> &args1, std::tuple<T2...> &args2,
                std::integer_sequence<std::size_t, I1...> /*not-used*/,
                std::integer_sequence<std::size_t, I2...> /*not-used*/) {
    auto [itr, success] = interp_.emplace(std::piecewise_construct, std::forward_as_tuple(name),
                                          std::forward_as_tuple(TConfig{std::forward<T1>(std::get<I1>(args1))...},
                                                                std::forward<T2>(std::get<I2>(args2))...));
    if (success) {
      return itr->second.get();
    }
    return NewError::EXISTS;
  }

  template <typename... T1, typename... T2>
  NewResult New(std::piecewise_construct_t /*not-used*/, std::string name, std::tuple<T1...> args1,
                std::tuple<T2...> args2) {
    return New(name, args1, args2, std::make_index_sequence<sizeof...(T1)>{},
               std::make_index_sequence<sizeof...(T2)>{});
  }

  std::optional<std::shared_ptr<TContext>> Get(const std::string &name) {
    if (auto search = interp_.find(name); search != interp_.end()) {
      return search->second.get();
    }
    return {};
  }

  /**
   * @brief Delete the interpreter context associated with the "name" database
   *
   * @param name name of the database
   * @return true on success
   */
  bool Delete(const std::string &name) {
    if (auto itr = interp_.find(name); itr != interp_.end()) {
      interp_.erase(itr);
      return true;
    }
    return false;
  }

  auto cbegin() const { return interp_.cbegin(); }
  auto cend() const { return interp_.cend(); }

 private:
  std::unordered_map<std::string, utils::SyncPtr<TContext, TConfig>> interp_;  //!< map to all active interpreters
};

}  // namespace memgraph::dbms

#endif
