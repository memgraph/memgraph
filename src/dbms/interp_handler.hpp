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

#include "global.hpp"
#include "query/auth_checker.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "storage/v2/storage.hpp"

#include "handler.hpp"

namespace memgraph::dbms {

/**
 * @brief Simple class that adds useful information to the query's InterpreterContext
 *
 * @tparam T Multi-database handler type
 */
template <typename T>
class ExpandedInterpContext : public query::InterpreterContext {
 public:
  template <typename... TArgs>
  explicit ExpandedInterpContext(T &ref, TArgs &&...args)
      : query::InterpreterContext(std::forward<TArgs>(args)...), sc_handler_(ref) {}

  T &sc_handler_;  //!< Multi-database/SessionContext handler (used in some queries)
};

/**
 * @brief Simple structure that expands on the query's InterpreterConfig
 *
 */
struct ExpandedInterpConfig {
  storage::Config storage_config;          //!< Storage configuration
  query::InterpreterConfig interp_config;  //!< Interpreter configuration
};

/**
 * @brief Multi-database interpreter context handler
 *
 * @tparam TSCHandler High-level multi-database/SessionContext handler type
 */
template <typename TSCHandler>
class InterpContextHandler : public Handler<ExpandedInterpContext<TSCHandler>, ExpandedInterpConfig> {
 public:
  using InterpContextT = ExpandedInterpContext<TSCHandler>;
  using HandlerT = Handler<InterpContextT, ExpandedInterpConfig>;

  /**
   * @brief Generate a new interpreter context associated with the passed name.
   *
   * @param name Name associating the new interpreter context
   * @param sc_handler Multi-database/SessionContext handler used (some queries might use it)
   * @param db Storage associated with the interpreter context
   * @param config Interpreter's configuration
   * @param dir Directory used by the interpreter
   * @param auth_handler AuthQueryHandler used
   * @param auth_checker AuthChecker used
   * @return HandlerT::NewResult
   */
  typename HandlerT::NewResult New(const std::string &name, TSCHandler &sc_handler, storage::Config storage_config,
                                   const query::InterpreterConfig &interpreter_config,
                                   query::AuthQueryHandler &auth_handler, query::AuthChecker &auth_checker) {
    // Check if compatible with the existing interpreters
    if (std::any_of(HandlerT::cbegin(), HandlerT::cend(), [&](const auto &elem) {
          const auto &config = elem.second.config().storage_config;
          return config.durability.storage_directory == storage_config.durability.storage_directory;
        })) {
      spdlog::info("Tried to generate a new context using claimed directory and/or storage.");
      return NewError::EXISTS;
    }
    const auto dir = storage_config.durability.storage_directory;
    storage_config.name = name;  // Set storage id via config
    return HandlerT::New(
        name, std::forward_as_tuple(storage_config, interpreter_config),
        std::forward_as_tuple(sc_handler, storage_config, interpreter_config, dir, &auth_handler, &auth_checker));
  }

  /**
   * @brief All currently active storage.
   *
   * @return std::vector<std::string>
   */
  std::vector<std::string> All() const {
    std::vector<std::string> res;
    res.reserve(std::distance(HandlerT::cbegin(), HandlerT::cend()));
    std::for_each(HandlerT::cbegin(), HandlerT::cend(), [&](const auto &elem) { res.push_back(elem.first); });
    return res;
  }
};

}  // namespace memgraph::dbms

#endif
