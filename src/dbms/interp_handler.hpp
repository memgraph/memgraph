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

template <typename T>
class ExpandedInterpContext : public query::InterpreterContext {
 public:
  template <typename... TArgs>
  explicit ExpandedInterpContext(T &ref, TArgs &&...args)
      : query::InterpreterContext(std::forward<TArgs>(args)...), sc_handler_(ref) {}

  T &sc_handler_;
};

struct ExpandedInterpConfig {
  query::InterpreterConfig interp_config;
  std::filesystem::path storage_dir;
};

template <typename TSCHandler>
class InterpContextHandler : public Handler<ExpandedInterpContext<TSCHandler>, ExpandedInterpConfig> {
 public:
  using InterpContextT = ExpandedInterpContext<TSCHandler>;
  using HandlerT = Handler<InterpContextT, ExpandedInterpConfig>;

  typename HandlerT::NewResult New(const std::string &name, TSCHandler &sc_handler, storage::Storage &db,
                                   const query::InterpreterConfig &config, const std::filesystem::path &dir,
                                   query::AuthQueryHandler &auth_handler, query::AuthChecker &auth_checker) {
    // Check if compatible with the existing interpreters
    if (std::any_of(HandlerT::cbegin(), HandlerT::cend(), [&](const auto &elem) {
          const auto &config = elem.second.config();
          const auto &context = *elem.second.get();
          return config.storage_dir == dir || context.db == &db;
        })) {
      // LOG
      return NewError::EXISTS;
    }
    return HandlerT::New(name, std::forward_as_tuple(config, dir),
                         std::forward_as_tuple(sc_handler, &db, config, dir, &auth_handler, &auth_checker));
  }
};

}  // namespace memgraph::dbms

#endif
