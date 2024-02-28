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

#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "query/config.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/replication_query_handler.hpp"
#include "query/typed_value.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/transaction.hpp"
#include "system/state.hpp"
#include "system/system.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#ifdef MG_ENTERPRISE
#include "coordination/coordinator_state.hpp"
#endif

namespace memgraph::dbms {
class DbmsHandler;
}  // namespace memgraph::dbms

namespace memgraph::query {

constexpr uint64_t kInterpreterTransactionInitialId = 1ULL << 63U;

class AuthQueryHandler;
class AuthChecker;
class Interpreter;
struct QueryUserOrRole;

/**
 * Holds data shared between multiple `Interpreter` instances (which might be
 * running concurrently).
 *
 */
struct InterpreterContext {
  static InterpreterContext *instance;

  static InterpreterContext *getInstance() {
    MG_ASSERT(instance != nullptr, "Interpreter context has not been initialized!");
    return instance;
  }

  static InterpreterContext *getInstance(InterpreterConfig interpreter_config, dbms::DbmsHandler *dbms_handler,
                                         replication::ReplicationState *rs, memgraph::system::System &system,
#ifdef MG_ENTERPRISE
                                         memgraph::coordination::CoordinatorState *coordinator_state,
#endif
                                         AuthQueryHandler *ah = nullptr, AuthChecker *ac = nullptr,
                                         ReplicationQueryHandler *replication_handler = nullptr) {
    if (instance == nullptr) {
      instance = new InterpreterContext(interpreter_config, dbms_handler, rs, system,
#ifdef MG_ENTERPRISE
                                        coordinator_state,
#endif
                                        ah, ac, replication_handler);
    }

    return instance;
  }

  memgraph::dbms::DbmsHandler *dbms_handler;

  // Internal
  const InterpreterConfig config;
  std::atomic<bool> is_shutting_down{false};  // TODO: Do we even need this, since there is a global one also
  memgraph::utils::SkipList<QueryCacheEntry> ast_cache;

  // GLOBAL
  memgraph::replication::ReplicationState *repl_state;
#ifdef MG_ENTERPRISE
  memgraph::coordination::CoordinatorState *coordinator_state_;
#endif

  AuthQueryHandler *auth;
  AuthChecker *auth_checker;
  ReplicationQueryHandler *replication_handler_;
  system::System *system_;

  // Used to check active transactions
  // TODO: Have a way to read the current database
  memgraph::utils::Synchronized<std::unordered_set<Interpreter *>, memgraph::utils::SpinLock> interpreters;

  struct {
    auto next() -> uint64_t { return transaction_id++; }

   private:
    std::atomic<uint64_t> transaction_id = kInterpreterTransactionInitialId;
  } id_handler;

  /// Function that is used to tell all active interpreters that they should stop
  /// their ongoing execution.
  void Shutdown() { is_shutting_down.store(true, std::memory_order_release); }

  std::vector<std::vector<TypedValue>> TerminateTransactions(
      std::vector<std::string> maybe_kill_transaction_ids, QueryUserOrRole *user_or_role,
      std::function<bool(QueryUserOrRole *, std::string const &)> privilege_checker);

 private:
  InterpreterContext(InterpreterConfig interpreter_config, dbms::DbmsHandler *dbms_handler,
                     replication::ReplicationState *rs, memgraph::system::System &system,
#ifdef MG_ENTERPRISE
                     memgraph::coordination::CoordinatorState *coordinator_state,
#endif
                     AuthQueryHandler *ah = nullptr, AuthChecker *ac = nullptr,
                     ReplicationQueryHandler *replication_handler = nullptr);
};
}  // namespace memgraph::query
