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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "query/interpreter_context.hpp"

#include "parameters/parameters.hpp"
#include "query/interpreter.hpp"
#include "query/query_user.hpp"

#include "system/include/system/system.hpp"
#include "utils/resource_monitoring.hpp"

namespace memgraph::query {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::optional<InterpreterContext> InterpreterContextHolder::instance{};

InterpreterContext::InterpreterContext(InterpreterConfig interpreter_config, memgraph::utils::Settings *settings,
                                       memgraph::parameters::Parameters *parameters, dbms::DbmsHandler *dbms_handler,
                                       utils::Synchronized<replication::ReplicationState, utils::RWSpinLock> *rs,
                                       memgraph::system::System &system,
                                       communication::ServerContext *bolt_server_context,
#ifdef MG_ENTERPRISE
                                       coordination::CoordinatorState *coordinator_state,
                                       utils::ResourceMonitoring *resource_monitoring,
#endif
                                       AuthQueryHandler *ah, AuthChecker *ac,
                                       ReplicationQueryHandler *replication_handler,
                                       utils::PriorityThreadPool *worker_pool)
    : settings(settings),
      parameters(parameters),
      dbms_handler(dbms_handler),
      config(std::move(interpreter_config)),
      ast_cache{static_cast<std::size_t>(FLAGS_query_ast_cache_max_size)},
      repl_state(rs),
#ifdef MG_ENTERPRISE
      coordinator_state_(coordinator_state),
      resource_monitoring(resource_monitoring),
#endif
      auth(ah),
      auth_checker(ac),
      replication_handler_{replication_handler},
      system_{&system},
      bolt_server_context_(bolt_server_context),
      worker_pool(worker_pool) {
}

namespace {

/// Pins `interpreter`'s transaction so it can neither commit nor abort, hands its id to
/// `should_kill`, and marks it TERMINATED if the predicate accepts. Only an ACTIVE
/// transaction can be pinned, so one already committing, aborting or terminated is left
/// alone. Returns whether the transaction was terminated.
template <typename ShouldKill>
bool TryTerminateInterpreter(Interpreter *interpreter, ShouldKill &&should_kill) {
  TransactionStatus alive_status = TransactionStatus::ACTIVE;
  // if it is just checking kill, commit and abort should wait for the end of the check
  // The only way to start checking if the transaction will get killed is if the transaction_status is
  // active
  if (!interpreter->transaction_status_.compare_exchange_strong(alive_status, TransactionStatus::VERIFYING)) {
    return false;
  }
  bool killed = false;
  const utils::OnScopeExit clean_status([interpreter, &killed]() {
    if (killed) {
      interpreter->transaction_status_.store(TransactionStatus::TERMINATED, std::memory_order_release);
    } else {
      interpreter->transaction_status_.store(TransactionStatus::ACTIVE, std::memory_order_release);
    }
  });
  std::optional<uint64_t> intr_trans = interpreter->GetTransactionId();
  if (!intr_trans) return false;

  killed = should_kill(*intr_trans);  // Note: this is used by the above `clean_status` (OnScopeExit)
  return killed;
}

/// A caller may kill a transaction it owns, or any transaction on a database it holds
/// TRANSACTION_MANAGEMENT for.
bool MayTerminate(Interpreter const *interpreter, QueryUserOrRole *user_or_role,
                  std::function<bool(QueryUserOrRole *, std::string const &)> const &privilege_checker) {
  auto same_user = [](const auto &lv, const auto &rv) {
    if (lv.get() == rv) return true;
    if (lv && rv) return *lv == *rv;
    return false;
  };
  if (same_user(interpreter->user_or_role_, user_or_role)) return true;

  // Route through CurrentDB::name() -- the single definition of "this session's database" --
  // so a later change to what that means can't miss this site.
  auto const db_name = interpreter->current_db_.name();
  return privilege_checker(user_or_role, db_name);
}

}  // namespace

std::vector<std::vector<TypedValue>> InterpreterContext::TerminateTransactions(
    const std::unordered_set<Interpreter *> &interpreters, std::vector<uint64_t> maybe_kill_transaction_ids,
    QueryUserOrRole *user_or_role, std::function<bool(QueryUserOrRole *, std::string const &)> privilege_checker) {
  auto not_found_midpoint = maybe_kill_transaction_ids.end();

  // Multiple simultaneous TERMINATE TRANSACTIONS aren't allowed
  // TERMINATE and SHOW TRANSACTIONS are mutually exclusive
  for (Interpreter *interpreter : interpreters) {
    TryTerminateInterpreter(interpreter, [&](uint64_t transaction_id) {
      auto it = std::find(maybe_kill_transaction_ids.begin(), not_found_midpoint, transaction_id);
      if (it == not_found_midpoint) return false;

      if (!MayTerminate(interpreter, user_or_role, privilege_checker)) {
        spdlog::warn("Not enough rights to kill the transaction");
        return false;
      }
      // Only authorized kills join the killed partition. An unauthorized match stays in the
      // not-found partition so it reports killed=false and its existence isn't leaked.
      --not_found_midpoint;
      std::iter_swap(it, not_found_midpoint);
      spdlog::warn("Transaction {} successfully killed", transaction_id);
      return true;
    });
  }

  std::vector<std::vector<TypedValue>> results;
  for (auto it = maybe_kill_transaction_ids.begin(); it != not_found_midpoint; ++it) {
    results.push_back({TypedValue(std::to_string(*it)), TypedValue(false)});
    spdlog::warn("Transaction {} not found", *it);
  }
  for (auto it = not_found_midpoint; it != maybe_kill_transaction_ids.end(); ++it) {
    results.push_back({TypedValue(std::to_string(*it)), TypedValue(true)});
  }

  return results;
}

std::vector<std::vector<TypedValue>> InterpreterContext::TerminateAllTransactions(
    const std::unordered_set<Interpreter *> &interpreters, Interpreter const *self, QueryUserOrRole *user_or_role,
    std::function<bool(QueryUserOrRole *, std::string const &)> privilege_checker) {
  std::vector<uint64_t> killed_transaction_ids;

  for (Interpreter *interpreter : interpreters) {
    // Terminating the issuing transaction would make its own commit throw, so the caller would
    // never see which transactions it killed.
    if (interpreter == self) continue;

    TryTerminateInterpreter(interpreter, [&](uint64_t transaction_id) {
      if (!MayTerminate(interpreter, user_or_role, privilege_checker)) {
        spdlog::warn("Not enough rights to kill the transaction");
        return false;
      }
      killed_transaction_ids.push_back(transaction_id);
      spdlog::warn("Transaction {} successfully killed", transaction_id);
      return true;
    });
  }

  // Ids are handed out monotonically, so ascending id is oldest transaction first. Sort the
  // numbers rather than the formatted strings, which would order lexicographically.
  std::ranges::sort(killed_transaction_ids);

  std::vector<std::vector<TypedValue>> results;
  results.reserve(killed_transaction_ids.size());
  for (auto const transaction_id : killed_transaction_ids) {
    results.push_back({TypedValue(std::to_string(transaction_id)), TypedValue(true)});
  }

  return results;
}

std::vector<uint64_t> InterpreterContext::ShowTransactionsUsingDBName(
    const std::unordered_set<Interpreter *> &interpreters, std::string_view db_name) {
  std::vector<uint64_t> results;
  results.reserve(interpreters.size());
  for (Interpreter *interpreter : interpreters) {
    const auto verifier = interpreter->TryAcquireForVerification();
    if (!verifier) {
      continue;
    }
    // Transaction is running, so cannot change the underlying db
    // No current DB (db_acc_ null) deliberately passes this filter: the caller uses this list as a
    // DROP DATABASE ... FORCE kill-list, and a no-DB interpreter must stay a termination candidate.
    if (interpreter->current_db_.db_acc_ && interpreter->current_db_.name() != db_name) {
      continue;
    }
    std::optional<uint64_t> transaction_id = interpreter->GetTransactionId();
    if (transaction_id) {
      results.push_back(transaction_id.value());
    }
  }
  return results;
}
}  // namespace memgraph::query
