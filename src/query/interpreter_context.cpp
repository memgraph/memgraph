// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <memory>
#include <utility>

#include "query/interpreter_context.hpp"

#include "query/interpreter.hpp"

#include "system/include/system/system.hpp"
namespace memgraph::query {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::optional<InterpreterContext> InterpreterContextHolder::instance{};

InterpreterContext::InterpreterContext(
    InterpreterConfig interpreter_config, dbms::DbmsHandler *dbms_handler,
    utils::Synchronized<replication::ReplicationState, utils::RWSpinLock> &rs, memgraph::system::System &system,
#ifdef MG_ENTERPRISE
    std::optional<std::reference_wrapper<memgraph::coordination::CoordinatorState>> const &coordinator_state,
#endif
    AuthQueryHandler *ah, AuthChecker *ac, ReplicationQueryHandler *replication_handler)
    : dbms_handler(dbms_handler),
      config(std::move(interpreter_config)),
      repl_state(rs),
#ifdef MG_ENTERPRISE
      coordinator_state_(coordinator_state),
#endif
      auth(ah),
      auth_checker(ac),
      replication_handler_{replication_handler},
      system_{&system} {
}

std::vector<std::vector<TypedValue>> InterpreterContext::TerminateTransactions(
    std::vector<std::string> maybe_kill_transaction_ids, QueryUserOrRole *user_or_role,
    std::function<bool(QueryUserOrRole *, std::string const &)> privilege_checker) {
  auto not_found_midpoint = maybe_kill_transaction_ids.end();

  // Multiple simultaneous TERMINATE TRANSACTIONS aren't allowed
  // TERMINATE and SHOW TRANSACTIONS are mutually exclusive
  interpreters.WithLock([&not_found_midpoint, &maybe_kill_transaction_ids, user_or_role,
                         privilege_checker = std::move(privilege_checker)](const auto &interpreters) {
    for (Interpreter *interpreter : interpreters) {
      // Quick check to skip any transactions which are already flagged for
      // termination, or which have began to commit or rollback.
      if (interpreter->transaction_status_.load(std::memory_order_acquire) != TransactionStatus::ACTIVE) {
        continue;
      }

      std::lock_guard const lg{interpreter->transaction_info_lock_};
      std::optional<uint64_t> intr_trans = interpreter->GetTransactionId();
      if (!intr_trans.has_value()) continue;

      auto transaction_id = std::to_string(intr_trans.value());

      auto it = std::find(maybe_kill_transaction_ids.begin(), not_found_midpoint, transaction_id);
      if (it != not_found_midpoint) {
        // update the maybe_kill_transaction_ids (partitioning not found + killed)
        --not_found_midpoint;
        std::iter_swap(it, not_found_midpoint);
        auto get_interpreter_db_name = [&]() -> std::string const & {
          static std::string all;
          return interpreter->current_db_.db_acc_ ? interpreter->current_db_.db_acc_->get()->name() : all;
        };

        auto same_user = [](const auto &lv, const auto &rv) { return (lv.get() == rv) || (lv && rv && *lv == *rv); };

        if (same_user(interpreter->user_or_role_, user_or_role) ||
            privilege_checker(user_or_role, get_interpreter_db_name())) {
          if (interpreter->transaction_status_.exchange(TransactionStatus::TERMINATED, std::memory_order_release) ==
              TransactionStatus::ACTIVE) {
            spdlog::warn("Transaction {} successfully killed", transaction_id);
          }
        } else {
          spdlog::warn("Not enough rights to kill the transaction");
        }
      }
    }
  });

  std::vector<std::vector<TypedValue>> results;
  for (auto it = maybe_kill_transaction_ids.begin(); it != not_found_midpoint; ++it) {
    results.push_back({TypedValue(*it), TypedValue(false)});
    spdlog::warn("Transaction {} not found", *it);
  }
  for (auto it = not_found_midpoint; it != maybe_kill_transaction_ids.end(); ++it) {
    results.push_back({TypedValue(*it), TypedValue(true)});
  }

  return results;
}
}  // namespace memgraph::query
