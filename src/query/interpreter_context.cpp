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

#include "query/interpreter_context.hpp"

#include "query/interpreter.hpp"
#include "system/include/system/system.hpp"
namespace memgraph::query {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
InterpreterContext *InterpreterContext::instance = nullptr;

InterpreterContext::InterpreterContext(
    InterpreterConfig interpreter_config, dbms::DbmsHandler *dbms_handler, replication::ReplicationState *rs,
    memgraph::system::System &system,
#ifdef MG_ENTERPRISE
    std::optional<std::reference_wrapper<memgraph::coordination::CoordinatorState>> const &coordinator_state,
#endif
    AuthQueryHandler *ah, AuthChecker *ac, ReplicationQueryHandler *replication_handler)
    : dbms_handler(dbms_handler),
      config(interpreter_config),
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
      TransactionStatus alive_status = TransactionStatus::ACTIVE;
      // if it is just checking kill, commit and abort should wait for the end of the check
      // The only way to start checking if the transaction will get killed is if the transaction_status is
      // active
      if (!interpreter->transaction_status_.compare_exchange_strong(alive_status, TransactionStatus::VERIFYING)) {
        continue;
      }
      bool killed = false;
      utils::OnScopeExit clean_status([interpreter, &killed]() {
        if (killed) {
          interpreter->transaction_status_.store(TransactionStatus::TERMINATED, std::memory_order_release);
        } else {
          interpreter->transaction_status_.store(TransactionStatus::ACTIVE, std::memory_order_release);
        }
      });
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

        auto same_user = [](const auto &lv, const auto &rv) {
          if (lv.get() == rv) return true;
          if (lv && rv) return *lv == *rv;
          return false;
        };

        if (same_user(interpreter->user_or_role_, user_or_role) ||
            privilege_checker(user_or_role, get_interpreter_db_name())) {
          killed = true;  // Note: this is used by the above `clean_status` (OnScopeExit)
          spdlog::warn("Transaction {} successfully killed", transaction_id);
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
