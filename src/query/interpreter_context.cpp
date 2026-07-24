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
#include <chrono>
#include <memory>
#include <utility>

#include "query/interpreter_context.hpp"

#include "flags/general.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "parameters/parameters.hpp"
#include "query/interpreter.hpp"
#include "query/query_user.hpp"
#include "spdlog/spdlog.h"

#include "system/include/system/system.hpp"
#include "utils/resource_monitoring.hpp"

namespace memgraph::query {

namespace {
// Poll cadence for the idle-in-transaction watchdog. Fixed (not a flag): thresholds are in tens of
// seconds to minutes, so 5s granularity is fine and the scan is cheap.
constexpr auto kIdleTransactionScanInterval = std::chrono::seconds(5);

// Per-transaction log cooldown so a long-idle transaction doesn't spam the log once per tick. The
// metric counter is NOT throttled -- it bumps every tick so its rate stays meaningful for alerting.
constexpr auto kIdleWarnLogCooldown = std::chrono::seconds(60);
}  // namespace

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
  // Idle-in-transaction watchdog: always scheduled (like the storage GC runner). The scan no-ops
  // when both thresholds are 0, so running it unconditionally costs nothing.
  idle_transaction_scanner_.SetInterval(kIdleTransactionScanInterval);
  idle_transaction_scanner_.Run("Idle Tx Watchdog", [this] { ScanIdleTransactions(); });
}

std::vector<std::vector<TypedValue>> InterpreterContext::TerminateTransactions(
    const std::unordered_set<Interpreter *> &interpreters, std::vector<uint64_t> maybe_kill_transaction_ids,
    QueryUserOrRole *user_or_role, std::function<bool(QueryUserOrRole *, std::string const &)> privilege_checker) {
  auto not_found_midpoint = maybe_kill_transaction_ids.end();

  // Multiple simultaneous TERMINATE TRANSACTIONS aren't allowed
  // TERMINATE and SHOW TRANSACTIONS are mutually exclusive
  for (Interpreter *interpreter : interpreters) {
    TransactionStatus alive_status = TransactionStatus::ACTIVE;
    // if it is just checking kill, commit and abort should wait for the end of the check
    // The only way to start checking if the transaction will get killed is if the transaction_status is
    // active
    if (!interpreter->transaction_status_.compare_exchange_strong(alive_status, TransactionStatus::VERIFYING)) {
      continue;
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
    if (!intr_trans) continue;

    auto transaction_id = intr_trans.value();

    auto it = std::find(maybe_kill_transaction_ids.begin(), not_found_midpoint, transaction_id);
    if (it != not_found_midpoint) {
      // update the maybe_kill_transaction_ids (partitioning not found + killed)
      --not_found_midpoint;
      std::iter_swap(it, not_found_midpoint);
      auto get_interpreter_db_name = [&]() -> std::string {
        return interpreter->current_db_.db_acc_ ? interpreter->current_db_.db_acc_->get()->name() : "";
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
    if (interpreter->current_db_.db_acc_ && interpreter->current_db_.db_acc_->get()->name() != db_name) {
      continue;
    }
    std::optional<uint64_t> transaction_id = interpreter->GetTransactionId();
    if (transaction_id) {
      results.push_back(transaction_id.value());
    }
  }
  return results;
}

void InterpreterContext::ScanIdleTransactions() {
  const uint64_t warn_threshold_sec = FLAGS_query_idle_in_transaction_warn_sec;
  const uint64_t abort_threshold_sec = FLAGS_query_idle_in_transaction_abort_sec;
  // Disabled: nothing configured, cheap early exit.
  if (warn_threshold_sec == 0 && abort_threshold_sec == 0) return;

  const auto now = std::chrono::steady_clock::now();
  std::vector<uint64_t> to_abort;
  // (transaction id, idle seconds) for transactions past the warn threshold this tick.
  std::vector<std::pair<uint64_t, int64_t>> to_warn;

  interpreters.WithLock([&](auto &interpreters_set) {
    for (Interpreter *interpreter : interpreters_set) {
      // Fast-path: skip actively-executing interpreters before paying for the verification CAS.
      if (interpreter->query_in_progress_.load(std::memory_order_acquire)) continue;

      // CAS into VERIFYING to safely read the interpreter's transaction fields from this thread
      // (same mechanism as ShowTransactionsUsingDBName).
      const auto verifier = interpreter->TryAcquireForVerification();
      if (!verifier) continue;  // no transaction alive enough to be worth watching right now

      if (!interpreter->in_explicit_transaction_) continue;  // autocommit; idle-in-tx cannot apply

      const std::optional<uint64_t> tx_id = interpreter->GetTransactionId();
      if (!tx_id) continue;

      const auto last_activity_ns = interpreter->last_activity_steady_ns_.load(std::memory_order_relaxed);
      const std::chrono::steady_clock::time_point last_activity{std::chrono::steady_clock::duration{last_activity_ns}};
      const auto idle_sec = std::chrono::duration_cast<std::chrono::seconds>(now - last_activity).count();
      if (idle_sec < 0) continue;  // clock artifact guard; nothing sane to report

      if (abort_threshold_sec > 0 && static_cast<uint64_t>(idle_sec) >= abort_threshold_sec) {
        to_abort.push_back(*tx_id);
        continue;  // abort supersedes warn for this tick
      }
      if (warn_threshold_sec > 0 && static_cast<uint64_t>(idle_sec) >= warn_threshold_sec) {
        to_warn.emplace_back(*tx_id, idle_sec);
      }
    }

    if (to_abort.empty()) return;

    // Reuse the TERMINATE TRANSACTIONS path rather than inventing a new cross-thread abort. The
    // null user + always-true privilege check make this an unconditional system termination (the
    // operator opted in via --query-idle-in-transaction-abort-sec > 0).
    auto results =
        TerminateTransactions(interpreters_set,
                              std::move(to_abort),
                              /*user_or_role=*/nullptr,
                              [](QueryUserOrRole * /*user_or_role*/, std::string const & /*db_name*/) { return true; });

    for (auto &row : results) {
      if (row.size() != 2 || !row[1].IsBool() || !row[1].ValueBool()) continue;  // not found / not killed
      metrics::Metrics().global.idle_in_transaction_aborted->Increment();
      spdlog::warn(
          "Idle-in-transaction watchdog: terminated transaction {} for exceeding the "
          "--query-idle-in-transaction-abort-sec={} threshold.",
          row[0].ValueString(),
          abort_threshold_sec);
    }
  });

  // Always run the prune block, even when to_warn is empty: the erase_if below is the only place
  // that evicts resolved tx ids from idle_warn_last_logged_, so skipping it on quiet ticks would
  // leak entries. With an empty to_warn the loop bumps no metric and logs nothing (preserving
  // "bump only on a real warn"), while erase_if drops every stale entry.
  // Metric bumps every tick (unthrottled, for alerting); the log line is throttled per tx id.
  idle_warn_last_logged_.WithLock([&](auto &last_logged) {
    for (const auto &[tx_id, idle_sec] : to_warn) {
      metrics::Metrics().global.idle_in_transaction_warnings->Increment();

      auto it = last_logged.find(tx_id);
      const bool due = it == last_logged.end() || (now - it->second) >= kIdleWarnLogCooldown;
      if (due) {
        spdlog::warn(
            "Transaction {} has been idle-in-transaction for {}s (BEGIN issued, no query "
            "currently executing); consider COMMIT/ROLLBACK. Warn threshold is "
            "--query-idle-in-transaction-warn-sec={}s.",
            tx_id,
            idle_sec,
            warn_threshold_sec);
        last_logged[tx_id] = now;
      }
    }

    // Prune ids no longer idle-past-threshold this tick so the map can't grow unbounded.
    std::erase_if(last_logged, [&](const auto &entry) {
      return std::ranges::none_of(to_warn, [&](const auto &w) { return w.first == entry.first; });
    });
  });
}
}  // namespace memgraph::query
