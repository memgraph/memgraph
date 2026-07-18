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
// Poll cadence for the idle-in-transaction watchdog. Deliberately a fixed constant rather than
// another flag: the warn/abort thresholds are configured in tens of seconds to minutes, so a 5s
// granularity is more than fine, and the scan itself is a cheap walk of `interpreters` guarded by
// per-interpreter atomics -- no need to make the poll interval independently tunable.
constexpr auto kIdleTransactionScanInterval = std::chrono::seconds(5);

// Log lines for a still-idle transaction are throttled to at most once per this cooldown so a
// transaction stuck idle for hours doesn't spam the log once per scan tick. The metric counter
// (global.idle_in_transaction_warnings) is NOT throttled -- it increments every tick so its rate
// stays meaningful for alerting.
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
  // Idle-in-transaction watchdog: always scheduled (mirrors the storage GC runner pattern of
  // starting a periodic utils::Scheduler task from the owning object's constructor). The scan
  // itself is cheap and no-ops immediately if both thresholds are 0, so there is no cost to
  // leaving this on unconditionally rather than gating construction on a flag.
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
  // Fully disabled: neither warn-tracking nor abort is configured. Cheap early exit so an
  // instance that doesn't want the watchdog pays essentially nothing for the scheduler tick.
  if (warn_threshold_sec == 0 && abort_threshold_sec == 0) return;

  const auto now = std::chrono::steady_clock::now();
  std::vector<uint64_t> to_abort;
  // (transaction id, idle seconds) for transactions past the warn threshold this tick; used below
  // to decide whether the log line is due (rate-limited) or just the metric bump.
  std::vector<std::pair<uint64_t, int64_t>> to_warn;

  interpreters.WithLock([&](auto &interpreters_set) {
    for (Interpreter *interpreter : interpreters_set) {
      // Cheap fast-path: skip interpreters actively executing a statement before paying for the
      // TryAcquireForVerification() CAS below. query_in_progress_ is a plain atomic, safe to read
      // from any thread.
      if (interpreter->query_in_progress_.load(std::memory_order_acquire)) continue;

      // Same reused mechanism ShowTransactionsUsingDBName uses to safely read this interpreter's
      // otherwise-unsynchronized transaction fields (in_explicit_transaction_ below) from another
      // thread: CAS the transaction into VERIFYING for the duration of the read.
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

    // Reuse the exact TERMINATE TRANSACTIONS path -- the same transaction_status_ CAS both the
    // owning session thread and TERMINATE TRANSACTIONS already contend on -- instead of inventing
    // a new cross-thread abort. Passing user_or_role=nullptr with an always-true privilege
    // checker makes this an unconditional system-initiated termination: the operator already
    // opted in by setting --query-idle-in-transaction-abort-sec > 0, so no further per-user
    // authorization is meaningful here.
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

  if (to_warn.empty()) return;

  // Metric: bump once per idle transaction per tick (unthrottled -- its rate is exactly "how many
  // idle-transaction-ticks were observed", useful for alerting). Log: throttled per transaction
  // id so a session stuck idle for hours logs once per cooldown window, not once per 5s tick.
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

    // Prune entries for transactions that are no longer idle-past-threshold this tick (resolved,
    // or activity resumed) so this map never grows unbounded over the process lifetime.
    std::erase_if(last_logged, [&](const auto &entry) {
      return std::ranges::none_of(to_warn, [&](const auto &w) { return w.first == entry.first; });
    });
  });
}
}  // namespace memgraph::query
