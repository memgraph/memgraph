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
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "query/interpreter_context.hpp"

#include "communication/v2/session_registry.hpp"
#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "parameters/parameters.hpp"
#include "query/interpreter.hpp"
#include "query/query_user.hpp"

#include "system/include/system/system.hpp"
#include "utils/resource_monitoring.hpp"

namespace memgraph::query {

bool SameUser(const std::shared_ptr<QueryUserOrRole> &lv, QueryUserOrRole *rv) {
  if (lv.get() == rv) return true;
  if (lv && rv) return *lv == *rv;
  return false;
}

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

#ifdef MG_ENTERPRISE
// A FORCE-dropped database keeps draining while any session still has it as its current database, and an idle
// pooled connection may never send the query that would release it, so such sessions are closed. A session with a
// live transaction is left alone so the transaction runs to completion (a replica never aborts it); it releases the
// database with its first query after the transaction ends, or is closed on a later tick once idle. A transaction
// MAIN's FORCE drop already terminated does not protect its session.
void CloseSessionsOnDroppingDatabases(InterpreterSet &interpreters) {
  std::vector<std::string> to_close;
  interpreters.WithLock([&to_close](auto const &all) {
    for (auto *interpreter : all) {
      auto const status = interpreter->transaction_status_.load(std::memory_order_acquire);
      if (status != TransactionStatus::IDLE && status != TransactionStatus::TERMINATED) continue;
      if (!interpreter->current_db_.foreign_db_view().marked_for_deletion) continue;
      auto const session = interpreter->foreign_session_view_.load(std::memory_order_acquire);
      if (session && !session->uuid.empty()) to_close.push_back(session->uuid);
    }
  });
  // Closing re-enters interpreters from the session's teardown, so it happens only after the lock is released.
  for (auto const &uuid : to_close) {
    if (auto session = communication::v2::SessionRegistry::Instance().Find(uuid)) session->RequestTermination();
  }
}
#endif

/// Foreign VERIFYING pins are only taken under `interpreters`, so concurrent SHOW/TERMINATE statements never
/// observe each other's pins.
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

/// What decides whether a caller may kill a target: owning the target, or holding the privilege on its database.
struct TargetOwner {
  bool same_user{false};
  std::string db_name;

  bool SameAuthorization(TargetOwner const &o) const {
    return same_user == o.same_user && (same_user || db_name == o.db_name);
  }
};

/// Call with `interpreters` locked. user_or_role_ is owning-thread state; foreign_user_view_.load() snapshots it
/// safely. foreign_db_view() (not name()) because neither the VERIFYING CAS nor an IDLE target orders against
/// SetCurrentDB, so a raw name() could tear against a concurrent USE DATABASE.
TargetOwner ReadOwner(Interpreter const *interpreter, QueryUserOrRole *user_or_role, bool dbless_falls_back) {
  TargetOwner owner;
  owner.same_user = SameUser(interpreter->foreign_user_view_.load(std::memory_order_acquire), user_or_role);
  owner.db_name = interpreter->current_db_.foreign_db_view().name;
  if (dbless_falls_back && owner.db_name.empty()) owner.db_name = std::string{dbms::kDefaultDB};
  return owner;
}

bool Authorized(TargetOwner const &owner, PrivilegeByDb &privilege_by_db) {
  return owner.same_user || privilege_by_db(owner.db_name);
}

/// Phase 1: ACTIVE transactions (all, or only `wanted`) with their owner, read under the ACTIVE->VERIFYING pin.
std::unordered_map<uint64_t, TargetOwner> SnapshotTransactions(InterpreterSet &interpreters, Interpreter const *self,
                                                               std::unordered_set<uint64_t> const *wanted,
                                                               QueryUserOrRole *user_or_role) {
  std::unordered_map<uint64_t, TargetOwner> snapshot;
  interpreters.WithLock([&](auto const &all) {
    for (Interpreter *interpreter : all) {
      if (interpreter == self) continue;
      TryTerminateInterpreter(interpreter, [&](uint64_t id) {
        if (!wanted || wanted->contains(id)) snapshot.emplace(id, ReadOwner(interpreter, user_or_role, false));
        return false;
      });
    }
  });
  return snapshot;
}

/// Phases 2-3 shared by both transaction statements. Returns the ids actually killed.
std::unordered_set<uint64_t> KillAuthorizedTransactions(InterpreterSet &interpreters, Interpreter const *self,
                                                        std::unordered_set<uint64_t> const *wanted,
                                                        QueryUserOrRole *user_or_role,
                                                        PrivilegeChecker const &privilege_checker) {
  auto const snapshot = SnapshotTransactions(interpreters, self, wanted, user_or_role);

  PrivilegeByDb privilege_by_db{user_or_role, privilege_checker};
  std::unordered_map<uint64_t, TargetOwner> authorized;
  for (auto const &[id, owner] : snapshot) {
    if (Authorized(owner, privilege_by_db)) {
      authorized.emplace(id, owner);
    } else {
      spdlog::warn("Not enough rights to kill the transaction");
    }
  }

  std::unordered_set<uint64_t> killed;
  if (authorized.empty()) return killed;
  interpreters.WithLock([&](auto const &all) {
    for (Interpreter *interpreter : all) {
      if (interpreter == self) continue;
      TryTerminateInterpreter(interpreter, [&](uint64_t transaction_id) {
        auto const it = authorized.find(transaction_id);
        if (it == authorized.end()) return false;
        // Re-read under the pin: the owner may have changed user or database since the snapshot.
        if (!ReadOwner(interpreter, user_or_role, false).SameAuthorization(it->second)) return false;
        killed.insert(transaction_id);
        return true;
      });
    }
  });
  return killed;
}

}  // namespace

std::vector<std::vector<TypedValue>> InterpreterContext::TerminateTransactions(
    InterpreterSet &interpreters, std::vector<uint64_t> maybe_kill_transaction_ids, QueryUserOrRole *user_or_role,
    PrivilegeChecker const &privilege_checker) {
  std::unordered_set<uint64_t> const wanted(maybe_kill_transaction_ids.begin(), maybe_kill_transaction_ids.end());
  auto killed = KillAuthorizedTransactions(interpreters, nullptr, &wanted, user_or_role, privilege_checker);

  // An unauthorized match reports killed=false like a missing id, so its existence isn't leaked. For a duplicated
  // id only the first occurrence is the killed one. Not-killed rows come first, then killed rows, each in input order.
  std::vector<std::vector<TypedValue>> not_killed_rows;
  std::vector<std::vector<TypedValue>> killed_rows;
  for (auto const id : maybe_kill_transaction_ids) {
    if (killed.erase(id) != 0) {
      killed_rows.push_back({TypedValue(std::to_string(id)), TypedValue(true)});
      spdlog::warn("Transaction {} successfully killed", id);
    } else {
      not_killed_rows.push_back({TypedValue(std::to_string(id)), TypedValue(false)});
      spdlog::warn("Transaction {} not found", id);
    }
  }

  auto results = std::move(not_killed_rows);
  results.reserve(results.size() + killed_rows.size());
  std::ranges::move(killed_rows, std::back_inserter(results));
  return results;
}

std::vector<std::vector<TypedValue>> InterpreterContext::TerminateAllTransactions(
    InterpreterSet &interpreters, Interpreter const *self, QueryUserOrRole *user_or_role,
    PrivilegeChecker const &privilege_checker) {
  // Terminating the issuing transaction would make its own commit throw, so the caller would
  // never see which transactions it killed.
  auto const killed = KillAuthorizedTransactions(interpreters, self, nullptr, user_or_role, privilege_checker);

  // Ids are handed out monotonically, so ascending id is oldest transaction first. Sort the
  // numbers rather than the formatted strings, which would order lexicographically.
  std::vector<uint64_t> killed_transaction_ids(killed.begin(), killed.end());
  std::ranges::sort(killed_transaction_ids);

  std::vector<std::vector<TypedValue>> results;
  results.reserve(killed_transaction_ids.size());
  for (auto const transaction_id : killed_transaction_ids) {
    results.push_back({TypedValue(std::to_string(transaction_id)), TypedValue(true)});
    spdlog::warn("Transaction {} successfully killed", transaction_id);
  }

  return results;
}

TerminateSessionsResult InterpreterContext::TerminateSessions(InterpreterSet &interpreters,
                                                              const std::vector<std::string> &session_ids,
                                                              QueryUserOrRole *user_or_role,
                                                              PrivilegeChecker const &privilege_checker,
                                                              std::string_view caller_session_uuid) {
  TerminateSessionsResult result;
  result.rows.reserve(session_ids.size());

  std::unordered_set<std::string> requested;
  for (auto const &id : session_ids) {
    if (!id.empty() && id != caller_session_uuid) requested.insert(id);
  }

  // Phase 1: the targets by uuid. The shared_ptr keeps the login's SessionInfo alive so phase 3 can compare identity.
  struct Target {
    std::shared_ptr<const Interpreter::SessionInfo> session;
    TargetOwner owner;
  };

  std::unordered_map<std::string, Target> snapshot;
  interpreters.WithLock([&](auto const &all) {
    for (Interpreter *interpreter : all) {
      // A null snapshot means SetSessionInfo has not run yet (unauthenticated), so it cannot carry a non-empty uuid.
      auto session = interpreter->foreign_session_view_.load(std::memory_order_acquire);
      if (!session || session->uuid.empty() || !requested.contains(session->uuid)) continue;
      // A dbless session has no tenant; kDefaultDB lets a default-db admin still terminate it.
      auto owner = ReadOwner(interpreter, user_or_role, true);
      auto uuid = session->uuid;
      snapshot.emplace(std::move(uuid), Target{std::move(session), std::move(owner)});
    }
  });

  // Phase 2: authorize without the lock. Rule order and logs are per input id.
  PrivilegeByDb privilege_by_db{user_or_role, privilege_checker};
  std::unordered_set<std::string> accepted_for_kill;
  std::vector<std::optional<size_t>> accepted_row(session_ids.size());
  for (size_t i = 0; i < session_ids.size(); ++i) {
    auto const &id = session_ids[i];
    // A connection is registered into `interpreters` before authentication completes, so a mid-handshake
    // session carries an empty uuid; without this guard an empty id would match every such session at once.
    if (id.empty()) {
      result.rows.push_back({TypedValue(id), TypedValue(false)});
      continue;
    }

    // Refuse to sever the caller's own control channel mid-statement -- the response couldn't be delivered
    // afterwards anyway. Reported as a row (not silently dropped) so the refusal is visible.
    if (id == caller_session_uuid) {
      result.rows.push_back({TypedValue(id), TypedValue(false)});
      spdlog::warn("Cannot terminate the session that issued the command");
      continue;
    }

    // Duplicate: uuid was already accepted for kill earlier in this same statement. Keep one row per
    // input id (1:1 contract), but the repeat occurrence reports killed=false.
    if (accepted_for_kill.contains(id)) {
      result.rows.push_back({TypedValue(id), TypedValue(false)});
      continue;
    }

    auto const it = snapshot.find(id);
    if (it == snapshot.end()) {
      result.rows.push_back({TypedValue(id), TypedValue(false)});
      spdlog::warn("Session {} not found", id);
      continue;
    }

    if (!Authorized(it->second.owner, privilege_by_db)) {
      result.rows.push_back({TypedValue(id), TypedValue(false)});
      spdlog::warn("Not enough rights to kill the session");
      continue;
    }

    accepted_for_kill.insert(id);
    accepted_row[i] = result.rows.size();
    result.rows.push_back({TypedValue(id), TypedValue(true)});
  }

  // Phase 3: authorization was evaluated without the lock, so the kill only proceeds if the target is still the same
  // login (every login publishes a fresh SessionInfo) with the same authorization key. The later RequestTermination
  // is still by uuid.
  std::unordered_set<std::string> confirmed;
  if (!accepted_for_kill.empty()) {
    interpreters.WithLock([&](auto const &all) {
      for (Interpreter *interpreter : all) {
        auto const session = interpreter->foreign_session_view_.load(std::memory_order_acquire);
        if (!session || !accepted_for_kill.contains(session->uuid)) continue;
        auto const &target = snapshot.at(session->uuid);
        if (session.get() != target.session.get()) continue;
        if (!ReadOwner(interpreter, user_or_role, true).SameAuthorization(target.owner)) continue;

        TransactionStatus alive_status = TransactionStatus::ACTIVE;
        if (interpreter->transaction_status_.compare_exchange_strong(alive_status, TransactionStatus::VERIFYING)) {
          interpreter->transaction_status_.store(TransactionStatus::TERMINATED, std::memory_order_release);
        }
        // Unlike TerminateTransactions, a failed CAS here must NOT skip termination -- the primary target of this
        // feature is an IDLE session, for which the CAS above is expected to fail.
        // Mid-commit case: CAS fails because status is STARTED_COMMITTING (not ACTIVE) -- intentionally left to
        // complete on the owning thread; we terminate the session, not the in-flight commit.
        confirmed.insert(session->uuid);
      }
    });
  }

  for (size_t i = 0; i < session_ids.size(); ++i) {
    if (!accepted_row[i]) continue;
    auto const &id = session_ids[i];
    if (confirmed.contains(id)) {
      result.to_close.push_back(id);
      spdlog::warn("Session {} successfully killed", id);
    } else {
      result.rows[*accepted_row[i]][1] = TypedValue(false);
      spdlog::warn("Session {} not found", id);
    }
  }

  return result;
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
    // Foreign thread: route through foreign_db_view() (takes db_acc_mutex_). The verifier CAS does NOT
    // order against SetCurrentDB, so the unlocked db_acc_/name() read could tear against USE DATABASE.
    // A no-DB interpreter (empty view name) deliberately passes this filter: the caller uses this list as a
    // DROP DATABASE ... FORCE kill-list, and a no-DB interpreter must stay a termination candidate.
    // db_name is always a real, non-empty database name, so an empty view name means "no current DB".
    auto const view = interpreter->current_db_.foreign_db_view();
    if (!view.name.empty() && view.name != db_name) {
      continue;
    }
    std::optional<uint64_t> transaction_id = interpreter->GetTransactionId();
    if (transaction_id) {
      results.push_back(transaction_id.value());
    }
  }
  return results;
}

void InterpreterContext::RegisterDropDrainHook() {
#ifdef MG_ENTERPRISE
  if (dbms_handler) dbms_handler->SetDrainHook([this] { CloseSessionsOnDroppingDatabases(interpreters); });
#endif
}

void InterpreterContext::UnregisterDropDrainHook() const {
#ifdef MG_ENTERPRISE
  if (dbms_handler) dbms_handler->ClearDrainHook();
#endif
}

}  // namespace memgraph::query
