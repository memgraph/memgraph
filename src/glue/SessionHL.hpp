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
#pragma once

#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "glue/SessionContext.hpp"
#include "query/interpreter.hpp"

namespace memgraph::glue {
using bolt_value_t = memgraph::communication::bolt::Value;
using bolt_map_t = memgraph::communication::bolt::map_t;

// Forward declaration
class SessionHL;

struct ParseRes {
  query::Interpreter::ParseRes parsed_query;
  query::UserParameters_fn get_params_pv;
  query::QueryExtras extra;
};

#ifdef MG_ENTERPRISE
class RuntimeConfig {
 public:
  explicit RuntimeConfig(SessionHL *session) : session_(session) {}

  void Configure(const bolt_map_t &run_time_info, bool in_explicit_tx);

  bool db_explicit_ = false;
  bool user_explicit_ = false;

 private:
  SessionHL *session_;
  std::optional<bolt_map_t> previous_run_time_info_;
};
#endif

class SessionHL final : public memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                                                      memgraph::communication::v2::OutputStream> {
 public:
  SessionHL(Context context, memgraph::communication::v2::InputStream *input_stream,
            memgraph::communication::v2::OutputStream *output_stream);

  ~SessionHL();

  SessionHL(const SessionHL &) = delete;
  SessionHL &operator=(const SessionHL &) = delete;
  SessionHL(SessionHL &&) = delete;
  SessionHL &operator=(SessionHL &&) = delete;

#ifdef MG_ENTERPRISE
  // Friend classes to allow access to private members
  friend class RuntimeConfig;
#endif

  /// BOLT level API ///

  void Configure(const bolt_map_t &run_time_info);

  void BeginTransaction(const bolt_map_t &extra);

  void CommitTransaction();

  void RollbackTransaction();

  void InterpretParse(const std::string &query, bolt_map_t params, const bolt_map_t &extra);

  std::pair<std::vector<std::string>, std::optional<int>> InterpretPrepare();

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(const std::string &query, const bolt_map_t &params,
                                                                    const bolt_map_t &extra) {
    // Interpret has been split in two (Parse and Prepare)
    // This allows us to Parse, deduce the priority and then schedule accordingly
    // Leaving this one-shot version for back-compatiblity
    InterpretParse(query, params, extra);
    return InterpretPrepare();
  }

#ifdef MG_ENTERPRISE
  auto Route(bolt_map_t const &routing, std::vector<bolt_value_t> const &bookmarks,
             std::optional<std::string> const &db, bolt_map_t const &extra) -> bolt_map_t;
#endif

  /// Sync Pull — existing inline path.  Called by the Bolt handler directly for non-coro queries
  /// and by FinishPull internally.  The parked out-param is forwarded into Interpreter::Pull;
  /// it is set to true if the pull parked (d2+), left false (d1: parks are dormant).
  ///
  /// @param event_parked  c3.0 out-param.  If non-null and the pull returns EventParked
  ///                      (event-kind park via ProgressAwaitable), *event_parked is set true.
  ///                      The caller (pull-task body) must NOT self-reschedule in this case;
  ///                      NotifyProgress will re-enqueue.  When *parked is true but
  ///                      *event_parked is false the park is yield-kind: self-reschedule applies.
  bolt_map_t Pull(std::optional<int> n, std::optional<int> qid, bool *parked = nullptr, bool *event_parked = nullptr);

  /// Returns true iff the query identified by @p qid (or the last query when nullopt) was
  /// prepared with a Coro-mode root cursor.  Forwards to Interpreter::IsCursorCoroDriven().
  /// Called by the Bolt dispatch layer (d1b) to choose between the inline sync path and a
  /// resumable pool task.
  bool IsPullCoroDriven(std::optional<int> qid) const;

  // ── d1b: pull-task dispatch plumbing ─────────────────────────────────────────────────────────
  //
  // When HandlePullDiscard detects a coro pull it does NOT call Pull inline.  Instead it stashes
  // the (n, qid, is_pull) triple on SessionHL and sets pull_task_dispatched_.  DoWork sees the
  // flag, dispatches a resumable pool task, and stops the decode loop.  The pull-task re-arms
  // DoWork on completion.  Sequential per session is guaranteed by pull_in_flight_ in v2::Session.

  /// Stash for a pending coro pull dispatched by HandlePullDiscard.
  struct PendingPull {
    std::optional<int> n;
    std::optional<int> qid;
    bool is_pull{true};  ///< true = PULL, false = DISCARD (DISCARD never coro, but wire for symmetry)
  };

  /// Called by HandlePullDiscard when a coro pull is detected: stash the pull params.
  void StashPendingPull(std::optional<int> n, std::optional<int> qid, bool is_pull) noexcept {
    pending_pull_ = PendingPull{n, qid, is_pull};
  }

  /// Called by HandlePullDiscard immediately after StashPendingPull to signal DoWork to dispatch.
  void SetPullTaskDispatched() noexcept { pull_task_dispatched_ = true; }

  /// Read by the Execute_ decode-loop guard and by DoWork.  Non-atomic: both callers are on the
  /// same pool task (the one running Execute_/DoWork), so no data race here.
  [[nodiscard]] bool PullTaskDispatched() const noexcept { return pull_task_dispatched_; }

  /// DoWork calls this to atomically take the stash and clear the dispatched flag.
  /// Returns the stashed pull params; the stash is cleared so it cannot be taken twice.
  [[nodiscard]] PendingPull TakePendingPull() noexcept {
    pull_task_dispatched_ = false;
    return std::exchange(pending_pull_, PendingPull{});
  }

  /// Execute the HandlePullDiscard TAIL for a completed (non-parked) pull:
  ///   encoder_.MessageSuccess(summary)  → sets Bolt State (Result if has_more, else Idle).
  /// Returns false if sending the summary failed (caller should shut down the session).
  /// The Bolt state_ is updated internally, so the caller does NOT need to call SetBoltState.
  /// Called by the pull-task after Pull() returns with parked==false.
  bool FinishPull(bolt_map_t summary, bool is_pull);

  bolt_map_t Discard(std::optional<int> n, std::optional<int> qid);

  void Abort();

  /// Server/Session level API ///

  // Called during Init
  std::expected<void, communication::bolt::AuthFailure> Authenticate(const std::string &username,
                                                                     const std::string &password);

  // Called during Init
  std::expected<void, communication::bolt::AuthFailure> SSOAuthenticate(const std::string &scheme,
                                                                        const std::string &identity_provider_response);

  void LogOff();

  static std::optional<std::string> GetServerNameForInit();

  utils::Priority ApproximateQueryPriority() const;

  inline bool Execute() { return Execute_(*this); }

  memgraph::logging::SessionLogContext *GetLogContext() noexcept { return interpreter_.GetLogContext(); }

  metrics::DatabaseMetricHandles *GetMetricHandles() {
    auto &db_acc = interpreter_.current_db_.db_acc_;
    return db_acc ? (*db_acc)->metric_handles() : nullptr;
  }

 private:
  bolt_map_t DecodeSummary(const std::map<std::string, memgraph::query::TypedValue> &summary);

  std::optional<std::string> GetDefaultDB() const;

  void TryDefaultDB();

  std::string GetCurrentDB() const;

  std::optional<std::string> GetDefaultUser() const;

  std::string GetCurrentUser() const;

  memgraph::query::InterpreterContext *interpreter_context_;      // Global context used by all interpreters
  memgraph::query::Interpreter interpreter_;                      // Session specific interpreter
  std::shared_ptr<query::QueryUserOrRole> session_user_or_role_;  // Connected user/role
#ifdef MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
  RuntimeConfig runtime_config_;  // Run-time configurable database started used by the interpreter
  std::shared_ptr<memgraph::utils::UserResources> user_resource_;  // User-related resource monitoring
#endif
  memgraph::auth::SynchedAuth *auth_;
  memgraph::communication::v2::ServerEndpoint endpoint_;
  metrics::ScopedGauge bolt_session_gauge_;
  std::optional<ParseRes> parsed_res_;  // SessionHL corresponds to a single connection (we do not support out of order
                                        // execution, so a single query can be prepared/executed)

  // ── d1b: pending coro pull stash ─────────────────────────────────────────────────────────────
  // Both fields are touched exclusively by the in-flight DoWork/pull-task — no data race.
  PendingPull pending_pull_{};        ///< Stashed (n, qid, is_pull) from HandlePullDiscard.
  bool pull_task_dispatched_{false};  ///< Set by HandlePullDiscard; cleared by TakePendingPull.
};

}  // namespace memgraph::glue
