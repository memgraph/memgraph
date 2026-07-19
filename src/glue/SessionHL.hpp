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
#include "utils/coro_task.hpp"

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

  // Invalidate the "run_time_info unchanged since last call => skip" cache below (see
  // Configure()) so the next RUN/BEGIN after a Bolt RESET/LogOff is forced to fully re-derive
  // user + db instead of silently keeping whatever a previous, logically unrelated, pooled
  // session configured. Without this, Interpreter::ResetForConnectionReuse()'s ResetDB() would
  // have no visible effect whenever the next session's extra/metadata map happens to be
  // identical to the one before RESET/LogOff.
  void ResetForConnectionReuse() { previous_run_time_info_.reset(); }

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

  /// Coroutine variant of InterpretPrepare() (Session-surgery Stage A). Mirrors InterpretPrepare()
  /// exactly except it `co_await`s Interpreter::PrepareCoro() instead of calling the blocking
  /// Interpreter::Prepare() -- see interpreter.hpp/.cpp for the accessor-acquire park mechanics.
  ///
  /// `parsed_res_` is moved out only INSIDE the coroutine body: since utils::Task<T> is lazily
  /// started (it does not run until first driven/co_await'd/Run()), this means the parse result is
  /// not actually consumed until whatever drives this Task starts doing so -- i.e. not before the
  /// accessor acquire is genuinely under way, matching the IP-1 design doc's ip1-design.md
  /// requirement ("moving parsed_res_ only INSIDE the coroutine").
  ///
  /// Stage A only: nothing yet drives/awaits this from the Bolt layer (that is Stage B -- a
  /// HandlePrepareCoro and a coroutine-aware Execute_/DoWork driver). Calling and immediately
  /// SyncWait()-ing this Task today would behave identically to InterpretPrepare(), just via the
  /// coroutine machinery and (when the experimental flag is on) with the possibility of an internal
  /// park during the accessor acquire.
  utils::Task<std::pair<std::vector<std::string>, std::optional<int>>> InterpretPrepareCoro();

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

  bolt_map_t Pull(std::optional<int> n, std::optional<int> qid);

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
};

}  // namespace memgraph::glue
