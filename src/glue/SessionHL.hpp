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
#pragma once

#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "communication/bolt/v1/session.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "glue/SessionContext.hpp"
#include "query/interpreter.hpp"
#include "utils/result.hpp"

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

  bolt_map_t Pull(std::optional<int> n, std::optional<int> qid);

  bolt_map_t Discard(std::optional<int> n, std::optional<int> qid);

  void Abort();

  /// Server/Session level API ///

  // Called during Init
  utils::BasicResult<communication::bolt::AuthFailure> Authenticate(const std::string &username,
                                                                    const std::string &password);

  // Called during Init
  utils::BasicResult<communication::bolt::AuthFailure> SSOAuthenticate(const std::string &scheme,
                                                                       const std::string &identity_provider_response);

  void LogOff();

  static std::optional<std::string> GetServerNameForInit();

  utils::Priority ApproximateQueryPriority() const;

  inline bool Execute() { return Execute_(*this); }

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
  std::optional<ParseRes> parsed_res_;  // SessionHL corresponds to a single connection (we do not support out of order
                                        // execution, so a single query can be prepared/executed)
};

}  // namespace memgraph::glue
