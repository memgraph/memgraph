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

#include "audit/log.hpp"
#include "auth/auth.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "dbms/database.hpp"
#include "glue/query_user.hpp"
#include "query/interpreter.hpp"

namespace memgraph::glue {

using bolt_value_t = memgraph::communication::bolt::Value;
using bolt_map_t = memgraph::communication::bolt::map_t;

class SessionHL final : public memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                                                      memgraph::communication::v2::OutputStream> {
 public:
  SessionHL(memgraph::query::InterpreterContext *interpreter_context,
            memgraph::communication::v2::ServerEndpoint endpoint,
            memgraph::communication::v2::InputStream *input_stream,
            memgraph::communication::v2::OutputStream *output_stream, memgraph::auth::SynchedAuth *auth
#ifdef MG_ENTERPRISE
            ,
            memgraph::audit::Log *audit_log
#endif
  );

  ~SessionHL() override;

  SessionHL(const SessionHL &) = delete;
  SessionHL &operator=(const SessionHL &) = delete;
  SessionHL(SessionHL &&) = delete;
  SessionHL &operator=(SessionHL &&) = delete;

  void Configure(const bolt_map_t &run_time_info) override;

  using TEncoder = memgraph::communication::bolt::Encoder<
      memgraph::communication::bolt::ChunkedEncoderBuffer<memgraph::communication::v2::OutputStream>>;

  void BeginTransaction(const bolt_map_t &extra) override;

  void CommitTransaction() override;

  void RollbackTransaction() override;

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(const std::string &query, const bolt_map_t &params,
                                                                    const bolt_map_t &extra) override;

#ifdef MG_ENTERPRISE
  auto Route(bolt_map_t const &routing, std::vector<bolt_value_t> const &bookmarks, bolt_map_t const &extra)
      -> bolt_map_t override;
#endif

  bolt_map_t Pull(TEncoder *encoder, std::optional<int> n, std::optional<int> qid) override;

  bolt_map_t Discard(std::optional<int> n, std::optional<int> qid) override;

  void Abort() override;

  void TryDefaultDB();

  // Called during Init
  bool Authenticate(const std::string &username, const std::string &password) override;

  // Called during Init
  bool SSOAuthenticate(const std::string &scheme, const std::string &identity_provider_response) override;

  std::optional<std::string> GetServerNameForInit() override;

  std::string GetCurrentDB() const override;

 private:
  bolt_map_t DecodeSummary(const std::map<std::string, memgraph::query::TypedValue> &summary);

  /**
   * @brief Get the user's default database
   *
   * @return std::string
   */
  std::optional<std::string> GetDefaultDB();

  memgraph::query::InterpreterContext *interpreter_context_;
  memgraph::query::Interpreter interpreter_;
  std::unique_ptr<query::QueryUserOrRole> user_or_role_;
#ifdef MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
  bool in_explicit_db_{false};  //!< If true, the user has defined the database to use via metadata
#endif
  memgraph::auth::SynchedAuth *auth_;
  memgraph::communication::v2::ServerEndpoint endpoint_;
  std::optional<std::string> implicit_db_;
};

}  // namespace memgraph::glue
