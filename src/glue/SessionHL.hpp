// Copyright 2023 Memgraph Ltd.
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

#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "dbms/session_context.hpp"
#include "query/interpreter.hpp"

#ifdef MG_ENTERPRISE
#include "dbms/session_context_handler.hpp"
#else
#include "dbms/session_context.hpp"
#endif

namespace memgraph::glue {

struct ContextWrapper {
  explicit ContextWrapper(memgraph::dbms::SessionContext sc);
  ~ContextWrapper();

  ContextWrapper(const ContextWrapper &) = delete;
  ContextWrapper &operator=(const ContextWrapper &) = delete;

  ContextWrapper(ContextWrapper &&in) noexcept;
  ContextWrapper &operator=(ContextWrapper &&in) noexcept;

  void Defunct();
  memgraph::query::InterpreterContext *interpreter_context();
  memgraph::query::Interpreter *interp();
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth() const;
  std::string run_id() const;
  bool defunct() const;
#ifdef MG_ENTERPRISE
  memgraph::audit::Log *audit_log() const;
#endif

 private:
  memgraph::dbms::SessionContext session_context;
  std::unique_ptr<memgraph::query::Interpreter> interpreter;
  bool defunct_;
};

class SessionHL final : public memgraph::communication::bolt::Session<memgraph::communication::v2::InputStream,
                                                                      memgraph::communication::v2::OutputStream> {
 public:
  SessionHL(memgraph::query::InterpreterContext *interpreter_context,
            const memgraph::communication::v2::ServerEndpoint &endpoint,
            memgraph::communication::v2::InputStream *input_stream,
            memgraph::communication::v2::OutputStream *output_stream,
            memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth,
            memgraph::audit::Log *audit_log);

  ~SessionHL() override;

  SessionHL(const SessionHL &) = delete;
  SessionHL &operator=(const SessionHL &) = delete;
  SessionHL(SessionHL &&) = delete;
  SessionHL &operator=(SessionHL &&) = delete;

  void Configure(const std::map<std::string, memgraph::communication::bolt::Value> &run_time_info) override;

  using TEncoder = memgraph::communication::bolt::Encoder<
      memgraph::communication::bolt::ChunkedEncoderBuffer<memgraph::communication::v2::OutputStream>>;

  void BeginTransaction(const std::map<std::string, memgraph::communication::bolt::Value> &extra) override;

  void CommitTransaction() override;

  void RollbackTransaction() override;

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(
      const std::string &query, const std::map<std::string, memgraph::communication::bolt::Value> &params,
      const std::map<std::string, memgraph::communication::bolt::Value> &extra) override;

  std::map<std::string, memgraph::communication::bolt::Value> Pull(TEncoder *encoder, std::optional<int> n,
                                                                   std::optional<int> qid) override;

  std::map<std::string, memgraph::communication::bolt::Value> Discard(std::optional<int> n,
                                                                      std::optional<int> qid) override;

  void Abort() override;

  // Called during Init
  bool Authenticate(const std::string &username, const std::string &password) override;

  std::optional<std::string> GetServerNameForInit() override;

  std::string GetDatabaseName() const override;

 private:
  std::map<std::string, memgraph::communication::bolt::Value> DecodeSummary(
      const std::map<std::string, memgraph::query::TypedValue> &summary);

  /**
   * @brief Get the user's default database
   *
   * @return std::string
   */
  std::string GetDefaultDB();

#ifdef MG_ENTERPRISE
  // memgraph::dbms::SessionContextHandler &sc_handler_;
#endif
  memgraph::query::InterpreterContext *interpreter_context_;
  memgraph::query::Interpreter interpreter_;
  memgraph::utils::Synchronized<memgraph::auth::Auth, memgraph::utils::WritePrioritizedRWLock> *auth_;
  std::optional<memgraph::auth::User> user_;
#ifdef MG_ENTERPRISE
  memgraph::audit::Log *audit_log_;
  bool in_explicit_db_{false};  //!< If true, the user has defined the database to use via metadata
#endif
  memgraph::communication::v2::ServerEndpoint endpoint_;
  // NOTE: run_id should be const but that complicates code a lot.
  std::optional<std::string> run_id_;
};

}  // namespace memgraph::glue
