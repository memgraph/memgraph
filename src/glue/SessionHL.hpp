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
#include "communication/bolt/v1/state.hpp"
#include "communication/v2/server.hpp"
#include "communication/v2/session.hpp"
#include "glue/SessionContext.hpp"
#include "query/interpreter.hpp"
#include "utils/priority_thread_pool.hpp"

namespace memgraph::glue {

using bolt_value_t = memgraph::communication::bolt::Value;
using bolt_map_t = memgraph::communication::bolt::map_t;

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

  void Configure(const bolt_map_t &run_time_info);

  using TEncoder = memgraph::communication::bolt::Encoder<
      memgraph::communication::bolt::ChunkedEncoderBuffer<memgraph::communication::v2::OutputStream>>;

  void BeginTransaction(const bolt_map_t &extra);

  void CommitTransaction();

  void RollbackTransaction();

  std::pair<std::vector<std::string>, std::optional<int>> Interpret(const std::string &query, const bolt_map_t &params,
                                                                    const bolt_map_t &extra) {
    InterpretParse(query, params, extra);
    return InterpretPrepare();
  }

  void InterpretParse(const std::string &query, bolt_map_t params, const bolt_map_t &extra);

  std::pair<std::vector<std::string>, std::optional<int>> InterpretPrepare();

#ifdef MG_ENTERPRISE
  auto Route(bolt_map_t const &routing, std::vector<bolt_value_t> const &bookmarks, bolt_map_t const &extra)
      -> bolt_map_t;
#endif

  bolt_map_t Pull(std::optional<int> n, std::optional<int> qid);

  bolt_map_t Discard(std::optional<int> n, std::optional<int> qid);

  void Abort();

  void TryDefaultDB();

  // Called during Init
  bool Authenticate(const std::string &username, const std::string &password);

  // Called during Init
  bool SSOAuthenticate(const std::string &scheme, const std::string &identity_provider_response);

  std::optional<std::string> GetServerNameForInit();

  inline auto ApproximateQueryPriority() const {
    // Query has been parsed and a proprity can be determined
    if (parsed_res_ && state_ == memgraph::communication::bolt::State::Parsed) {
      return std::visit(utils::Overloaded{
                            [](const query::Interpreter::TransactionQuery &) {
                              // BEGIN; COMMIT; ROLLBACK
                              return utils::PriorityThreadPool::Priority::LOW;
                            },
                            [](const query::Interpreter::ParseInfo &parse_info) {
                              // Many variants of queries
                              // Cypher -> low
                              // all others -> high
                              return utils::Downcast<query::CypherQuery>(parse_info.parsed_query.query)
                                         ? utils::PriorityThreadPool::Priority::LOW
                                         : utils::PriorityThreadPool::Priority::HIGH;
                            },
                            [](const auto &) { MG_ASSERT(false, "Unexpected ParseRes variant!"); },
                        },
                        parsed_res_->parsed_query);
    }

    // Result means query has been prepared and we are pulling
    return state_ == memgraph::communication::bolt::State::Result ? interpreter_.ApproximateNextQueryPriority()
                                                                  : utils::PriorityThreadPool::Priority::HIGH;
  }

  std::string GetCurrentDB() const;

  inline bool Execute() { return Execute_(*this); }

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
  struct ParseRes {
    query::Interpreter::ParseRes parsed_query;
    query::UserParameters_fn get_params_pv;
    query::QueryExtras extra;
  };
  std::optional<ParseRes> parsed_res_;
};

}  // namespace memgraph::glue
