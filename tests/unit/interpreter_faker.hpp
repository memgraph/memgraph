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

#include "communication/result_stream_faker.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"

struct InterpreterFaker {
  InterpreterFaker(memgraph::query::InterpreterContext *interpreter_context, memgraph::dbms::DatabaseAccess db)
      : interpreter_context(interpreter_context), interpreter(interpreter_context, db) {
    interpreter_context->auth_checker = &auth_checker;
    interpreter_context->interpreters.WithLock([this](auto &interpreters) { interpreters.insert(&interpreter); });
  }

  auto Prepare(const std::string &query, const std::map<std::string, memgraph::storage::PropertyValue> &params = {}) {
    const auto [header, _1, qid, _2] = interpreter.Prepare(query, params, {});
    auto &db = interpreter.current_db_.db_acc_;
    ResultStreamFaker stream(db ? db->get()->storage() : nullptr);
    stream.Header(header);
    return std::make_pair(std::move(stream), qid);
  }

  void Pull(ResultStreamFaker *stream, std::optional<int> n = {}, std::optional<int> qid = {}) {
    const auto summary = interpreter.Pull(stream, n, qid);
    stream->Summary(summary);
  }

  /**
   * Execute the given query and commit the transaction.
   *
   * Return the query stream.
   */
  auto Interpret(const std::string &query, const std::map<std::string, memgraph::storage::PropertyValue> &params = {}) {
    auto prepare_result = Prepare(query, params);
    auto &stream = prepare_result.first;
    auto summary = interpreter.Pull(&stream, {}, prepare_result.second);
    stream.Summary(summary);
    return std::move(stream);
  }
  memgraph::query::AllowEverythingAuthChecker auth_checker;
  memgraph::query::InterpreterContext *interpreter_context;
  memgraph::query::Interpreter interpreter;
};
