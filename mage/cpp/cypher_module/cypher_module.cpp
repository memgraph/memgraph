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

#include <string>
#include <string_view>

#include <fmt/core.h>
#include <mgp.hpp>

#include "../do_module/execute_query_utils.hpp"

namespace {
constexpr std::string_view kProcedureDoIt = "doIt";
constexpr std::string_view kArgumentStatement = "statement";
constexpr std::string_view kArgumentParams = "params";
constexpr std::string_view kReturnValue = "value";

// Runs a single writable Cypher statement in the caller's transaction and returns one
// `value` map per result row. Node/relationship parameters are passed through natively — they
// resolve to the same live handles in the caller's snapshot.
void DoIt(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};

  const auto arguments = mgp::List(args);
  const auto statement = std::string(arguments[0].ValueString());
  const auto params = arguments[1].ValueMap();

  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto query_execution = mgp::QueryExecution(memgraph_graph);

    // cypher.doIt runs the statement inside the caller's WRITE transaction. A statement requiring
    // UNIQUE or READ_ONLY storage access (schema/DDL: index, constraint, enum, ...) can never
    // obtain that access from within, so it would stall until the access timeout. Reject it up
    // front instead. If the probe itself can't parse the statement, fall through so the executor
    // surfaces the real parse error rather than a lossy one.
    bool is_schema_statement = false;
    try {
      const auto access_type = query_execution.QueryStorageAccessType(statement, params);
      is_schema_statement =
          access_type == mgp::StorageAccessType::Unique || access_type == mgp::StorageAccessType::ReadOnly;
    } catch (...) {
    }
    if (is_schema_statement) {
      throw std::runtime_error(fmt::format("cypher.doIt cannot execute a schema-modifying statement: {}", statement));
    }

    const auto query_results =
        execute_query_utils::ExecuteQueryInCurrentTransaction(query_execution, statement, params);
    execute_query_utils::InsertQueryResults(record_factory, query_results, kReturnValue);
  } catch (const std::exception &e) {
    // Prefer the engine's real error text (syntax/semantic/runtime); the caught mgp exception
    // only carries a generic message. Fall back to that if no detail was recorded.
    const auto detail = mgp::LastErrorMessage();
    record_factory.SetErrorMessage(detail.empty() ? e.what() : detail);
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    mgp::AddProcedure(DoIt,
                      kProcedureDoIt,
                      mgp::ProcedureType::Write,
                      {mgp::Parameter(kArgumentStatement, mgp::Type::String),
                       mgp::Parameter(kArgumentParams, mgp::Type::Map, mgp::Value(mgp::Map()))},
                      {mgp::Return(kReturnValue, mgp::Type::Map)},
                      module,
                      memory);

  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
