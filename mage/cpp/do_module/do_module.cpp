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
#include <ranges>
#include <string_view>

#include <fmt/core.h>
#include <mgp.hpp>

#include "execute_query_utils.hpp"

constexpr std::string_view kProcedureCase = "case";
constexpr std::string_view kArgumentConditionals = "conditionals";
constexpr std::string_view kArgumentElseQuery = "else_query";
constexpr std::string_view kArgumentParams = "params";

constexpr std::string_view kProcedureWhen = "when";
constexpr std::string_view kArgumentCondition = "condition";
constexpr std::string_view kArgumentIfQuery = "if_query";

constexpr std::string_view kReturnValue = "value";

const std::vector<std::string_view> kGlobalOperations = {"CREATE INDEX ON",
                                                         "DROP INDEX ON",
                                                         "CREATE CONSTRAINT ON",
                                                         "DROP CONSTRAINT ON",
                                                         "SET GLOBAL TRANSACTION ISOLATION LEVEL",
                                                         "STORAGE MODE IN_MEMORY_TRANSACTIONAL",
                                                         "STORAGE MODE IN_MEMORY_ANALYTICAL"};

namespace {
using execute_query_utils::ExecuteQuery;
using execute_query_utils::InsertQueryResults;

bool IsGlobalOperation(std::string_view query) {
  return std::ranges::any_of(kGlobalOperations,
                             [&query](const auto &global_op) { return query.starts_with(global_op); });
}

void When(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};

  const auto arguments = mgp::List(args);
  const auto condition = arguments[0].ValueBool();

  const auto &query_to_execute = std::string(arguments[condition ? 1 : 2].ValueString());
  const auto params = arguments[3].ValueMap();

  const auto record_factory = mgp::RecordFactory(result);

  try {
    if (IsGlobalOperation(query_to_execute)) {
      throw std::runtime_error(fmt::format(
          "The query {} isn’t supported by `do.when` because it would execute a global operation.", query_to_execute));
    }

    const auto query_execution = mgp::QueryExecution(memgraph_graph);
    const auto query_results = ExecuteQuery(query_execution, query_to_execute, params);
    InsertQueryResults(record_factory, query_results, kReturnValue);
    return;
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Case(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};

  const auto arguments = mgp::List(args);
  const auto conditionals = arguments[0].ValueList();
  const auto else_query = std::string(arguments[1].ValueString());
  const auto params = arguments[2].ValueMap();

  if (conditionals.Empty()) {
    throw std::runtime_error("Conditionals list must not be empty!");
  }

  const auto conditionals_size = conditionals.Size();

  if (conditionals_size % 2) {
    throw std::runtime_error("Size of the conditionals size must be even!");
  }

  for (size_t i = 0; i < conditionals_size; i++) {
    if (!(i % 2) && !conditionals[i].IsBool()) {
      throw std::runtime_error(fmt::format("Argument on index {} in do.case conditionals is not bool!", i));
    }
    if (i % 2 && !conditionals[i].IsString()) {
      throw std::runtime_error(fmt::format("Argument on index {} in do.case conditionals is not string!", i));
    }
  }

  auto found_true_conditional = -1;
  for (size_t i = 0; i < conditionals_size; i += 2) {
    const auto conditional = conditionals[i].ValueBool();
    if (conditional) {
      found_true_conditional = static_cast<int>(i);
      break;
    }
  }

  const auto query_to_execute =
      found_true_conditional != -1 ? std::string(conditionals[found_true_conditional + 1].ValueString()) : else_query;

  const auto record_factory = mgp::RecordFactory(result);

  try {
    if (IsGlobalOperation(query_to_execute)) {
      throw std::runtime_error(fmt::format(
          "The query {} isn’t supported by `do.case` because it would execute a global operation.", query_to_execute));
    }

    const auto query_execution = mgp::QueryExecution(memgraph_graph);
    const auto query_results = ExecuteQuery(query_execution, query_to_execute, params);
    InsertQueryResults(record_factory, query_results, kReturnValue);
    return;
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    mgp::AddProcedure(Case,
                      kProcedureCase,
                      mgp::ProcedureType::Read,
                      {mgp::Parameter(kArgumentConditionals, {mgp::Type::List, mgp::Type::Any}),
                       mgp::Parameter(kArgumentElseQuery, mgp::Type::String),
                       mgp::Parameter(kArgumentParams, mgp::Type::Map, mgp::Value(mgp::Map()))},
                      {mgp::Return(kReturnValue, mgp::Type::Map)},
                      module,
                      memory);

    mgp::AddProcedure(When,
                      kProcedureWhen,
                      mgp::ProcedureType::Read,
                      {mgp::Parameter(kArgumentCondition, mgp::Type::Bool),
                       mgp::Parameter(kArgumentIfQuery, mgp::Type::String),
                       mgp::Parameter(kArgumentElseQuery, mgp::Type::String),
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
