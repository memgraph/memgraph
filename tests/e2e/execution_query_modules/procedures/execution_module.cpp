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

#include <exception>
#include <stdexcept>

#include <functional>

#include "mg_procedure.h"

#include "mgp.hpp"

namespace {

using mgp_int = decltype(mgp::Value().ValueInt());
static mgp_int num_ints{0};
static mgp_int returned_ints{0};

static mgp_int num_strings{0};
static int returned_strings{0};

constexpr char const *kModuleExecuteFetchHeaders = "execute_fetch_headers";
constexpr char const *kModuleExecuteFetchResultCount = "execute_fetch_result_count";
constexpr char const *kModuleExecuteFetchResults = "execute_fetch_results";
constexpr char const *kInputArgumentQuery = "query";
constexpr char const *kReturnOutput = "output";

void ExecuteGetHeaders(struct mgp_list *args, mgp_graph *graph, mgp_result *result, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  const auto query = arguments[0].ValueString();

  const auto query_execution = mgp::QueryExecution(graph);
  const auto execution_result = query_execution.ExecuteQuery(query);

  auto result_list = mgp::List(execution_result.Headers().Size());
  for (const auto &header : execution_result.Headers()) {
    result_list.Append(mgp::Value(header));
  }

  auto record = record_factory.NewRecord();
  record.Insert(kReturnOutput, mgp::Value(std::move(result_list)));
}

void ExecuteGetResultCount(struct mgp_list *args, mgp_graph *graph, mgp_result *result, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  const auto query = arguments[0].ValueString();

  const auto query_execution = mgp::QueryExecution(graph);
  const auto execution_result = query_execution.ExecuteQuery(query);

  auto result_count = 0;
  while (auto maybe_result = execution_result.PullOne()) {
    result_count++;
  }

  auto record = record_factory.NewRecord();
  record.Insert(kReturnOutput, mgp::Value(static_cast<int64_t>(result_count)));
}

void ExecuteGetResults(struct mgp_list *args, mgp_graph *graph, mgp_result *result, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  const auto query = arguments[0].ValueString();

  const auto query_execution = mgp::QueryExecution(graph);
  const auto execution_result = query_execution.ExecuteQuery(query);

  while (auto maybe_result = execution_result.PullOne()) {
    auto record = record_factory.NewRecord();
    record.Insert(kReturnOutput, mgp::Value((*maybe_result).Values()));
  }
}

}  // namespace

// Each module needs to define mgp_init_module function.
// Here you can register multiple functions/procedures your module supports.
extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    mgp::AddProcedure(ExecuteGetHeaders, kModuleExecuteFetchHeaders, mgp::ProcedureType::Read,
                      {mgp::Parameter(kInputArgumentQuery, mgp::Type::String)},
                      {mgp::Return(kReturnOutput, mgp::Type::Any)}, module, memory);
    mgp::AddProcedure(ExecuteGetResultCount, kModuleExecuteFetchResultCount, mgp::ProcedureType::Read,
                      {mgp::Parameter(kInputArgumentQuery, mgp::Type::String)},
                      {mgp::Return(kReturnOutput, mgp::Type::Any)}, module, memory);
    mgp::AddProcedure(ExecuteGetResults, kModuleExecuteFetchResults, mgp::ProcedureType::Read,
                      {mgp::Parameter(kInputArgumentQuery, mgp::Type::String)},
                      {mgp::Return(kReturnOutput, mgp::Type::Any)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
extern "C" int mgp_shutdown_module() { return 0; }
