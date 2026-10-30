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

#include <mgp.hpp>

#include <string>

namespace {

constexpr std::string_view kProcedureRun = "run";
constexpr std::string_view kParameterQuery = "query";
constexpr std::string_view kReturnRow = "row";

void Run(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const std::string query(arguments[0].ValueString());
    const auto execution = mgp::QueryExecution(memgraph_graph).ExecuteQuery(query);
    while (auto row = execution.PullOne()) {
      auto record = record_factory.NewRecord();
      record.Insert(kReturnRow.data(), row->Values());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    mgp::AddProcedure(Run,
                      kProcedureRun,
                      mgp::ProcedureType::Write,
                      {mgp::Parameter(kParameterQuery, mgp::Type::String)},
                      {mgp::Return(kReturnRow, mgp::Type::Map)},
                      module,
                      memory);
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
