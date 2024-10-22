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

// TODO(gitbuda): Try to use logging under xyz_search modules.
#include <algorithm>
#include <iostream>
#include <string_view>

#include <fmt/format.h>

#include <mgp.hpp>

namespace VectorSearch {
constexpr std::string_view kProcedureSearch = "search";
constexpr std::string_view kParameterIndexName = "index_name";
constexpr std::string_view kParameterResultSetSize = "result_set_size";
constexpr std::string_view kParameterQueryVector = "query_vector";
constexpr std::string_view kReturnNodeId = "node_id";
constexpr std::string_view kReturnScore = "score";

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace VectorSearch

void VectorSearch::Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_name = arguments[0].ValueString();
    const auto result_set_size = arguments[1].ValueInt();
    auto query_vector = arguments[2].ValueList();

    auto results = mgp::SearchVectorIndex(memgraph_graph, index_name, query_vector, result_set_size);

    for (const auto &result : results) {
      auto record = record_factory.NewRecord();
      // result is also a list with two elements: node_id and score
      auto result_list = result.ValueList();
      record.Insert(VectorSearch::kReturnNodeId.data(), result_list[0].ValueNode());
      record.Insert(VectorSearch::kReturnScore.data(), result_list[1].ValueDouble());
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    AddProcedure(VectorSearch::Search, VectorSearch::kProcedureSearch, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(VectorSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(VectorSearch::kParameterResultSetSize, mgp::Type::Int),
                     mgp::Parameter(VectorSearch::kParameterQueryVector, {mgp::Type::List, mgp::Type::Any}),
                 },
                 {
                     mgp::Return(VectorSearch::kReturnNodeId, mgp::Type::Node),
                     mgp::Return(VectorSearch::kReturnScore, mgp::Type::Double),
                 },
                 module, memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << "\n";
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
