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

#include <mgp.hpp>

namespace TextSearch {
constexpr std::string_view kProcedureSearch = "search";
constexpr std::string_view kParameterLabel = "label";
constexpr std::string_view kParameterSearchString = "search_query";
constexpr std::string_view kReturnNode = "node";

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace TextSearch

void TextSearch::Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);
  auto label = arguments[0].ValueString();
  auto search_query = arguments[1].ValueString();

  // 1. See if the given label is text-indexed
  if (!mgp::graph_has_text_index(memgraph_graph, label.data())) {
    return;
  }

  // 2. Run a text search of that index and return the search results
  for (const auto &node :
       mgp::List(mgp::graph_search_text_index(memgraph_graph, memory, label.data(), search_query.data()))) {
    auto record = record_factory.NewRecord();
    record.Insert(TextSearch::kReturnNode.data(), node);
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(TextSearch::Search, TextSearch::kProcedureSearch, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterLabel, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchString, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node)}, module, memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
