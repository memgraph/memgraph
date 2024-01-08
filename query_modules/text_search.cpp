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
// #include "query/procedure/mg_procedure_impl.hpp"
// #include "storage/v2/indices/mgcxx_mock.hpp"
#include "storage/v2/mgcxx_mock.hpp"

namespace TextSearch {
constexpr std::string_view kProcedureSearch = "search";
constexpr std::string_view kParameterLabel = "label";
constexpr std::string_view kParameterSearchString = "search_string";
constexpr std::string_view kReturnNode = "node";

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace TextSearch

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);
  auto label = arguments[0].ValueString();
  auto search_string = arguments[1].ValueString();

  // TODO antepusic:
  // 1. Match the label to the appropriate text index
  // auto label_id = memgraph_graph->impl->NameToLabel(label); <- needs API method

  if (!mgp::graph_has_text_index(memgraph_graph, label.data())) {
    return;
  }

  // 2. Run text search of that index
  //    * Add metadata to the return fields before search

  // text_index.search(label, search_string);

  // 3. Get the graph elements from their IDs in the search results

  // 4. Return records (one per element)
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
