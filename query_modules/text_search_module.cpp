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

#include <string>
#include <string_view>

#include <fmt/format.h>

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

  try {
    auto label = arguments[0].ValueString().data();
    auto search_query = arguments[1].ValueString().data();

    // 1. See if the given label is text-indexed
    if (!mgp::graph_has_text_index(memgraph_graph, label)) {
      record_factory.SetErrorMessage(fmt::format("Text index \"{}\" doesn’t exist.", label));
      return;
    }

    // 2. Run a text search of that index and return the search results
    const auto results = mgp::graph_search_text_index(memgraph_graph, memory, label, search_query);
    if (!results) return;

    for (const auto &node : mgp::List(results)) {
      auto record = record_factory.NewRecord();
      record.Insert(TextSearch::kReturnNode.data(), node);
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
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
