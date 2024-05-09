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

#include <iostream>
#include <mgp.hpp>

namespace TextSearch {
constexpr std::string_view kProcedureSearch = "search";
constexpr std::string_view kProcedureRegexSearch = "regex_search";
constexpr std::string_view kProcedureSearchAllProperties = "search_all";
constexpr std::string_view kProcedureAggregate = "aggregate";
constexpr std::string_view kParameterIndexName = "index_name";
constexpr std::string_view kParameterSearchQuery = "search_query";
constexpr std::string_view kParameterAggregationQuery = "aggregation_query";
constexpr std::string_view kReturnNode = "node";
constexpr std::string_view kReturnAggregation = "aggregation";
const std::string kSearchAllPrefix = "all";

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RegexSearch(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchAllProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void Aggregate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace TextSearch

void TextSearch::Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto *index_name = arguments[0].ValueString().data();
    const auto *search_query = arguments[1].ValueString().data();
    for (const auto &node :
         mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::SPECIFIED_PROPERTIES)) {
      auto record = record_factory.NewRecord();
      record.Insert(TextSearch::kReturnNode.data(), node.ValueNode());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void TextSearch::RegexSearch(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto *index_name = arguments[0].ValueString().data();
    const auto *search_query = arguments[1].ValueString().data();
    for (const auto &node : mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::REGEX)) {
      auto record = record_factory.NewRecord();
      record.Insert(TextSearch::kReturnNode.data(), node.ValueNode());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void TextSearch::SearchAllProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result,
                                     mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto *index_name = arguments[0].ValueString().data();
    std::string search_query = fmt::format("{}:{}", kSearchAllPrefix, arguments[1].ValueString());
    for (const auto &node :
         mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::ALL_PROPERTIES)) {
      auto record = record_factory.NewRecord();
      record.Insert(TextSearch::kReturnNode.data(), node.ValueNode());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void TextSearch::Aggregate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto *index_name = arguments[0].ValueString().data();
    const auto *search_query = arguments[1].ValueString().data();
    const auto *aggregation_query = arguments[2].ValueString().data();
    const auto aggregation_result =
        mgp::AggregateOverTextIndex(memgraph_graph, index_name, search_query, aggregation_query);
    auto record = record_factory.NewRecord();
    record.Insert(TextSearch::kReturnAggregation.data(), aggregation_result.data());
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

extern "C" int mgp_init_module(struct mgp_module *query_module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(TextSearch::Search, TextSearch::kProcedureSearch, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node)}, query_module, memory);

    AddProcedure(TextSearch::RegexSearch, TextSearch::kProcedureRegexSearch, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node)}, query_module, memory);

    AddProcedure(TextSearch::SearchAllProperties, TextSearch::kProcedureSearchAllProperties, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node)}, query_module, memory);

    AddProcedure(TextSearch::Aggregate, TextSearch::kProcedureAggregate, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterAggregationQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnAggregation, mgp::Type::String)}, query_module, memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << std::endl;
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
