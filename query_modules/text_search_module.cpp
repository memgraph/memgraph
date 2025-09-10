// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <fmt/format.h>
#include <iostream>
#include <mgp.hpp>
#include <string>
#include <string_view>

namespace TextSearch {
constexpr std::string_view kProcedureSearch = "search";
constexpr std::string_view kProcedureRegexSearch = "regex_search";
constexpr std::string_view kProcedureSearchAllProperties = "search_all";
constexpr std::string_view kProcedureAggregate = "aggregate";
constexpr std::string_view kProcedureSearchEdges = "search_edges";
constexpr std::string_view kProcedureRegexSearchEdges = "regex_search_edges";
constexpr std::string_view kProcedureSearchAllPropertiesEdges = "search_all_edges";
constexpr std::string_view kProcedureAggregateEdges = "aggregate_edges";
constexpr std::string_view kParameterIndexName = "index_name";
constexpr std::string_view kParameterSearchQuery = "search_query";
constexpr std::string_view kParameterAggregationQuery = "aggregation_query";
constexpr std::string_view kReturnNode = "node";
constexpr std::string_view kReturnEdge = "edge";
constexpr std::string_view kReturnAggregation = "aggregation";
constexpr std::string_view kReturnScore = "score";
constexpr std::string_view kSearchAllPrefix = "all";

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RegexSearch(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchAllProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RegexSearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchAllPropertiesEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void Aggregate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void AggregateEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace TextSearch

// Text search on nodes functions
void TextSearch::Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto *index_name = arguments[0].ValueString().data();
    const auto *search_query = arguments[1].ValueString().data();
    for (const auto &result :
         mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::SPECIFIED_PROPERTIES)) {
      auto record = record_factory.NewRecord();
      auto result_list = result.ValueList();
      record.Insert(TextSearch::kReturnNode.data(), result_list[0].ValueNode());
      record.Insert(TextSearch::kReturnScore.data(), result_list[1].ValueDouble());
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
    const auto index_name = arguments[0].ValueString();
    const auto search_query = arguments[1].ValueString();
    for (const auto &result : mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::REGEX)) {
      auto record = record_factory.NewRecord();
      auto result_list = result.ValueList();
      record.Insert(TextSearch::kReturnNode.data(), result_list[0].ValueNode());
      record.Insert(TextSearch::kReturnScore.data(), result_list[1].ValueDouble());
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
    auto const index_name = arguments[0].ValueString();
    auto const search_query = fmt::format("{}:{}", kSearchAllPrefix, arguments[1].ValueString());
    for (const auto &result :
         mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::ALL_PROPERTIES)) {
      auto record = record_factory.NewRecord();
      auto result_list = result.ValueList();
      record.Insert(TextSearch::kReturnNode.data(), result_list[0].ValueNode());
      record.Insert(TextSearch::kReturnScore.data(), result_list[1].ValueDouble());
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
    const auto index_name = arguments[0].ValueString();
    const auto search_query = arguments[1].ValueString();
    const auto aggregation_query = arguments[2].ValueString();
    const auto aggregation_result =
        mgp::AggregateOverTextIndex(memgraph_graph, index_name, search_query, aggregation_query);
    auto record = record_factory.NewRecord();
    record.Insert(TextSearch::kReturnAggregation.data(), aggregation_result.data());
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

// Text search on edges functions
void TextSearch::SearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto *index_name = arguments[0].ValueString().data();
    const auto *search_query = arguments[1].ValueString().data();
    for (const auto &result :
         mgp::SearchTextEdgeIndex(memgraph_graph, index_name, search_query, text_search_mode::SPECIFIED_PROPERTIES)) {
      auto record = record_factory.NewRecord();
      auto result_list = result.ValueList();
      record.Insert(TextSearch::kReturnEdge.data(), result_list[0].ValueRelationship());
      record.Insert(TextSearch::kReturnScore.data(), result_list[1].ValueDouble());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void TextSearch::RegexSearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_name = arguments[0].ValueString();
    const auto search_query = arguments[1].ValueString();
    for (const auto &result :
         mgp::SearchTextEdgeIndex(memgraph_graph, index_name, search_query, text_search_mode::REGEX)) {
      auto record = record_factory.NewRecord();
      auto result_list = result.ValueList();
      record.Insert(TextSearch::kReturnEdge.data(), result_list[0].ValueRelationship());
      record.Insert(TextSearch::kReturnScore.data(), result_list[1].ValueDouble());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void TextSearch::SearchAllPropertiesEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result,
                                          mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    auto const index_name = arguments[0].ValueString();
    auto const search_query = fmt::format("{}:{}", kSearchAllPrefix, arguments[1].ValueString());
    for (const auto &result :
         mgp::SearchTextEdgeIndex(memgraph_graph, index_name, search_query, text_search_mode::ALL_PROPERTIES)) {
      auto record = record_factory.NewRecord();
      auto result_list = result.ValueList();
      record.Insert(TextSearch::kReturnEdge.data(), result_list[0].ValueRelationship());
      record.Insert(TextSearch::kReturnScore.data(), result_list[1].ValueDouble());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void TextSearch::AggregateEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_name = arguments[0].ValueString();
    const auto search_query = arguments[1].ValueString();
    const auto aggregation_query = arguments[2].ValueString();
    const auto aggregation_result =
        mgp::AggregateOverTextEdgeIndex(memgraph_graph, index_name, search_query, aggregation_query);
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
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module, memory);

    AddProcedure(TextSearch::RegexSearch, TextSearch::kProcedureRegexSearch, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module, memory);

    AddProcedure(TextSearch::SearchAllProperties, TextSearch::kProcedureSearchAllProperties, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module, memory);

    AddProcedure(TextSearch::Aggregate, TextSearch::kProcedureAggregate, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterAggregationQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnAggregation, mgp::Type::String)}, query_module, memory);

    AddProcedure(TextSearch::SearchEdges, TextSearch::kProcedureSearchEdges, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnEdge, mgp::Type::Relationship),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module, memory);

    AddProcedure(TextSearch::RegexSearchEdges, TextSearch::kProcedureRegexSearchEdges, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnEdge, mgp::Type::Relationship),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module, memory);

    AddProcedure(TextSearch::SearchAllPropertiesEdges, TextSearch::kProcedureSearchAllPropertiesEdges,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnEdge, mgp::Type::Relationship),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module, memory);

    AddProcedure(TextSearch::AggregateEdges, TextSearch::kProcedureAggregateEdges, mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterAggregationQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnAggregation, mgp::Type::String)}, query_module, memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
