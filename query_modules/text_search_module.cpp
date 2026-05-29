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

#include <fmt/format.h>
#include <algorithm>
#include <array>
#include <iostream>
#include <mgp.hpp>
#include <stdexcept>
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
constexpr std::string_view kParameterConfig = "config";
constexpr std::string_view kReturnNode = "node";
constexpr std::string_view kReturnEdge = "edge";
constexpr std::string_view kReturnAggregation = "aggregation";
constexpr std::string_view kReturnScore = "score";
constexpr std::string_view kSearchAllPrefix = "all";

constexpr std::string_view kConfigLimit = "limit";
constexpr std::string_view kConfigFuzzyDistance = "fuzzy_distance";
constexpr std::string_view kConfigFuzzyPrefix = "fuzzy_prefix";
constexpr std::string_view kConfigFuzzyTranspositions = "fuzzy_transpositions";
constexpr std::array<std::string_view, 4> kRecognisedConfigKeys{
    kConfigLimit, kConfigFuzzyDistance, kConfigFuzzyPrefix, kConfigFuzzyTranspositions};

mgp::TextSearchConfig ParseConfig(const mgp::Map &config, bool fuzzy_supported);

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RegexSearch(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchAllProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RegexSearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchAllPropertiesEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void Aggregate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void AggregateEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace TextSearch

namespace {
// mgp::Map::At returns Value by value; never bind it to a reference.
mgp::Value ExtractTyped(const mgp::Map &config, std::string_view key, bool (mgp::Value::*is_type)() const,
                        std::string_view type_name) {
  auto value = config.At(key);
  if (!(value.*is_type)()) {
    throw std::invalid_argument(fmt::format("text_search config '{}' must be {}.", key, type_name));
  }
  return value;
}

void RejectIfUnsupported(bool fuzzy_supported, bool key_makes_difference, std::string_view key) {
  if (!fuzzy_supported && key_makes_difference) {
    throw std::invalid_argument(
        fmt::format("text_search config '{}' is not supported on regex_search procedures.", key));
  }
}
}  // namespace

mgp::TextSearchConfig TextSearch::ParseConfig(const mgp::Map &config, bool fuzzy_supported) {
  mgp::TextSearchConfig parsed{};

  for (const auto &[key, _] : config) {
    if (!std::ranges::contains(kRecognisedConfigKeys, key)) {
      throw std::invalid_argument(fmt::format("Unknown text_search config key: '{}'.", key));
    }
  }

  if (config.KeyExists(kConfigLimit)) {
    const auto raw = ExtractTyped(config, kConfigLimit, &mgp::Value::IsInt, "an integer").ValueInt();
    if (raw < 0) {
      throw std::invalid_argument("text_search config 'limit' must be non-negative.");
    }
    parsed.limit = static_cast<std::size_t>(raw);
  }

  if (config.KeyExists(kConfigFuzzyDistance)) {
    const auto raw = ExtractTyped(config, kConfigFuzzyDistance, &mgp::Value::IsInt, "an integer").ValueInt();
    RejectIfUnsupported(fuzzy_supported, raw != 0, kConfigFuzzyDistance);
    parsed.fuzzy_distance = static_cast<std::uint8_t>(raw);
  }

  if (config.KeyExists(kConfigFuzzyPrefix)) {
    const auto raw = ExtractTyped(config, kConfigFuzzyPrefix, &mgp::Value::IsBool, "a boolean").ValueBool();
    RejectIfUnsupported(fuzzy_supported, raw, kConfigFuzzyPrefix);
    parsed.fuzzy_prefix = raw;
  }

  if (config.KeyExists(kConfigFuzzyTranspositions)) {
    const auto raw = ExtractTyped(config, kConfigFuzzyTranspositions, &mgp::Value::IsBool, "a boolean").ValueBool();
    RejectIfUnsupported(fuzzy_supported, !raw, kConfigFuzzyTranspositions);  // default is true
    parsed.fuzzy_transpositions = raw;
  }

  return parsed;
}

void TextSearch::Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_name = arguments[0].ValueString();
    const auto search_query = arguments[1].ValueString();
    const auto config = ParseConfig(arguments[2].ValueMap(), /*fuzzy_supported=*/true);
    for (const auto &row : mgp::SearchTextIndex(
             memgraph_graph, index_name, search_query, text_search_mode::SPECIFIED_PROPERTIES, config)) {
      auto record = record_factory.NewRecord();
      auto row_list = row.ValueList();
      record.Insert(TextSearch::kReturnNode.data(), row_list[0].ValueNode());
      record.Insert(TextSearch::kReturnScore.data(), row_list[1].ValueDouble());
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
    const auto config = ParseConfig(arguments[2].ValueMap(), /*fuzzy_supported=*/false);
    for (const auto &row :
         mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::REGEX, config)) {
      auto record = record_factory.NewRecord();
      auto row_list = row.ValueList();
      record.Insert(TextSearch::kReturnNode.data(), row_list[0].ValueNode());
      record.Insert(TextSearch::kReturnScore.data(), row_list[1].ValueDouble());
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
    const auto index_name = arguments[0].ValueString();
    const auto search_query = fmt::format("{}:{}", kSearchAllPrefix, arguments[1].ValueString());
    const auto config = ParseConfig(arguments[2].ValueMap(), /*fuzzy_supported=*/true);
    for (const auto &row :
         mgp::SearchTextIndex(memgraph_graph, index_name, search_query, text_search_mode::ALL_PROPERTIES, config)) {
      auto record = record_factory.NewRecord();
      auto row_list = row.ValueList();
      record.Insert(TextSearch::kReturnNode.data(), row_list[0].ValueNode());
      record.Insert(TextSearch::kReturnScore.data(), row_list[1].ValueDouble());
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

void TextSearch::SearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_name = arguments[0].ValueString();
    const auto search_query = arguments[1].ValueString();
    const auto config = ParseConfig(arguments[2].ValueMap(), /*fuzzy_supported=*/true);
    for (const auto &row : mgp::SearchTextEdgeIndex(
             memgraph_graph, index_name, search_query, text_search_mode::SPECIFIED_PROPERTIES, config)) {
      auto record = record_factory.NewRecord();
      auto row_list = row.ValueList();
      record.Insert(TextSearch::kReturnEdge.data(), row_list[0].ValueRelationship());
      record.Insert(TextSearch::kReturnScore.data(), row_list[1].ValueDouble());
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
    const auto config = ParseConfig(arguments[2].ValueMap(), /*fuzzy_supported=*/false);
    for (const auto &row :
         mgp::SearchTextEdgeIndex(memgraph_graph, index_name, search_query, text_search_mode::REGEX, config)) {
      auto record = record_factory.NewRecord();
      auto row_list = row.ValueList();
      record.Insert(TextSearch::kReturnEdge.data(), row_list[0].ValueRelationship());
      record.Insert(TextSearch::kReturnScore.data(), row_list[1].ValueDouble());
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
    const auto index_name = arguments[0].ValueString();
    const auto search_query = fmt::format("{}:{}", kSearchAllPrefix, arguments[1].ValueString());
    const auto config = ParseConfig(arguments[2].ValueMap(), /*fuzzy_supported=*/true);
    for (const auto &row :
         mgp::SearchTextEdgeIndex(memgraph_graph, index_name, search_query, text_search_mode::ALL_PROPERTIES, config)) {
      auto record = record_factory.NewRecord();
      auto row_list = row.ValueList();
      record.Insert(TextSearch::kReturnEdge.data(), row_list[0].ValueRelationship());
      record.Insert(TextSearch::kReturnScore.data(), row_list[1].ValueDouble());
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

    const auto default_config = mgp::Value(mgp::Map{});

    AddProcedure(TextSearch::Search,
                 TextSearch::kProcedureSearch,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterConfig, {mgp::Type::Map, mgp::Type::Any}, default_config),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module,
                 memory);

    AddProcedure(TextSearch::RegexSearch,
                 TextSearch::kProcedureRegexSearch,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterConfig, {mgp::Type::Map, mgp::Type::Any}, default_config),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module,
                 memory);

    AddProcedure(TextSearch::SearchAllProperties,
                 TextSearch::kProcedureSearchAllProperties,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterConfig, {mgp::Type::Map, mgp::Type::Any}, default_config),
                 },
                 {mgp::Return(TextSearch::kReturnNode, mgp::Type::Node),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module,
                 memory);

    AddProcedure(TextSearch::Aggregate,
                 TextSearch::kProcedureAggregate,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterAggregationQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnAggregation, mgp::Type::String)},
                 query_module,
                 memory);

    AddProcedure(TextSearch::SearchEdges,
                 TextSearch::kProcedureSearchEdges,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterConfig, {mgp::Type::Map, mgp::Type::Any}, default_config),
                 },
                 {mgp::Return(TextSearch::kReturnEdge, mgp::Type::Relationship),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module,
                 memory);

    AddProcedure(TextSearch::RegexSearchEdges,
                 TextSearch::kProcedureRegexSearchEdges,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterConfig, {mgp::Type::Map, mgp::Type::Any}, default_config),
                 },
                 {mgp::Return(TextSearch::kReturnEdge, mgp::Type::Relationship),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module,
                 memory);

    AddProcedure(TextSearch::SearchAllPropertiesEdges,
                 TextSearch::kProcedureSearchAllPropertiesEdges,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterConfig, {mgp::Type::Map, mgp::Type::Any}, default_config),
                 },
                 {mgp::Return(TextSearch::kReturnEdge, mgp::Type::Relationship),
                  mgp::Return(TextSearch::kReturnScore, mgp::Type::Double)},
                 query_module,
                 memory);

    AddProcedure(TextSearch::AggregateEdges,
                 TextSearch::kProcedureAggregateEdges,
                 mgp::ProcedureType::Read,
                 {
                     mgp::Parameter(TextSearch::kParameterIndexName, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterSearchQuery, mgp::Type::String),
                     mgp::Parameter(TextSearch::kParameterAggregationQuery, mgp::Type::String),
                 },
                 {mgp::Return(TextSearch::kReturnAggregation, mgp::Type::String)},
                 query_module,
                 memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
