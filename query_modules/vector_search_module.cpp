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

#include <iostream>
#include <mgp.hpp>
#include <string_view>

namespace VectorSearch {
static constexpr std::string_view kProcedureSearch = "search";
static constexpr std::string_view kParameterIndexName = "index_name";
static constexpr std::string_view kParameterResultSetSize = "result_set_size";
static constexpr std::string_view kParameterQueryVector = "query_vector";
static constexpr std::string_view kReturnNode = "node";
static constexpr std::string_view kReturnDistance = "distance";
static constexpr std::string_view kReturnSimilarity = "similarity";

static constexpr std::string_view kProcedureShowIndexInfo = "show_index_info";
static constexpr std::string_view kReturnIndexName = "index_name";
static constexpr std::string_view kReturnLabel = "label";
static constexpr std::string_view kReturnProperty = "property";
static constexpr std::string_view kMetric = "metric";
static constexpr std::string_view kReturnDimension = "dimension";
static constexpr std::string_view kReturnCapacity = "capacity";
static constexpr std::string_view kReturnSize = "size";
static constexpr std::string_view kReturnScalarKind = "scalar_kind";

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void ShowIndexInfo(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
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
      record.Insert(VectorSearch::kReturnNode.data(), result_list[0].ValueNode());
      record.Insert(VectorSearch::kReturnDistance.data(), result_list[1].ValueDouble());
      record.Insert(VectorSearch::kReturnSimilarity.data(), result_list[2].ValueDouble());
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void VectorSearch::ShowIndexInfo(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_info = mgp::GetVectorIndexInfo(memgraph_graph);

    for (const auto &info : index_info) {
      auto record = record_factory.NewRecord();
      auto info_list = info.ValueList();
      record.Insert(VectorSearch::kReturnIndexName.data(), std::string(info_list[0].ValueString()));
      record.Insert(VectorSearch::kReturnLabel.data(), std::string(info_list[1].ValueString()));
      record.Insert(VectorSearch::kReturnProperty.data(), std::string(info_list[2].ValueString()));
      record.Insert(VectorSearch::kMetric.data(), std::string(info_list[3].ValueString()));
      record.Insert(VectorSearch::kReturnDimension.data(), info_list[4].ValueInt());
      record.Insert(VectorSearch::kReturnCapacity.data(), info_list[5].ValueInt());
      record.Insert(VectorSearch::kReturnSize.data(), info_list[6].ValueInt());
      record.Insert(VectorSearch::kReturnScalarKind.data(), std::string(info_list[7].ValueString()));
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
                     mgp::Return(VectorSearch::kReturnNode, mgp::Type::Node),
                     mgp::Return(VectorSearch::kReturnDistance, mgp::Type::Double),
                     mgp::Return(VectorSearch::kReturnSimilarity, mgp::Type::Double),
                 },
                 module, memory);

    AddProcedure(VectorSearch::ShowIndexInfo, VectorSearch::kProcedureShowIndexInfo, mgp::ProcedureType::Read, {},
                 {
                     mgp::Return(VectorSearch::kReturnIndexName, mgp::Type::String),
                     mgp::Return(VectorSearch::kReturnLabel, mgp::Type::String),
                     mgp::Return(VectorSearch::kReturnProperty, mgp::Type::String),
                     mgp::Return(VectorSearch::kMetric, mgp::Type::String),
                     mgp::Return(VectorSearch::kReturnDimension, mgp::Type::Int),
                     mgp::Return(VectorSearch::kReturnCapacity, mgp::Type::Int),
                     mgp::Return(VectorSearch::kReturnSize, mgp::Type::Int),
                     mgp::Return(VectorSearch::kReturnScalarKind, mgp::Type::String),
                 },
                 module, memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
