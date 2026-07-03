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

#include <cmath>
#include <iostream>
#include <string_view>
#include <utility>
#include <vector>

#include <mgp.hpp>

namespace VectorSearch {
static constexpr std::string_view kProcedureSearch = "search";
static constexpr std::string_view kProcedureSearchEdges = "search_edges";
static constexpr std::string_view kParameterIndexName = "index_name";
static constexpr std::string_view kParameterResultSetSize = "result_set_size";
static constexpr std::string_view kParameterQueryVector = "query_vector";
static constexpr std::string_view kParameterPrefilteredNodes = "prefiltered_nodes";
static constexpr std::string_view kParameterPrefilteredEdges = "prefiltered_edges";
static constexpr std::string_view kReturnNode = "node";
static constexpr std::string_view kReturnEdge = "edge";
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
static constexpr std::string_view kReturnIndexType = "index_type";

static constexpr std::string_view kProcedureCosineSimilarity = "cosine_similarity";
static constexpr std::string_view kProcedureInnerProduct = "ip";
static constexpr std::string_view kProcedureL2Squared = "l2sq";
static constexpr std::string_view kProcedureHaversine = "haversine";
static constexpr std::string_view kParameterVector1 = "vector1";
static constexpr std::string_view kParameterVector2 = "vector2";

// Builds a list of entity ids from the optional prefilter argument at `index` in `arguments`. The argument is a list
// of nodes, relationships, or integer ids; the returned list of ids is passed to the vector index to restrict the
// search. Returns an empty list (no filtering) when the argument is absent or empty.
mgp::List BuildElementFilter(const mgp::List &arguments, size_t index);

void Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void SearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void ShowIndexInfo(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void CosineSimilarityFunction(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);
void InnerProductFunction(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);
void L2SquaredFunction(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);
void HaversineFunction(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);
}  // namespace VectorSearch

mgp::List VectorSearch::BuildElementFilter(const mgp::List &arguments, size_t index) {
  mgp::List filter{};
  if (arguments.Size() <= index) {
    return filter;
  }
  const auto filter_arg = arguments[index].ValueList();
  for (const auto &val : filter_arg) {
    if (val.IsNode()) {
      filter.AppendExtend(mgp::Value(val.ValueNode().Id().AsInt()));
    } else if (val.IsRelationship()) {
      filter.AppendExtend(mgp::Value(val.ValueRelationship().Id().AsInt()));
    } else if (val.IsInt()) {
      filter.AppendExtend(mgp::Value(val.ValueInt()));
    } else {
      throw std::invalid_argument("The vector search filter must contain nodes, relationships, or their integer ids.");
    }
  }
  return filter;
}

void VectorSearch::Search(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_name = arguments[0].ValueString();
    const auto result_set_size = arguments[1].ValueInt();
    auto query_vector = arguments[2].ValueList();
    auto search_filter = VectorSearch::BuildElementFilter(arguments, 3);

    auto results = mgp::SearchVectorIndex(memgraph_graph, index_name, query_vector, result_set_size, search_filter);

    for (const auto &result : results) {
      auto record = record_factory.NewRecord();

      auto result_list = result.ValueList();
      record.Insert(VectorSearch::kReturnNode.data(), result_list[0].ValueNode());
      record.Insert(VectorSearch::kReturnDistance.data(), result_list[1].ValueDouble());
      record.Insert(VectorSearch::kReturnSimilarity.data(), result_list[2].ValueDouble());
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void VectorSearch::SearchEdges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);
  auto arguments = mgp::List(args);

  try {
    const auto index_name = arguments[0].ValueString();
    const auto result_set_size = arguments[1].ValueInt();
    auto query_vector = arguments[2].ValueList();
    auto search_filter = VectorSearch::BuildElementFilter(arguments, 3);

    auto results =
        mgp::SearchVectorIndexOnEdges(memgraph_graph, index_name, query_vector, result_set_size, search_filter);

    for (const auto &result : results) {
      auto record = record_factory.NewRecord();

      auto result_list = result.ValueList();
      record.Insert(VectorSearch::kReturnEdge.data(), result_list[0].ValueRelationship());
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
      record.Insert(VectorSearch::kReturnIndexName.data(), info_list[0].ValueString());
      record.Insert(VectorSearch::kReturnLabel.data(), info_list[1].ValueString());
      record.Insert(VectorSearch::kReturnProperty.data(), info_list[2].ValueString());
      record.Insert(VectorSearch::kMetric.data(), info_list[3].ValueString());
      record.Insert(VectorSearch::kReturnDimension.data(), info_list[4].ValueInt());
      record.Insert(VectorSearch::kReturnCapacity.data(), info_list[5].ValueInt());
      record.Insert(VectorSearch::kReturnSize.data(), info_list[6].ValueInt());
      record.Insert(VectorSearch::kReturnScalarKind.data(), info_list[7].ValueString());
      record.Insert(VectorSearch::kReturnIndexType.data(), info_list[8].ValueString());
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

namespace {
// Extracts two equal-length numeric vectors from the function arguments. Must be called with an active
// MemoryDispatcherGuard, as it constructs mgp values.
std::pair<std::vector<double>, std::vector<double>> ExtractVectorPair(mgp_list *args) {
  const auto vector1 = mgp::Value(mgp::ref_type, mgp::list_at(args, 0)).ValueList();
  const auto vector2 = mgp::Value(mgp::ref_type, mgp::list_at(args, 1)).ValueList();

  if (vector1.Size() == 0 || vector1.Size() != vector2.Size()) {
    throw std::invalid_argument("Vectors must be non-empty and have the same dimension");
  }

  auto get_numeric_value = [](const mgp::Value &val) {
    if (val.IsDouble()) {
      return val.ValueDouble();
    }
    if (val.IsInt()) {
      return static_cast<double>(val.ValueInt());
    }
    throw std::invalid_argument("Vector elements must be numeric (int or double)");
  };

  std::vector<double> v1;
  std::vector<double> v2;
  v1.reserve(vector1.Size());
  v2.reserve(vector2.Size());
  for (size_t i = 0; i < vector1.Size(); ++i) {
    v1.push_back(get_numeric_value(vector1[i]));
    v2.push_back(get_numeric_value(vector2[i]));
  }
  return {std::move(v1), std::move(v2)};
}
}  // namespace

void VectorSearch::CosineSimilarityFunction(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res,
                                            mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    const auto [v1, v2] = ExtractVectorPair(args);

    auto dot_product = 0.0;
    auto magnitude1 = 0.0;
    auto magnitude2 = 0.0;
    for (size_t i = 0; i < v1.size(); ++i) {
      dot_product += v1[i] * v2[i];
      magnitude1 += v1[i] * v1[i];
      magnitude2 += v2[i] * v2[i];
    }
    magnitude1 = std::sqrt(magnitude1);
    magnitude2 = std::sqrt(magnitude2);
    if (magnitude1 == 0.0 || magnitude2 == 0.0) [[unlikely]] {
      throw std::invalid_argument("Cannot calculate cosine similarity for zero vectors");
    }

    const auto cosine_similarity = dot_product / (magnitude1 * magnitude2);
    auto result = mgp::Result(res);
    result.SetValue(cosine_similarity);
  } catch (const std::exception &e) {
    mgp::func_result_set_error_msg(res, e.what(), memory);
  }
}

// Inner-product distance, matching uSearch's `ip` metric: 1 - dot(v1, v2).
void VectorSearch::InnerProductFunction(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res,
                                        mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    const auto [v1, v2] = ExtractVectorPair(args);

    auto dot_product = 0.0;
    for (size_t i = 0; i < v1.size(); ++i) {
      dot_product += v1[i] * v2[i];
    }

    auto result = mgp::Result(res);
    result.SetValue(1.0 - dot_product);
  } catch (const std::exception &e) {
    mgp::func_result_set_error_msg(res, e.what(), memory);
  }
}

// Squared Euclidean distance, matching uSearch's `l2sq` metric (the default index metric): sum((a - b)^2).
void VectorSearch::L2SquaredFunction(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res,
                                     mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    const auto [v1, v2] = ExtractVectorPair(args);

    auto squared_distance = 0.0;
    for (size_t i = 0; i < v1.size(); ++i) {
      const auto diff = v1[i] - v2[i];
      squared_distance += diff * diff;
    }

    auto result = mgp::Result(res);
    result.SetValue(squared_distance);
  } catch (const std::exception &e) {
    mgp::func_result_set_error_msg(res, e.what(), memory);
  }
}

// Haversine (great-circle) distance on the unit sphere, matching uSearch's `haversine` metric. Both inputs must be
// 2-element [latitude, longitude] vectors in degrees; the result is the central angle in radians.
void VectorSearch::HaversineFunction(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res,
                                     mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    const auto [v1, v2] = ExtractVectorPair(args);
    if (v1.size() != 2) {
      throw std::invalid_argument("Haversine distance requires 2-element [latitude, longitude] vectors (in degrees)");
    }

    auto to_radians = [](double degrees) { return degrees * M_PI / 180.0; };
    const auto lat_a = v1[0];
    const auto lon_a = v1[1];
    const auto lat_b = v2[0];
    const auto lon_b = v2[1];

    const auto lat_delta = to_radians(lat_b - lat_a) / 2.0;
    const auto lon_delta = to_radians(lon_b - lon_a) / 2.0;

    const auto x = std::sin(lat_delta) * std::sin(lat_delta) + std::cos(to_radians(lat_a)) *
                                                                   std::cos(to_radians(lat_b)) * std::sin(lon_delta) *
                                                                   std::sin(lon_delta);

    auto result = mgp::Result(res);
    result.SetValue(2.0 * std::asin(std::sqrt(x)));
  } catch (const std::exception &e) {
    mgp::func_result_set_error_msg(res, e.what(), memory);
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    const auto empty_filter = mgp::Value(mgp::List{});
    AddProcedure(
        VectorSearch::Search,
        VectorSearch::kProcedureSearch,
        mgp::ProcedureType::Read,
        {
            mgp::Parameter(VectorSearch::kParameterIndexName, mgp::Type::String),
            mgp::Parameter(VectorSearch::kParameterResultSetSize, mgp::Type::Int),
            mgp::Parameter(VectorSearch::kParameterQueryVector, {mgp::Type::List, mgp::Type::Any}),
            mgp::Parameter(VectorSearch::kParameterPrefilteredNodes, {mgp::Type::List, mgp::Type::Any}, empty_filter),
        },
        {
            mgp::Return(VectorSearch::kReturnNode, mgp::Type::Node),
            mgp::Return(VectorSearch::kReturnDistance, mgp::Type::Double),
            mgp::Return(VectorSearch::kReturnSimilarity, mgp::Type::Double),
        },
        module,
        memory);

    AddProcedure(VectorSearch::ShowIndexInfo,
                 VectorSearch::kProcedureShowIndexInfo,
                 mgp::ProcedureType::Read,
                 {},
                 {
                     mgp::Return(VectorSearch::kReturnIndexName, mgp::Type::String),
                     mgp::Return(VectorSearch::kReturnLabel, mgp::Type::String),
                     mgp::Return(VectorSearch::kReturnProperty, mgp::Type::String),
                     mgp::Return(VectorSearch::kMetric, mgp::Type::String),
                     mgp::Return(VectorSearch::kReturnDimension, mgp::Type::Int),
                     mgp::Return(VectorSearch::kReturnCapacity, mgp::Type::Int),
                     mgp::Return(VectorSearch::kReturnSize, mgp::Type::Int),
                     mgp::Return(VectorSearch::kReturnScalarKind, mgp::Type::String),
                     mgp::Return(VectorSearch::kReturnIndexType, mgp::Type::String),
                 },
                 module,
                 memory);

    AddProcedure(
        VectorSearch::SearchEdges,
        VectorSearch::kProcedureSearchEdges,
        mgp::ProcedureType::Read,
        {
            mgp::Parameter(VectorSearch::kParameterIndexName, mgp::Type::String),
            mgp::Parameter(VectorSearch::kParameterResultSetSize, mgp::Type::Int),
            mgp::Parameter(VectorSearch::kParameterQueryVector, {mgp::Type::List, mgp::Type::Any}),
            mgp::Parameter(VectorSearch::kParameterPrefilteredEdges, {mgp::Type::List, mgp::Type::Any}, empty_filter),
        },
        {
            mgp::Return(VectorSearch::kReturnEdge, mgp::Type::Relationship),
            mgp::Return(VectorSearch::kReturnDistance, mgp::Type::Double),
            mgp::Return(VectorSearch::kReturnSimilarity, mgp::Type::Double),
        },
        module,
        memory);

    mgp::AddFunction(VectorSearch::CosineSimilarityFunction,
                     VectorSearch::kProcedureCosineSimilarity,
                     {
                         mgp::Parameter(VectorSearch::kParameterVector1, {mgp::Type::List, mgp::Type::Any}),
                         mgp::Parameter(VectorSearch::kParameterVector2, {mgp::Type::List, mgp::Type::Any}),
                     },
                     module,
                     memory);

    mgp::AddFunction(VectorSearch::InnerProductFunction,
                     VectorSearch::kProcedureInnerProduct,
                     {
                         mgp::Parameter(VectorSearch::kParameterVector1, {mgp::Type::List, mgp::Type::Any}),
                         mgp::Parameter(VectorSearch::kParameterVector2, {mgp::Type::List, mgp::Type::Any}),
                     },
                     module,
                     memory);

    mgp::AddFunction(VectorSearch::L2SquaredFunction,
                     VectorSearch::kProcedureL2Squared,
                     {
                         mgp::Parameter(VectorSearch::kParameterVector1, {mgp::Type::List, mgp::Type::Any}),
                         mgp::Parameter(VectorSearch::kParameterVector2, {mgp::Type::List, mgp::Type::Any}),
                     },
                     module,
                     memory);

    mgp::AddFunction(VectorSearch::HaversineFunction,
                     VectorSearch::kProcedureHaversine,
                     {
                         mgp::Parameter(VectorSearch::kParameterVector1, {mgp::Type::List, mgp::Type::Any}),
                         mgp::Parameter(VectorSearch::kParameterVector2, {mgp::Type::List, mgp::Type::Any}),
                     },
                     module,
                     memory);
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
