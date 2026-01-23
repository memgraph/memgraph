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

#include <fmt/core.h>
#include <mgp.hpp>
#include <string>
#include <string_view>

constexpr const char *kProcedurePeriodicIterate = "iterate";
constexpr const char *kProcedurePeriodicDelete = "delete";
constexpr const char *kArgumentInputQuery = "input_query";
constexpr const char *kArgumentRunningQuery = "running_query";
constexpr const char *kArgumentConfig = "config";
constexpr const char *kConfigKeyBatchSize = "batch_size";
constexpr const char *kBatchInternalName = "__batch";
constexpr const char *kBatchRowInternalName = "__batch_row";
constexpr const char *kConfigKeyLabels = "labels";
constexpr const char *kConfigKeyEdgeTypes = "edge_types";

constexpr const char *kReturnSuccess = "success";
constexpr const char *kReturnNumBatches = "number_of_executed_batches";
constexpr const char *kReturnNumDeletedNodes = "number_of_deleted_nodes";
constexpr const char *kReturnNumDeletedRelationships = "number_of_deleted_relationships";
constexpr const char *kReturnInternalNumDeleted = "num_deleted";

struct ParamNames {
  std::vector<std::string> node_names;
  std::vector<std::string> relationship_names;
  std::vector<std::string> primitive_names;
};

struct DeletionInfo {
  uint64_t batch_size{0};
  std::vector<std::string> labels;
  std::vector<std::string> edge_types;
};

struct DeletionResult {
  uint64_t num_batches{0};
  uint64_t num_deleted_nodes{0};
  uint64_t num_deleted_relationships{0};
};

namespace {

ParamNames ExtractParamNames(const mgp::ExecutionHeaders &headers, const mgp::List &batch_row) {
  ParamNames res;
  for (size_t i = 0; i < headers.Size(); i++) {
    if (batch_row[i].IsNode()) {
      res.node_names.emplace_back(headers[i]);
    } else if (batch_row[i].IsRelationship()) {
      res.relationship_names.emplace_back(headers[i]);
    } else {
      res.primitive_names.emplace_back(headers[i]);
    }
  }

  return res;
}

std::string Join(const std::vector<std::string> &strings, const std::string &delimiter) {
  if (strings.empty()) {
    return "";
  }

  size_t joined_strings_size = 0;
  for (const auto &string : strings) {
    joined_strings_size += string.size();
  }

  std::string joined_strings;
  joined_strings.reserve(joined_strings_size + (delimiter.size() * (strings.size() - 1)));

  joined_strings += strings[0];
  for (size_t i = 1; i < strings.size(); i++) {
    joined_strings += delimiter + strings[i];
  }

  return joined_strings;
}

std::string GetGraphFirstClassEntityAlias(const std::string &internal_name, const std::string &entity_name) {
  return fmt::format("{0}.{1} AS __{1}_id", internal_name, entity_name);
}

std::string GetPrimitiveEntityAlias(const std::string &internal_name, const std::string &primitive_name) {
  return fmt::format("{0}.{1} AS {1}", internal_name, primitive_name);
}

std::string ConstructWithStatement(const ParamNames &names) {
  std::vector<std::string> with_entity_vector;
  with_entity_vector.reserve(names.node_names.size() + names.relationship_names.size() + names.primitive_names.size());
  for (const auto &node_name : names.node_names) {
    with_entity_vector.emplace_back(GetGraphFirstClassEntityAlias(kBatchRowInternalName, node_name));
  }
  for (const auto &rel_name : names.relationship_names) {
    with_entity_vector.emplace_back(GetGraphFirstClassEntityAlias(kBatchRowInternalName, rel_name));
  }
  for (const auto &prim_name : names.primitive_names) {
    with_entity_vector.emplace_back(GetPrimitiveEntityAlias(kBatchRowInternalName, prim_name));
  }

  return fmt::format("WITH {}", Join(with_entity_vector, ", "));
}

std::string ConstructMatchingNodeById(const std::string &node_name) {
  return fmt::format("MATCH ({0}) WHERE ID({0}) = __{0}_id", node_name);
}

std::string ConstructMatchingRelationshipById(const std::string &rel_name) {
  return fmt::format("MATCH ()-[{0}]->() WHERE ID({0}) = __{0}_id", rel_name);
}

std::string ConstructMatchGraphEntitiesById(const ParamNames &names) {
  std::string match_string;
  std::vector<std::string> match_by_id_vector;
  match_by_id_vector.reserve(names.node_names.size() + names.relationship_names.size());
  for (const auto &node_name : names.node_names) {
    match_by_id_vector.emplace_back(ConstructMatchingNodeById(node_name));
  }
  for (const auto &rel_name : names.relationship_names) {
    match_by_id_vector.emplace_back(ConstructMatchingRelationshipById(rel_name));
  }

  if (!match_by_id_vector.empty()) {
    match_string = Join(match_by_id_vector, " ");
  }

  return match_string;
}

std::string ConstructQueryPrefix(const ParamNames &names) {
  if (names.node_names.empty() && names.relationship_names.empty() && names.primitive_names.empty()) {
    return {};
  }

  auto unwind_batch = fmt::format("UNWIND ${} AS {}", kBatchInternalName, kBatchRowInternalName);
  auto with_variables = ConstructWithStatement(names);
  auto match_string = ConstructMatchGraphEntitiesById(names);

  return fmt::format("{} {} {}", unwind_batch, with_variables, match_string);
}

mgp::Map ConstructQueryParams(const mgp::ExecutionHeaders &headers, const mgp::List &batch) {
  mgp::List list_value(batch.Size());

  auto param_row_size = headers.Size();

  for (size_t row = 0; row < batch.Size(); row++) {
    mgp::Map constructed_row;

    mgp::List row_list = batch[row].ValueList();

    for (size_t i = 0; i < param_row_size; i++) {
      if (row_list[i].IsNode()) {
        constructed_row.Insert(headers[i], mgp::Value(row_list[i].ValueNode().Id().AsInt()));
      } else if (row_list[i].IsRelationship()) {
        constructed_row.Insert(headers[i], mgp::Value(row_list[i].ValueRelationship().Id().AsInt()));
      } else {
        constructed_row.Insert(headers[i], row_list[i]);
      }
    }

    list_value.Append(mgp::Value(std::move(constructed_row)));
  }

  mgp::Map params;
  params.Insert(kBatchInternalName, mgp::Value(std::move(list_value)));

  return params;
}

std::string ConstructFinalQuery(const std::string &running_query, const std::string &prefix_query) {
  return fmt::format("{} {}", prefix_query, running_query);
}

void ExecuteRunningQuery(const mgp::QueryExecution &query_execution, std::string running_query,
                         const mgp::ExecutionHeaders &headers, const mgp::List &batch) {
  if (batch.Empty()) {
    return;
  }

  auto param_names = ExtractParamNames(headers, batch[0].ValueList());
  auto prefix_query = ConstructQueryPrefix(param_names);
  auto final_query = ConstructFinalQuery(running_query, prefix_query);

  auto query_params = ConstructQueryParams(headers, batch);

  auto execution_result = query_execution.ExecuteQuery(final_query, query_params);

  while (execution_result.PullOne()) {
  }
}

void ValidateBatchSizeFromConfig(const mgp::Map &config) {
  auto batch_size_key = std::string(kConfigKeyBatchSize);

  if (!config.KeyExists(batch_size_key)) {
    throw std::runtime_error(fmt::format("Configuration parameter {} is not set.", kConfigKeyBatchSize));
  }

  auto batch_size_value = config.At(batch_size_key);
  if (!batch_size_value.IsInt()) {
    throw std::runtime_error("Batch size needs to be an integer!");
  }

  if (batch_size_value.ValueInt() <= 0) {
    throw std::runtime_error("Batch size can't be a non-negative integer!");
  }
}

void ValidateDeletionConfigEntities(const mgp::Map &config, std::string config_key) {
  auto key = std::string_view(config_key);
  if (!config.KeyExists(key)) {
    return;
  }

  auto value = config.At(key);
  if (!value.IsString() && !value.IsList()) {
    throw std::runtime_error(fmt::format("Invalid config for config parameter {}!", config_key));
  }

  if (value.IsString()) {
    return;
  }

  auto list_value = value.ValueList();
  for (auto elem : list_value) {
    if (!elem.IsString()) {
      throw std::runtime_error(fmt::format("Invalid config for config parameter {}!", config_key));
    }
  }
}

void ValidateDeletionConfig(const mgp::Map &config) {
  auto labels_key = std::string(kConfigKeyLabels);
  auto edge_types_key = std::string(kConfigKeyEdgeTypes);

  ValidateBatchSizeFromConfig(config);
  ValidateDeletionConfigEntities(config, labels_key);
  ValidateDeletionConfigEntities(config, edge_types_key);
}

void EmplaceFromConfig(const mgp::Map &config, std::vector<std::string> &vec, std::string &config_key) {
  auto key = std::string_view(config_key);
  if (!config.KeyExists(key)) {
    return;
  }

  auto value = config.At(key);
  if (value.IsString()) {
    vec.emplace_back(value.ValueString());
  } else if (value.IsList()) {
    auto list_value = value.ValueList();
    for (const auto elem : list_value) {
      vec.emplace_back(elem.ValueString());
    }
  }
}

DeletionInfo GetDeletionInfo(const mgp::Map &config) {
  std::vector<std::string> labels;
  std::vector<std::string> edge_types;

  ValidateDeletionConfig(config);

  auto batch_size_key = std::string(kConfigKeyBatchSize);
  auto labels_key = std::string(kConfigKeyLabels);
  auto edge_types_key = std::string(kConfigKeyEdgeTypes);

  auto batch_size = config.At(batch_size_key).ValueInt();

  EmplaceFromConfig(config, labels, labels_key);
  EmplaceFromConfig(config, edge_types, edge_types_key);

  return {.batch_size = static_cast<uint64_t>(batch_size),
          .labels = std::move(labels),
          .edge_types = std::move(edge_types)};
}

void ExecutePeriodicDelete(const mgp::QueryExecution &query_execution, const DeletionInfo &deletion_info,
                           DeletionResult &deletion_result) {
  auto delete_all = deletion_info.edge_types.empty() && deletion_info.labels.empty();
  auto delete_nodes = delete_all || !deletion_info.labels.empty();
  auto delete_edges = delete_all || !deletion_info.labels.empty() || !deletion_info.edge_types.empty();

  auto labels_formatted = deletion_info.labels.empty() ? "" : fmt::format(":{}", Join(deletion_info.labels, ":"));
  auto edge_types_formatted =
      deletion_info.edge_types.empty() ? "" : fmt::format(":{}", Join(deletion_info.edge_types, "|"));

  auto relationships_deletion_query =
      fmt::format("MATCH (n{})-[r{}]-(m) WITH DISTINCT r LIMIT {} DELETE r RETURN count(r) AS num_deleted",
                  labels_formatted, edge_types_formatted, deletion_info.batch_size);
  auto nodes_deletion_query =
      fmt::format("MATCH (n{}) WITH DISTINCT n LIMIT {} DETACH DELETE n RETURN count(n) AS num_deleted",
                  labels_formatted, deletion_info.batch_size);

  if (delete_edges) {
    while (true) {
      auto execution_result = query_execution.ExecuteQuery(relationships_deletion_query);

      auto result = execution_result.PullOne();
      if (!result || (*result).Size() != 1) {
        throw std::runtime_error("No result received from periodic delete!");
      }

      while (execution_result.PullOne()) {
      };

      const uint64_t num_deleted = static_cast<uint64_t>((*result).At(kReturnInternalNumDeleted).ValueInt());
      deletion_result.num_batches++;
      deletion_result.num_deleted_relationships += num_deleted;
      if (num_deleted < deletion_info.batch_size) {
        break;
      }
    }
  }

  if (delete_nodes) {
    while (true) {
      auto execution_result = query_execution.ExecuteQuery(nodes_deletion_query);

      auto result = execution_result.PullOne();
      if (!result || (*result).Size() != 1) {
        throw std::runtime_error("No result received from periodic delete!");
      }

      while (execution_result.PullOne()) {
      };

      const uint64_t num_deleted = static_cast<uint64_t>((*result).At(kReturnInternalNumDeleted).ValueInt());
      deletion_result.num_batches++;
      deletion_result.num_deleted_nodes += num_deleted;
      if (num_deleted < deletion_info.batch_size) {
        break;
      }
    }
  }
}

void PeriodicDelete(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);

  const auto record_factory = mgp::RecordFactory(result);
  auto record = record_factory.NewRecord();

  const auto config = arguments[0].ValueMap();

  DeletionResult deletion_result;

  try {
    auto query_execution = mgp::QueryExecution(memgraph_graph);

    auto deletion_info = GetDeletionInfo(config);

    ExecutePeriodicDelete(query_execution, deletion_info, deletion_result);

    record.Insert(kReturnSuccess, true);
    record.Insert(kReturnNumBatches, static_cast<int64_t>(deletion_result.num_batches));
    record.Insert(kReturnNumDeletedNodes, static_cast<int64_t>(deletion_result.num_deleted_nodes));
    record.Insert(kReturnNumDeletedRelationships, static_cast<int64_t>(deletion_result.num_deleted_relationships));
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    record.Insert(kReturnSuccess, false);
    record.Insert(kReturnNumBatches, static_cast<int64_t>(deletion_result.num_batches));
    record.Insert(kReturnNumDeletedNodes, static_cast<int64_t>(deletion_result.num_deleted_nodes));
    record.Insert(kReturnNumDeletedRelationships, static_cast<int64_t>(deletion_result.num_deleted_relationships));
  }
}

void PeriodicIterate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);

  auto num_of_executed_batches = 0;
  const auto record_factory = mgp::RecordFactory(result);
  auto record = record_factory.NewRecord();

  const auto input_query = std::string(arguments[0].ValueString());
  const auto running_query = std::string(arguments[1].ValueString());
  const auto config = arguments[2].ValueMap();

  try {
    ValidateBatchSizeFromConfig(config);
    const auto batch_size = config.At(kConfigKeyBatchSize).ValueInt();

    auto input_query_execution = mgp::QueryExecution(memgraph_graph);
    auto execution_result = input_query_execution.ExecuteQuery(input_query);

    auto headers = execution_result.Headers();

    mgp::List batch(batch_size);
    int rows = 0;
    while (const auto maybe_result = execution_result.PullOne()) {
      if ((*maybe_result).Size() == 0) {
        break;
      }

      const auto &result = *maybe_result;
      mgp::List row(result.Size());
      for (const auto &header : headers) {
        row.Append(result.At(header));
      }
      batch.Append(mgp::Value(std::move(row)));

      rows++;

      if (rows == batch_size) {
        auto iterative_query_execution = mgp::QueryExecution(memgraph_graph);
        ExecuteRunningQuery(iterative_query_execution, running_query, headers, batch);
        num_of_executed_batches++;
        rows = 0;
        batch = mgp::List(batch_size);
      }
    }

    if (!batch.Empty()) {
      auto iterative_query_execution = mgp::QueryExecution(memgraph_graph);
      ExecuteRunningQuery(iterative_query_execution, running_query, headers, batch);
      num_of_executed_batches++;
    }

    record.Insert(kReturnSuccess, true);
    record.Insert(kReturnNumBatches, static_cast<int64_t>(num_of_executed_batches));
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    record.Insert(kReturnSuccess, false);
    record.Insert(kReturnNumBatches, static_cast<int64_t>(num_of_executed_batches));
  }
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};
    mgp::AddProcedure(
        PeriodicIterate, kProcedurePeriodicIterate, mgp::ProcedureType::Read,
        {mgp::Parameter(kArgumentInputQuery, mgp::Type::String),
         mgp::Parameter(kArgumentRunningQuery, mgp::Type::String), mgp::Parameter(kArgumentConfig, mgp::Type::Map)},
        {mgp::Return(kReturnSuccess, mgp::Type::Bool), mgp::Return(kReturnNumBatches, mgp::Type::Int)}, module, memory);

    mgp::AddProcedure(PeriodicDelete, kProcedurePeriodicDelete, mgp::ProcedureType::Read,
                      {mgp::Parameter(kArgumentConfig, mgp::Type::Map)},
                      {mgp::Return(kReturnSuccess, mgp::Type::Bool), mgp::Return(kReturnNumBatches, mgp::Type::Int),
                       mgp::Return(kReturnNumDeletedNodes, mgp::Type::Int),
                       mgp::Return(kReturnNumDeletedRelationships, mgp::Type::Int)},
                      module, memory);
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
