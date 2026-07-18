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

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include <fmt/core.h>
#include <mgp.hpp>

// Helpers shared by modules that run a user-supplied Cypher statement in a fresh
// transaction. Graph entities (nodes/relationships) cannot be passed as query
// parameters directly, so they are rebound by ID via a generated query prefix.
namespace execute_query_utils {

struct ParamNames {
  std::vector<std::string> node_names;
  std::vector<std::string> relationship_names;
  std::vector<std::string> primitive_names;
};

struct QueryResults {
  mgp::ExecutionHeaders columns;
  mgp::List results;
};

inline ParamNames ExtractParamNames(const mgp::Map &parameters) {
  ParamNames res;
  for (const auto &map_item : parameters) {
    switch (map_item.value.Type()) {
      case mgp::Type::Node:
        res.node_names.emplace_back(map_item.key);
        break;
      case mgp::Type::Relationship:
        res.relationship_names.emplace_back(map_item.key);
        break;
      default:
        res.primitive_names.emplace_back(map_item.key);
    }
  }

  return res;
}

inline std::string Join(std::vector<std::string> const &strings, std::string_view delimiter) {
  if (strings.empty()) {
    return "";
  }

  auto joined_strings_size = strings[0].size();
  for (size_t i = 1; i < strings.size(); i++) {
    joined_strings_size += strings[i].size();
  }

  std::string joined_strings;
  joined_strings.reserve(joined_strings_size + (delimiter.size() * (strings.size() - 1)));

  joined_strings += strings[0];
  for (size_t i = 1; i < strings.size(); i++) {
    joined_strings += delimiter;
    joined_strings += strings[i];
  }

  return joined_strings;
}

inline std::string GetGraphFirstClassEntityAlias(const std::string &entity_name) {
  return fmt::format("${0} AS __{0}_id", entity_name);
}

inline std::string GetPrimitiveEntityAlias(const std::string &primitive_name) {
  return fmt::format("${0} AS {0}", primitive_name);
}

inline std::string ConstructWithStatement(const ParamNames &names) {
  std::vector<std::string> with_entity_vector;
  with_entity_vector.reserve(names.node_names.size() + names.relationship_names.size() + names.primitive_names.size());
  for (const auto &node_name : names.node_names) {
    with_entity_vector.emplace_back(GetGraphFirstClassEntityAlias(node_name));
  }
  for (const auto &rel_name : names.relationship_names) {
    with_entity_vector.emplace_back(GetGraphFirstClassEntityAlias(rel_name));
  }
  for (const auto &prim_name : names.primitive_names) {
    with_entity_vector.emplace_back(GetPrimitiveEntityAlias(prim_name));
  }

  return fmt::format("WITH {}", Join(with_entity_vector, ", "));
}

inline std::string ConstructMatchingNodeById(const std::string &node_name) {
  return fmt::format("MATCH ({0}) WHERE ID({0}) = __{0}_id", node_name);
}

inline std::string ConstructMatchingRelationshipById(const std::string &rel_name) {
  return fmt::format("MATCH ()-[{0}]->() WHERE ID({0}) = __{0}_id", rel_name);
}

inline std::string ConstructMatchGraphEntitiesById(const ParamNames &names) {
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

// NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
inline std::string ConstructQueryPreffix(const ParamNames &names) {
  if (names.node_names.empty() && names.relationship_names.empty() && names.primitive_names.empty()) {
    return {};
  }

  auto with_variables = ConstructWithStatement(names);
  auto match_string = ConstructMatchGraphEntitiesById(names);

  return fmt::format("{} {}",
                     with_variables,
                     match_string);  // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)
}

inline std::string ConstructFinalQuery(const std::string &running_query, const std::string &preffix_query) {
  return fmt::format("{} {}", preffix_query, running_query);
}

inline QueryResults ExecuteQuery(const mgp::QueryExecution &query_execution, const std::string &query,
                                 const mgp::Map &query_parameters) {
  auto param_names = ExtractParamNames(query_parameters);
  auto preffix_query = ConstructQueryPreffix(param_names);
  auto final_query = ConstructFinalQuery(query, preffix_query);

  auto results = query_execution.ExecuteQuery(final_query, query_parameters);

  auto headers = results.Headers();

  mgp::List result_list;
  while (const auto maybe_result = results.PullOne()) {
    if ((*maybe_result).Size() == 0) {
      break;
    }

    const auto &result = *maybe_result;
    result_list.AppendExtend(mgp::Value(result.Values()));
  }

  return QueryResults{.columns = headers, .results = std::move(result_list)};
}

// Emits one record per result row, each row wrapped as a map under `return_name`.
inline void InsertQueryResults(const mgp::RecordFactory &record_factory, const QueryResults &query_results,
                               std::string_view return_name) {
  for (const auto &result : query_results.results) {
    auto record = record_factory.NewRecord();
    record.Insert(std::string(return_name).c_str(), mgp::Value(result.ValueMap()));
  }
}

}  // namespace execute_query_utils
