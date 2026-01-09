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

#include <algorithm>
#include <ranges>
#include <string_view>

#include <fmt/core.h>
#include <mgp.hpp>

constexpr std::string_view kProcedureCase = "case";
constexpr std::string_view kArgumentConditionals = "conditionals";
constexpr std::string_view kArgumentElseQuery = "else_query";
constexpr std::string_view kArgumentParams = "params";

constexpr std::string_view kProcedureWhen = "when";
constexpr std::string_view kArgumentCondition = "condition";
constexpr std::string_view kArgumentIfQuery = "if_query";

constexpr std::string_view kReturnValue = "value";

const std::vector<std::string_view> kGlobalOperations = {"CREATE INDEX ON",
                                                         "DROP INDEX ON",
                                                         "CREATE CONSTRAINT ON",
                                                         "DROP CONSTRAINT ON",
                                                         "SET GLOBAL TRANSACTION ISOLATION LEVEL",
                                                         "STORAGE MODE IN_MEMORY_TRANSACTIONAL",
                                                         "STORAGE MODE IN_MEMORY_ANALYTICAL"};

struct ParamNames {
  std::vector<std::string> node_names;
  std::vector<std::string> relationship_names;
  std::vector<std::string> primitive_names;
};

struct QueryResults {
  mgp::ExecutionHeaders columns;
  mgp::List results;
};

namespace {
ParamNames ExtractParamNames(const mgp::Map &parameters) {
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

std::string Join(std::vector<std::string> const &strings, std::string_view delimiter) {
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

std::string GetGraphFirstClassEntityAlias(const std::string &entity_name) {
  return fmt::format("${0} AS __{0}_id", entity_name);
}

std::string GetPrimitiveEntityAlias(const std::string &primitive_name) {
  return fmt::format("${0} AS {0}", primitive_name);
}

std::string ConstructWithStatement(const ParamNames &names) {
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

// NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
std::string ConstructQueryPreffix(const ParamNames &names) {
  if (names.node_names.empty() && names.relationship_names.empty() && names.primitive_names.empty()) {
    return {};
  }

  auto with_variables = ConstructWithStatement(names);
  auto match_string = ConstructMatchGraphEntitiesById(names);

  return fmt::format("{} {}", with_variables,
                     match_string);  // NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)
}

// NOLINTNEXTLINE(clang-diagnostic-unused-function)
std::string ConstructPreffixQuery(const mgp::Map &parameters) {
  const auto param_names = ExtractParamNames(parameters);
  // NOLINTNEXTLINE(clang-analyzer-optin.cplusplus.UninitializedObject)
  return ConstructQueryPreffix(param_names);
}

std::string ConstructFinalQuery(const std::string &running_query, const std::string &preffix_query) {
  return fmt::format("{} {}", preffix_query, running_query);
}

QueryResults ExecuteQuery(const mgp::QueryExecution &query_execution, const std::string &query,
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

void InsertConditionalResults(const mgp::RecordFactory &record_factory, const QueryResults &query_results) {
  for (const auto &result : query_results.results) {
    auto record = record_factory.NewRecord();
    record.Insert(std::string(kReturnValue).c_str(), mgp::Value(result.ValueMap()));
  }
}

bool IsGlobalOperation(std::string_view query) {
  return std::ranges::any_of(kGlobalOperations,
                             [&query](const auto &global_op) { return query.starts_with(global_op); });
}

void When(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};

  const auto arguments = mgp::List(args);
  const auto condition = arguments[0].ValueBool();

  const auto &query_to_execute = std::string(arguments[condition ? 1 : 2].ValueString());
  const auto params = arguments[3].ValueMap();

  const auto record_factory = mgp::RecordFactory(result);

  try {
    if (IsGlobalOperation(query_to_execute)) {
      throw std::runtime_error(fmt::format(
          "The query {} isn’t supported by `do.when` because it would execute a global operation.", query_to_execute));
    }

    const auto query_execution = mgp::QueryExecution(memgraph_graph);
    const auto query_results = ExecuteQuery(query_execution, query_to_execute, params);
    InsertConditionalResults(record_factory, query_results);
    return;
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Case(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};

  const auto arguments = mgp::List(args);
  const auto conditionals = arguments[0].ValueList();
  const auto else_query = std::string(arguments[1].ValueString());
  const auto params = arguments[2].ValueMap();

  if (conditionals.Empty()) {
    throw std::runtime_error("Conditionals list must not be empty!");
  }

  const auto conditionals_size = conditionals.Size();

  if (conditionals_size % 2) {
    throw std::runtime_error("Size of the conditionals size must be even!");
  }

  for (size_t i = 0; i < conditionals_size; i++) {
    if (!(i % 2) && !conditionals[i].IsBool()) {
      throw std::runtime_error(fmt::format("Argument on index {} in do.case conditionals is not bool!", i));
    }
    if (i % 2 && !conditionals[i].IsString()) {
      throw std::runtime_error(fmt::format("Argument on index {} in do.case conditionals is not string!", i));
    }
  }

  auto found_true_conditional = -1;
  for (size_t i = 0; i < conditionals_size; i += 2) {
    const auto conditional = conditionals[i].ValueBool();
    if (conditional) {
      found_true_conditional = static_cast<int>(i);
      break;
    }
  }

  const auto query_to_execute =
      found_true_conditional != -1 ? std::string(conditionals[found_true_conditional + 1].ValueString()) : else_query;

  const auto record_factory = mgp::RecordFactory(result);

  try {
    if (IsGlobalOperation(query_to_execute)) {
      throw std::runtime_error(fmt::format(
          "The query {} isn’t supported by `do.case` because it would execute a global operation.", query_to_execute));
    }

    const auto query_execution = mgp::QueryExecution(memgraph_graph);
    const auto query_results = ExecuteQuery(query_execution, query_to_execute, params);
    InsertConditionalResults(record_factory, query_results);
    return;
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    mgp::AddProcedure(Case, kProcedureCase, mgp::ProcedureType::Read,
                      {mgp::Parameter(kArgumentConditionals, {mgp::Type::List, mgp::Type::Any}),
                       mgp::Parameter(kArgumentElseQuery, mgp::Type::String),
                       mgp::Parameter(kArgumentParams, mgp::Type::Map, mgp::Value(mgp::Map()))},
                      {mgp::Return(kReturnValue, mgp::Type::Map)}, module, memory);

    mgp::AddProcedure(
        When, kProcedureWhen, mgp::ProcedureType::Read,
        {mgp::Parameter(kArgumentCondition, mgp::Type::Bool), mgp::Parameter(kArgumentIfQuery, mgp::Type::String),
         mgp::Parameter(kArgumentElseQuery, mgp::Type::String),
         mgp::Parameter(kArgumentParams, mgp::Type::Map, mgp::Value(mgp::Map()))},
        {mgp::Return(kReturnValue, mgp::Type::Map)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
