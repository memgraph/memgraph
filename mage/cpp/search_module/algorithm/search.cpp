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

#include "algorithm/search.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

namespace Search {
namespace {

// One label and the properties searched for it. A property list is a disjunction (matches are unioned).
using LabelProperties = std::vector<std::pair<std::string, std::vector<std::string>>>;

constexpr std::string_view kWhitespace = " \t\n\r\f\v";

// Canonical Cypher operators emitted into the sub-query. STARTS WITH / ENDS WITH / CONTAINS are string-only.
namespace op {
constexpr std::string_view kEqual = "=";
constexpr std::string_view kNotEqual = "<>";
constexpr std::string_view kLess = "<";
constexpr std::string_view kLessEqual = "<=";
constexpr std::string_view kGreater = ">";
constexpr std::string_view kGreaterEqual = ">=";
constexpr std::string_view kStartsWith = "STARTS WITH";
constexpr std::string_view kEndsWith = "ENDS WITH";
constexpr std::string_view kContains = "CONTAINS";
constexpr std::string_view kRegex = "=~";
}  // namespace op

std::string NormalizeOperator(std::string_view raw) {
  const auto begin = raw.find_first_not_of(kWhitespace);
  if (begin == std::string_view::npos) return "";
  const auto end = raw.find_last_not_of(kWhitespace);
  std::string normalized{raw.substr(begin, end - begin + 1)};
  std::ranges::transform(
      normalized, normalized.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return normalized;
}

std::string_view ComparisonOperator(std::string_view raw) {
  const std::string normalized = NormalizeOperator(raw);
  if (normalized == "=" || normalized == "exact") return op::kEqual;
  if (normalized == "<>") return op::kNotEqual;
  if (normalized == "<") return op::kLess;
  if (normalized == "<=") return op::kLessEqual;
  if (normalized == ">") return op::kGreater;
  if (normalized == ">=") return op::kGreaterEqual;
  if (normalized == "starts with") return op::kStartsWith;
  if (normalized == "ends with") return op::kEndsWith;
  if (normalized == "contains") return op::kContains;
  if (normalized == "=~") return op::kRegex;
  throw mgp::ValueException(
      "operator `" + normalized +
      "` invalid, it must be one of (case insensitive): [<=, =~, contains, <>, ends with, starts with, exact, <, =, "
      ">, >=].");
}

// STARTS WITH / ENDS WITH / CONTAINS raise a runtime error on a non-string property (unlike the comparison
// operators, which simply do not match), so they must be guarded to only ever be evaluated against strings.
bool IsStringOnlyOperator(std::string_view comparison) {
  return comparison == op::kStartsWith || comparison == op::kEndsWith || comparison == op::kContains;
}

std::string EscapeIdentifier(std::string_view name) {
  std::string escaped;
  escaped.reserve(name.size());
  for (const char c : name) {
    escaped.push_back(c);
    if (c == '`') escaped.push_back('`');
  }
  return escaped;
}

LabelProperties ParseJsonLabelPropertyMap(std::string_view text) {
  nlohmann::json json;
  try {
    json = nlohmann::json::parse(text);
  } catch (const nlohmann::json::parse_error &e) {
    throw mgp::ValueException(std::string{"label_property_map: malformed JSON: "} + e.what());
  }
  if (!json.is_object()) throw mgp::ValueException("label_property_map JSON must be an object");
  LabelProperties result;
  for (const auto &[label, value] : json.items()) {
    std::vector<std::string> properties;
    if (value.is_string()) {
      properties.emplace_back(value.get<std::string>());
    } else if (value.is_array()) {
      for (const auto &element : value) {
        if (!element.is_string()) throw mgp::ValueException("label_property_map values must be strings");
        properties.emplace_back(element.get<std::string>());
      }
    } else {
      throw mgp::ValueException("label_property_map values must be a string or a list of strings");
    }
    result.emplace_back(label, std::move(properties));
  }
  return result;
}

LabelProperties ParseLabelPropertyMap(const mgp::Value &argument) {
  if (argument.IsNull()) {
    throw mgp::ValueException(R"(label_property_map cannot be null. Example: {Person: ["name"], Company: "name"})");
  }
  if (argument.IsString()) return ParseJsonLabelPropertyMap(argument.ValueString());
  if (!argument.IsMap()) {
    throw mgp::ValueException("label_property_map must be a map or a JSON string");
  }
  LabelProperties result;
  for (const auto item : argument.ValueMap()) {
    std::vector<std::string> properties;
    if (item.value.IsString()) {
      properties.emplace_back(item.value.ValueString());
    } else if (item.value.IsList()) {
      for (const auto property : item.value.ValueList()) {
        if (!property.IsString()) throw mgp::ValueException("label_property_map values must be strings");
        properties.emplace_back(property.ValueString());
      }
    } else {
      throw mgp::ValueException("label_property_map values must be a string or a list of strings");
    }
    result.emplace_back(std::string{item.key}, std::move(properties));
  }
  return result;
}

// Shared driver. `deduplicate` distinguishes `node` (dedup by id) from `node_all`; one sub-query per pair.
void Run(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, bool deduplicate) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto label_properties = ParseLabelPropertyMap(arguments[0]);
    const std::string_view comparison = ComparisonOperator(arguments[1].ValueString());
    if (arguments[2].IsNull()) return;  // a null value matches nothing (checked after operator validation)
    const std::string_view value = arguments[2].ValueString();

    std::unordered_set<int64_t> seen;
    // A sub-query node accessor is bound to the sub-query's transaction, freed with its result. Re-resolve each
    // id against the caller's graph in a single lookup so the emitted node outlives that transaction; a null
    // result means the id is not visible in the caller's snapshot, so skip it. The C API is used directly so the
    // freshly-fetched vertex is handed straight to the result value instead of being copied into a wrapper.
    const auto emit = [&](int64_t node_id) {
      if (deduplicate && !seen.insert(node_id).second) return;
      auto *vertex =
          mgp::MemHandlerCallback(mgp::graph_get_vertex_by_id, memgraph_graph, mgp_vertex_id{.as_int = node_id});
      if (vertex == nullptr) return;
      auto *value = mgp::value_make_vertex(vertex);  // takes ownership of vertex
      mgp::result_record_insert(mgp::result_new_record(result), kResultNode, value);
      mgp::value_destroy(value);  // frees the value and the vertex it owns
    };

    // A non-string operand would silently drop rows (the sub-query error surfaces as an empty result), so
    // string-only operators are guarded to never be evaluated against a non-string property.
    const bool string_only = IsStringOnlyOperator(comparison);

    // TODO(ivan): run these sub-queries inside the caller's transaction instead of a fresh one, so the search
    // observes the caller's uncommitted writes and a single snapshot.
    const mgp::QueryExecution query_execution{memgraph_graph};
    for (const auto &[label, properties] : label_properties) {
      for (const auto &property : properties) {
        const std::string node_property = fmt::format("n.`{}`", EscapeIdentifier(property));
        // The guard must be a CASE, not `valueType(...) = 'STRING' AND n.p <op> ...`: the planner does not
        // preserve AND operand order, so the operator can be evaluated before the type check and still throw.
        // A CASE evaluates only the branch it takes, so the operator is applied to string properties only.
        std::string predicate = fmt::format("{} {} $value", node_property, comparison);
        if (string_only) {
          predicate =
              fmt::format("CASE WHEN valueType({}) = 'STRING' THEN {} ELSE false END", node_property, predicate);
        }
        const std::string query =
            fmt::format("MATCH (n:`{}`) WHERE {} RETURN id(n) AS node_id", EscapeIdentifier(label), predicate);

        mgp::Map params;
        params.Insert("value", mgp::Value(value));

        auto results = query_execution.ExecuteQuery(query, params);
        while (const auto row = results.PullOne()) {
          emit(row->At("node_id").ValueInt());
        }
      }
    }
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

}  // namespace

void Node(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  Run(args, memgraph_graph, result, memory, /*deduplicate=*/true);
}

void NodeAll(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  Run(args, memgraph_graph, result, memory, /*deduplicate=*/false);
}

}  // namespace Search
