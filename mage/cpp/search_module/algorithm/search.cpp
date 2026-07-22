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

#include <nlohmann/json.hpp>

namespace Search {
namespace {

// One label and the properties searched for it. A property list is a disjunction (matches are unioned).
using LabelProperties = std::vector<std::pair<std::string, std::vector<std::string>>>;

constexpr std::string_view kWhitespace = " \t\n\r\f\v";

std::string NormalizeOperator(std::string_view raw) {
  const auto begin = raw.find_first_not_of(kWhitespace);
  if (begin == std::string_view::npos) return "";
  const auto end = raw.find_last_not_of(kWhitespace);
  std::string normalized{raw.substr(begin, end - begin + 1)};
  std::ranges::transform(
      normalized, normalized.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return normalized;
}

// Map the accepted operator to its query spelling. `exact` becomes `=`. Throws on an unknown operator.
std::string ComparisonOperator(std::string_view raw) {
  const std::string op = NormalizeOperator(raw);
  if (op == "=" || op == "exact") return "=";
  if (op == "<>") return "<>";
  if (op == "<") return "<";
  if (op == "<=") return "<=";
  if (op == ">") return ">";
  if (op == ">=") return ">=";
  if (op == "starts with") return "STARTS WITH";
  if (op == "ends with") return "ENDS WITH";
  if (op == "contains") return "CONTAINS";
  if (op == "=~") return "=~";
  throw mgp::ValueException(
      "operator `" + op +
      "` invalid, it must be one of (case insensitive): [<=, =~, contains, <>, ends with, starts with, exact, <, =, "
      ">, >=].");
}

// Escape a backtick-quoted identifier: a backtick inside is doubled.
std::string EscapeIdentifier(std::string_view name) {
  std::string escaped;
  escaped.reserve(name.size());
  for (const char c : name) {
    escaped.push_back(c);
    if (c == '`') escaped.push_back('`');
  }
  return escaped;
}

// Parse the label-property map's JSON-string form: an object whose values are a string or an array of
// strings (e.g. {"Person":"name","Movie":["title","tagline"]}).
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

// Shared driver for both procedures. `deduplicate` distinguishes `node` (by node id) from `node_all`.
// Each (label, property) pair is turned into a query and executed via the interpreter, which selects the
// index. A property list per label is a disjunction; matches across properties are unioned.
void Run(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, bool deduplicate) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto label_properties = ParseLabelPropertyMap(arguments[0]);
    const std::string comparison = ComparisonOperator(arguments[1].ValueString());
    if (arguments[2].IsNull()) return;  // a null value matches nothing (checked after operator validation)
    const std::string_view value = arguments[2].ValueString();

    std::unordered_set<int64_t> seen;
    const auto emit = [&](const mgp::Node &node) {
      if (deduplicate && !seen.insert(node.Id().AsInt()).second) return;
      auto record = record_factory.NewRecord();
      record.Insert(kResultNode, node);
    };

    const mgp::QueryExecution query_execution{memgraph_graph};
    for (const auto &[label, properties] : label_properties) {
      for (const auto &property : properties) {
        std::string query = "MATCH (n:`";
        query += EscapeIdentifier(label);
        query += "`) WHERE n.`";
        query += EscapeIdentifier(property);
        query += "` ";
        query += comparison;
        query += " $value RETURN n";

        mgp::Map params;
        params.Insert("value", mgp::Value(value));

        auto results = query_execution.ExecuteQuery(query, params);
        while (const auto row = results.PullOne()) {
          if (row->Size() == 0) break;
          emit(row->At("n").ValueNode());
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
