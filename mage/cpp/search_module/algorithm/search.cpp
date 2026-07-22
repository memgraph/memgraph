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
#include <cstdint>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

namespace Search {
namespace {

// Comparison operator, mirroring the set accepted by the equivalent name-mapped procedure.
enum class Op : std::uint8_t {
  kExact,
  kNotEqual,
  kLess,
  kLessEqual,
  kGreater,
  kGreaterEqual,
  kStartsWith,
  kEndsWith,
  kContains,
  kRegex
};

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

Op ParseOperator(std::string_view raw) {
  const std::string op = NormalizeOperator(raw);
  if (op == "=" || op == "exact") return Op::kExact;
  if (op == "<>") return Op::kNotEqual;
  if (op == "<") return Op::kLess;
  if (op == "<=") return Op::kLessEqual;
  if (op == ">") return Op::kGreater;
  if (op == ">=") return Op::kGreaterEqual;
  if (op == "starts with") return Op::kStartsWith;
  if (op == "ends with") return Op::kEndsWith;
  if (op == "contains") return Op::kContains;
  if (op == "=~") return Op::kRegex;
  throw mgp::ValueException(
      "operator `" + op +
      "` invalid, it must be one of (case insensitive): [<=, =~, contains, <>, ends with, starts with, exact, <, =, "
      ">, >=].");
}

// Only operators whose match set is a single contiguous run in the string type segment can use the index.
bool IsIndexable(Op op) {
  switch (op) {
    case Op::kExact:
    case Op::kLess:
    case Op::kLessEqual:
    case Op::kGreater:
    case Op::kGreaterEqual:
    case Op::kStartsWith:
      return true;
    // TODO: support via text index
    case Op::kNotEqual:
    case Op::kEndsWith:
    case Op::kContains:
    case Op::kRegex:
      return false;
  }
  return false;
}

// The single source of truth for a match, shared by the index fast path and the scan fallback.
// A property matches only when it is a string (except `<>`, which also matches any present non-string,
// as cross-type inequality holds); a property the node lacks never matches.
// `regex` is the precompiled pattern for `=~` (compiled once per call, not per node); null for other ops.
bool Matches(const mgp::Value &property, Op op, std::string_view value, const std::regex *regex) {
  if (property.IsNull()) return false;  // property absent on this node
  const bool is_string = property.IsString();
  const std::string_view s = is_string ? property.ValueString() : std::string_view{};
  switch (op) {
    case Op::kExact:
      return is_string && s == value;
    case Op::kNotEqual:
      return !(is_string && s == value);
    case Op::kLess:
      return is_string && s < value;
    case Op::kLessEqual:
      return is_string && s <= value;
    case Op::kGreater:
      return is_string && s > value;
    case Op::kGreaterEqual:
      return is_string && s >= value;
    case Op::kStartsWith:
      return is_string && s.starts_with(value);
    case Op::kEndsWith:
      return is_string && s.ends_with(value);
    case Op::kContains:
      return is_string && s.find(value) != std::string_view::npos;
    case Op::kRegex:
      return is_string && std::regex_match(s.begin(), s.end(), *regex);
  }
  return false;
}

// Smallest string strictly greater than every string with prefix `prefix` (the prefix "plus one").
// nullopt means no such bound exists (prefix is all 0xFF, or empty) — the upper side stays unbounded.
std::optional<std::string> PrefixSuccessor(std::string_view prefix) {
  std::string successor{prefix};
  while (!successor.empty() && static_cast<unsigned char>(successor.back()) == 0xFF) successor.pop_back();
  if (successor.empty()) return std::nullopt;
  successor.back() = static_cast<char>(static_cast<unsigned char>(successor.back()) + 1);
  return successor;
}

// Iterate the label-property index for `op`/`value`, re-checking each hit against the shared predicate.
// The index range is a superset (open operators can't express the string type-segment boundary), so the
// predicate remains the source of truth; for `=`/`starts with` the range is already tight.
template <typename Emit>
void FastPath(const mgp::Graph &graph, std::string_view label, const std::string &property, Op op,
              std::string_view value, const std::regex *regex, Emit &&emit) {
  std::optional<mgp::Value> lower;
  std::optional<mgp::Value> upper;
  bool lower_inclusive = false;
  bool upper_inclusive = false;
  switch (op) {
    case Op::kExact:
      lower.emplace(value);
      upper.emplace(value);
      lower_inclusive = upper_inclusive = true;
      break;
    case Op::kLess:
      upper.emplace(value);
      break;
    case Op::kLessEqual:
      upper.emplace(value);
      upper_inclusive = true;
      break;
    case Op::kGreater:
      lower.emplace(value);
      break;
    case Op::kGreaterEqual:
      lower.emplace(value);
      lower_inclusive = true;
      break;
    case Op::kStartsWith:
      lower.emplace(value);
      lower_inclusive = true;
      if (auto successor = PrefixSuccessor(value)) upper.emplace(*successor);
      break;
    default:
      return;  // never reached: non-indexable operators use the scan path
  }
  const mgp::Value *lower_ptr = lower ? &*lower : nullptr;
  const mgp::Value *upper_ptr = upper ? &*upper : nullptr;
  for (const auto node :
       graph.NodesByLabelPropertyRange(label, property, lower_ptr, lower_inclusive, upper_ptr, upper_inclusive)) {
    if (Matches(node.GetProperty(property), op, value, regex)) emit(node);
  }
}

template <typename Emit>
void ScanPath(const mgp::Graph &graph, std::string_view label, const std::string &property, Op op,
              std::string_view value, const std::regex *regex, Emit &&emit) {
  for (const auto node : graph.Nodes()) {
    if (node.HasLabel(label) && Matches(node.GetProperty(property), op, value, regex)) emit(node);
  }
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
void Run(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, bool deduplicate) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Graph graph{memgraph_graph};
    const auto label_properties = ParseLabelPropertyMap(arguments[0]);
    const Op op = ParseOperator(arguments[1].ValueString());
    if (arguments[2].IsNull()) return;  // a null value matches nothing (checked after operator validation)
    const std::string_view value = arguments[2].ValueString();

    // Compile the `=~` pattern once per call rather than once per scanned node.
    std::optional<std::regex> regex;
    if (op == Op::kRegex) regex.emplace(value.begin(), value.end());
    const std::regex *const regex_ptr = regex ? &*regex : nullptr;

    const bool indexable = IsIndexable(op);

    std::unordered_set<int64_t> seen;
    const auto emit = [&](const mgp::Node &node) {
      if (deduplicate && !seen.insert(node.Id().AsInt()).second) return;
      auto record = record_factory.NewRecord();
      record.Insert(kResultNode, node);
    };

    for (const auto &[label, properties] : label_properties) {
      for (const auto &property : properties) {
        if (indexable && graph.HasLabelPropertyIndex(label, property)) {
          FastPath(graph, label, property, op, value, regex_ptr, emit);
        } else {
          ScanPath(graph, label, property, op, value, regex_ptr, emit);
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
