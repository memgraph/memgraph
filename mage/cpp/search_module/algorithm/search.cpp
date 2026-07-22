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

// --- Minimal JSON parser for the label-property map string form: an object whose values are a string
// or an array of strings (e.g. {"Person":"name","Movie":["title","tagline"]}). ---
class JsonMapParser {
 public:
  explicit JsonMapParser(std::string_view text) : text_(text) {}

  LabelProperties Parse() {
    LabelProperties result;
    SkipWhitespace();
    Expect('{');
    SkipWhitespace();
    if (Peek() == '}') {
      Advance();
      return result;  // empty object
    }
    while (true) {
      SkipWhitespace();
      std::string label = ParseString();
      SkipWhitespace();
      Expect(':');
      SkipWhitespace();
      std::vector<std::string> properties;
      if (Peek() == '[') {
        Advance();
        SkipWhitespace();
        if (Peek() != ']') {
          while (true) {
            SkipWhitespace();
            properties.push_back(ParseString());
            SkipWhitespace();
            if (Peek() == ',') {
              Advance();
              continue;
            }
            break;
          }
        }
        SkipWhitespace();
        Expect(']');
      } else {
        properties.push_back(ParseString());
      }
      result.emplace_back(std::move(label), std::move(properties));
      SkipWhitespace();
      if (Peek() == ',') {
        Advance();
        continue;
      }
      break;
    }
    SkipWhitespace();
    Expect('}');
    return result;
  }

 private:
  char Peek() const {
    if (pos_ >= text_.size()) throw mgp::ValueException("label_property_map: malformed JSON (unexpected end)");
    return text_[pos_];
  }

  void Advance() { ++pos_; }

  void SkipWhitespace() {
    while (pos_ < text_.size() && std::isspace(static_cast<unsigned char>(text_[pos_]))) ++pos_;
  }

  void Expect(char c) {
    if (Peek() != c) throw mgp::ValueException("label_property_map: malformed JSON");
    Advance();
  }

  std::string ParseString() {
    Expect('"');
    std::string out;
    while (true) {
      if (pos_ >= text_.size()) throw mgp::ValueException("label_property_map: malformed JSON (unterminated string)");
      const char c = text_[pos_++];
      if (c == '"') break;
      if (c == '\\') {
        if (pos_ >= text_.size()) throw mgp::ValueException("label_property_map: malformed JSON (bad escape)");
        const char esc = text_[pos_++];
        switch (esc) {
          case '"':
          case '\\':
          case '/':
            out.push_back(esc);
            break;
          case 'n':
            out.push_back('\n');
            break;
          case 't':
            out.push_back('\t');
            break;
          case 'r':
            out.push_back('\r');
            break;
          case 'b':
            out.push_back('\b');
            break;
          case 'f':
            out.push_back('\f');
            break;
          default:
            throw mgp::ValueException("label_property_map: unsupported JSON escape");
        }
      } else {
        out.push_back(c);
      }
    }
    return out;
  }

  std::string_view text_;
  size_t pos_ = 0;
};

LabelProperties ParseLabelPropertyMap(const mgp::Value &argument) {
  if (argument.IsNull()) {
    throw mgp::ValueException(R"(label_property_map cannot be null. Example: {Person: ["name"], Company: "name"})");
  }
  if (argument.IsString()) return JsonMapParser(argument.ValueString()).Parse();
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
