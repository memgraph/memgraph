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
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace Search {
namespace {

// Comparison operator, mirroring the set accepted by the equivalent name-mapped procedure.
enum class Op {
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
  std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
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
bool Matches(const mgp::Value &property, Op op, std::string_view value) {
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
      return is_string && std::regex_match(std::string{s}, std::regex{std::string{value}});
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
              std::string_view value, Emit &&emit) {
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
    if (Matches(node.GetProperty(property), op, value)) emit(node);
  }
}

template <typename Emit>
void ScanPath(const mgp::Graph &graph, std::string_view label, const std::string &property, Op op,
              std::string_view value, Emit &&emit) {
  for (const auto node : graph.Nodes()) {
    if (node.HasLabel(label) && Matches(node.GetProperty(property), op, value)) emit(node);
  }
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
    throw mgp::ValueException("label_property_map cannot be null. Example: {Person: [\"name\"], Company: \"name\"}");
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
void Run(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory, bool deduplicate) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const mgp::Graph graph{memgraph_graph};
    const auto label_properties = ParseLabelPropertyMap(arguments[0]);
    const Op op = ParseOperator(arguments[1].ValueString());
    if (arguments[2].IsNull()) return;  // a null value matches nothing (checked after operator validation)
    const std::string_view value = arguments[2].ValueString();

    std::unordered_set<int64_t> seen;
    const auto emit = [&](const mgp::Node &node) {
      if (deduplicate && !seen.insert(node.Id().AsInt()).second) return;
      auto record = record_factory.NewRecord();
      record.Insert(std::string{kResultNode}.c_str(), node);
    };

    for (const auto &[label, properties] : label_properties) {
      for (const auto &property : properties) {
        if (IsIndexable(op) && graph.HasLabelPropertyIndex(label, property)) {
          FastPath(graph, label, property, op, value, emit);
        } else {
          ScanPath(graph, label, property, op, value, emit);
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
