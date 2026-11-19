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

#include "algorithm/export.hpp"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <fmt/format.h>

#include "algorithm/json_writer.hpp"

namespace Export {
namespace {

constexpr const char *kConfigStream = "stream";
constexpr const char *kConfigWriteNodeProperties = "writeNodeProperties";
constexpr const char *kConfigWriteRelationshipProperties = "writeRelationshipProperties";
constexpr const char *kConfigJsonFormat = "jsonFormat";
constexpr const char *kConfigCompression = "compression";
constexpr const char *kConfigCharset = "charset";

constexpr const char *kFormatJson = "json";
constexpr std::int64_t kBatchSize = -1;
constexpr std::int64_t kBatches = 0;

constexpr const char *kSourcePrefixData = "data";
constexpr const char *kSourcePrefixDatabase = "database";
constexpr const char *kSourcePrefixGraph = "graph";

struct Options {
  // `file` is the sink path, used verbatim and unset for an empty path; `echoed_file` keeps the argument as given
  // for the `file` result column.
  std::optional<std::string> file;
  std::optional<std::string> echoed_file;
  bool stream{false};
  WriteConfig write;
};

std::string Upper(std::string_view text) {
  std::string upper{text};
  std::ranges::transform(upper, upper.begin(), [](unsigned char c) { return std::toupper(c); });
  return upper;
}

// Case-insensitive, matching the reference: the accepted set is closed, so folding case cannot mask a typo.
JsonFormat ParseJsonFormat(std::string_view name) {
  const auto upper = Upper(name);
  if (upper == "JSON_LINES") return JsonFormat::kJsonLines;
  if (upper == "JSON") return JsonFormat::kJson;
  if (upper == "JSON_ID_AS_KEYS") return JsonFormat::kJsonIdAsKeys;
  throw mgp::ValueException(
      fmt::format("Unknown jsonFormat '{}'; expected one of JSON_LINES, JSON, JSON_ID_AS_KEYS", name));
}

// Accepts the spellings the reference coerces (true/false/yes/no/1/0, as booleans, integers or strings, and '' for
// false) and throws on anything else, where the reference reads any unknown value as true.
bool ConfigBool(const mgp::Map &config, const char *key, bool fallback) {
  const auto value = config.At(key);
  if (value.IsNull()) return fallback;
  if (value.IsBool()) return value.ValueBool();
  if (value.IsInt()) {
    const auto number = value.ValueInt();
    if (number == 0 || number == 1) return number == 1;
  } else if (value.IsString()) {
    const auto upper = Upper(value.ValueString());
    if (upper == "TRUE" || upper == "YES" || upper == "1") return true;
    if (upper == "FALSE" || upper == "NO" || upper == "0" || upper.empty()) return false;
  }
  throw mgp::ValueException(fmt::format("Config '{}' must be a boolean (or one of true/false/yes/no/1/0)", key));
}

// Unrecognized keys are ignored, matching the reference's leniency — `useTypes` in particular is measured to be a
// no-op for these procedures, so rejecting it would break otherwise-portable queries.
Options ParseOptions(const mgp::Value &file_arg, const mgp::Value &config_arg) {
  // An explicit null config means "no config", as it does on the reference.
  const auto config = config_arg.IsNull() ? mgp::Map{} : config_arg.ValueMap();

  for (const auto *unsupported : {kConfigCompression, kConfigCharset}) {
    // Rejected rather than ignored: a caller asking for gzip must not silently receive plaintext.
    if (!config.At(unsupported).IsNull()) {
      throw mgp::ValueException(fmt::format("Config '{}' is not supported by the export module", unsupported));
    }
  }

  Options options;
  if (!file_arg.IsNull()) {
    const auto file = std::string{file_arg.ValueString()};
    options.echoed_file = file;
    // An empty path means "no file", matching the reference and the procedures this one supersedes, whose `path`
    // argument defaulted to "".
    if (!file.empty()) options.file = file;
  }
  options.stream = ConfigBool(config, kConfigStream, false);
  options.write.write_node_properties = ConfigBool(config, kConfigWriteNodeProperties, true);
  // writeRelationshipProperties falls back to writeNodeProperties rather than to true (measured): passing only
  // writeNodeProperties:false suppresses relationship properties as well, but an explicit true overrides that.
  options.write.write_relationship_properties =
      ConfigBool(config, kConfigWriteRelationshipProperties, options.write.write_node_properties);

  const auto format = config.At(kConfigJsonFormat);
  if (!format.IsNull()) {
    if (!format.IsString()) throw mgp::ValueException("Config 'jsonFormat' must be a string");
    options.write.format = ParseJsonFormat(format.ValueString());
  }
  return options;
}

// `data` carries the payload only when there is no file and streaming was asked for; a file argument wins over
// `stream` (measured). This also decides whether the writer needs to retain anything at all.
bool Streaming(const Options &options) { return !options.file && options.stream; }

// Non-owning view of one argument: `mgp::List(args)` and `ValueList()` would each deep-copy every node and
// relationship in it, and the arguments outlive the call.
mgp::Value Argument(mgp_list *args, size_t index) { return mgp::Value(mgp::ref_type, mgp::list_at(args, index)); }

// Reads a nodes/relationships argument, tolerating null (the reference coerces it to an empty list). Returns the
// caller's list rather than a copy; null means "absent", which reads as empty.
mgp_list *OptionalList(const mgp::Value &value, const char *name) {
  if (value.IsNull()) return nullptr;
  if (!value.IsList()) throw mgp::ValueException(fmt::format("Argument '{}' must be a list or null", name));
  return mgp::value_get_list(value.ptr());
}

// The procedure signature types json_data's lists; json_graph's come out of a map, so they are checked here before
// any sink is opened.
void RequireElements(mgp_list *list, mgp::Type type, const char *name, std::string_view what) {
  if (list == nullptr) return;
  const auto size = mgp::list_size(list);
  for (size_t i = 0; i < size; ++i) {
    if (mgp::Value(mgp::ref_type, mgp::list_at(list, i)).Type() != type) {
      throw mgp::ValueException(fmt::format("Argument '{}' must contain only {}", name, what));
    }
  }
}

// Under JSON_ID_AS_KEYS ids are object keys, so a node the caller repeats is written once and still counted.
void AddNodes(JsonWriter &writer, const mgp::Graph &graph, mgp_list *nodes, JsonFormat format) {
  if (nodes == nullptr) return;
  const auto size = mgp::list_size(nodes);
  std::unordered_set<std::int64_t> written;
  for (size_t i = 0; i < size; ++i) {
    graph.CheckMustAbort();
    const auto node = mgp::Value(mgp::ref_type, mgp::list_at(nodes, i)).ValueNode();
    if (format != JsonFormat::kJsonIdAsKeys || written.insert(node.Id().AsInt()).second) {
      writer.AddNode(node);
    } else {
      writer.CountNode(node);
    }
  }
}

// Relationships form a set in the interchange format, so a repeated one is neither counted nor written. Only a
// caller's list can repeat one; the whole-database export walks each relationship once and keeps no such set.
void AddRelationships(JsonWriter &writer, const mgp::Graph &graph, mgp_list *relationships) {
  if (relationships == nullptr) return;
  const auto size = mgp::list_size(relationships);
  std::unordered_set<std::int64_t> seen;
  for (size_t i = 0; i < size; ++i) {
    graph.CheckMustAbort();
    const auto relationship = mgp::Value(mgp::ref_type, mgp::list_at(relationships, i)).ValueRelationship();
    if (seen.insert(relationship.Id().AsInt()).second) writer.AddRelationship(relationship);
  }
}

mgp::Value GraphKey(mgp_map *graph_map, const char *key) {
  auto *value = mgp::map_at(graph_map, key);
  return value != nullptr ? mgp::Value(mgp::ref_type, value) : mgp::Value();
}

bool IsPopulated(const mgp::Value &value) {
  return !value.IsNull() && !(value.IsList() && mgp::list_size(mgp::value_get_list(value.ptr())) == 0);
}

// The relationship half of a graph map: `relationships`, or `edges` as project() produces. An empty list does not
// shadow the other key, and two populated keys are refused.
mgp::Value GraphRelationships(mgp_map *graph_map, const char *&name) {
  auto relationships = GraphKey(graph_map, kGraphKeyRelationships);
  auto edges = GraphKey(graph_map, kGraphKeyEdges);
  if (relationships.IsNull() && edges.IsNull()) {
    throw mgp::ValueException(fmt::format("Argument 'graph' has no '{}' key", kGraphKeyRelationships));
  }
  const bool has_edges = IsPopulated(edges);
  if (IsPopulated(relationships) && has_edges) {
    throw mgp::ValueException(fmt::format(
        "Argument 'graph' has both '{}' and '{}'; provide only one", kGraphKeyRelationships, kGraphKeyEdges));
  }
  const bool use_edges = has_edges || relationships.IsNull();
  name = use_edges ? kGraphKeyEdges : kGraphKeyRelationships;
  return use_edges ? std::move(edges) : std::move(relationships);
}

// mgp::Record::Insert has no Type::Null case, and `file`/`data` are both nullable columns, so those two go in through
// the low-level insert.
void InsertNullable(mgp_result_record *record, const char *field, const std::optional<std::string> &value) {
  // Value(std::string_view) copies into the mgp_value, and result_record_insert copies again into a TypedValue, so the
  // temporary is safe to drop here.
  const auto wrapped = value ? mgp::Value(*value) : mgp::Value();
  mgp::result_record_insert(record, field, wrapped.ptr());
}

// Closes the writer's sink and emits the 12-column row. The elements were serialized as they arrived, so this only
// finishes the output — for a file that means flushing and renaming the temporary into place.
void EmitResult(mgp_result *result, JsonWriter &&writer, const Options &options, std::string_view source_prefix,
                std::chrono::steady_clock::time_point started_at) {
  auto retained = std::move(writer).Finish();
  std::optional<std::string> payload;
  if (Streaming(options)) payload = std::move(retained);

  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at);

  auto *raw_record = mgp::result_new_record(result);
  mgp::Record record{raw_record};
  InsertNullable(raw_record, kReturnFile, options.echoed_file);
  record.Insert(kReturnSource,
                fmt::format("{}: nodes({}), rels({})", source_prefix, writer.NodeCount(), writer.RelationshipCount()));
  record.Insert(kReturnFormat, kFormatJson);
  record.Insert(kReturnNodes, static_cast<std::int64_t>(writer.NodeCount()));
  record.Insert(kReturnRelationships, static_cast<std::int64_t>(writer.RelationshipCount()));
  record.Insert(kReturnProperties, static_cast<std::int64_t>(writer.PropertyCount()));
  record.Insert(kReturnTime, static_cast<std::int64_t>(elapsed.count()));
  record.Insert(kReturnRows, static_cast<std::int64_t>(writer.NodeCount() + writer.RelationshipCount()));
  record.Insert(kReturnBatchSize, kBatchSize);
  record.Insert(kReturnBatches, kBatches);
  record.Insert(kReturnDone, true);
  InsertNullable(raw_record, kReturnData, payload);
}

}  // namespace

void JsonData(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto started_at = std::chrono::steady_clock::now();
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto options = ParseOptions(Argument(args, 2), Argument(args, 3));
    const mgp::Graph graph{memgraph_graph};
    JsonWriter writer(options.write, options.file, Streaming(options));
    AddNodes(writer, graph, OptionalList(Argument(args, 0), kArgumentNodes), options.write.format);
    AddRelationships(writer, graph, OptionalList(Argument(args, 1), kArgumentRelationships));
    EmitResult(result, std::move(writer), options, kSourcePrefixData, started_at);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void JsonAll(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto started_at = std::chrono::steady_clock::now();
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto options = ParseOptions(Argument(args, 0), Argument(args, 1));
    const mgp::Graph graph{memgraph_graph};
    JsonWriter writer(options.write, options.file, Streaming(options));
    for (const auto node : graph.Nodes()) {
      graph.CheckMustAbort();
      writer.AddNode(node);
    }
    for (const auto relationship : graph.Relationships()) {
      graph.CheckMustAbort();
      writer.AddRelationship(relationship);
    }
    EmitResult(result, std::move(writer), options, kSourcePrefixDatabase, started_at);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void JsonGraph(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto started_at = std::chrono::steady_clock::now();
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto options = ParseOptions(Argument(args, 1), Argument(args, 2));
    auto *graph_map = mgp::value_get_map(Argument(args, 0).ptr());
    // A missing half would export part of the graph, or none of it, under `done: true`, so both are required.
    const auto nodes_value = GraphKey(graph_map, kGraphKeyNodes);
    if (nodes_value.IsNull())
      throw mgp::ValueException(fmt::format("Argument 'graph' has no '{}' key", kGraphKeyNodes));
    const char *relationships_key = nullptr;
    const auto relationships = GraphRelationships(graph_map, relationships_key);
    // Errors name the map keys, not the json_data argument names.
    auto *nodes = OptionalList(nodes_value, kGraphKeyNodes);
    auto *relationship_list = OptionalList(relationships, relationships_key);
    RequireElements(nodes, mgp::Type::Node, kGraphKeyNodes, "nodes");
    RequireElements(relationship_list, mgp::Type::Relationship, relationships_key, "relationships");
    const mgp::Graph graph{memgraph_graph};
    JsonWriter writer(options.write, options.file, Streaming(options));
    AddNodes(writer, graph, nodes, options.write.format);
    AddRelationships(writer, graph, relationship_list);
    EmitResult(result, std::move(writer), options, kSourcePrefixGraph, started_at);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

}  // namespace Export
