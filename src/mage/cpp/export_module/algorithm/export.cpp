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
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
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
  // `file` is written verbatim: Memgraph has no import-directory concept to resolve a relative path against. It is
  // unset for an empty path, which means "no file" — but `echoed_file` still reports the argument as given, because
  // the `file` result column echoes it rather than reporting the sink.
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

// Accepts the unambiguous spellings the reference coerces ('true'/'yes'/'1'/1, 'false'/'no'/'0'/0/'') so ported
// queries keep working, but throws on anything else. The reference instead coerces every unrecognized value to
// *true* — so `{stream: 'ture'}` there quietly turns streaming on rather than off — and a typo should be reported,
// not guessed at in either direction.
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
  // An explicit null config means "no config", as it does on the reference; the registered type allows it.
  if (!config_arg.IsNull() && !config_arg.IsMap()) throw mgp::ValueException("Argument 'config' must be a map or null");
  const auto config = config_arg.IsNull() ? mgp::Map{} : config_arg.ValueMap();

  for (const auto *unsupported : {kConfigCompression, kConfigCharset}) {
    // Rejected rather than ignored: a caller asking for gzip must not silently receive plaintext.
    if (!config.At(unsupported).IsNull()) {
      throw mgp::ValueException(fmt::format("Config '{}' is not supported by the export module", unsupported));
    }
  }

  Options options;
  if (!file_arg.IsNull()) {
    if (!file_arg.IsString()) throw mgp::ValueException("Argument 'file' must be a string or null");
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

void WriteFile(const std::string &path, const std::string &payload) {
  std::ofstream out(path, std::ios::binary | std::ios::trunc);
  if (!out) throw mgp::ValueException(fmt::format("Cannot open '{}' for writing", path));
  out << payload;
  // Closed explicitly: anything still buffered is flushed here, and the destructor could not report a failure. Without
  // this, a payload smaller than the stream buffer reports success on a full disk having written nothing.
  out.close();
  if (!out) throw mgp::ValueException(fmt::format("Failed writing to '{}'", path));
}

// Non-owning view of one argument. `mgp::List(args)` would deep-copy the entire argument list — every node and
// relationship in it included — before a byte is serialized, and `Value::ValueList()` would copy the element list a
// second time. Both are pure waste here: the arguments outlive the call.
mgp::Value Argument(mgp_list *args, size_t index) { return mgp::Value(mgp::ref_type, mgp::list_at(args, index)); }

// Reads a nodes/relationships argument, tolerating null (the reference coerces it to an empty list). Returns the
// caller's list rather than a copy; null means "absent", which reads as empty.
mgp_list *OptionalList(const mgp::Value &value, const char *name) {
  if (value.IsNull()) return nullptr;
  if (!value.IsList()) throw mgp::ValueException(fmt::format("Argument '{}' must be a list or null", name));
  return mgp::value_get_list(value.ptr());
}

void AddNodes(JsonWriter &writer, const mgp::Graph &graph, mgp_list *nodes, const char *name) {
  if (nodes == nullptr) return;
  const auto size = mgp::list_size(nodes);
  for (size_t i = 0; i < size; ++i) {
    graph.CheckMustAbort();
    const auto value = mgp::Value(mgp::ref_type, mgp::list_at(nodes, i));
    if (!value.IsNode()) throw mgp::ValueException(fmt::format("Argument '{}' must contain only nodes", name));
    writer.AddNode(value.ValueNode());
  }
}

void AddRelationships(JsonWriter &writer, const mgp::Graph &graph, mgp_list *relationships, const char *name) {
  if (relationships == nullptr) return;
  const auto size = mgp::list_size(relationships);
  for (size_t i = 0; i < size; ++i) {
    graph.CheckMustAbort();
    const auto value = mgp::Value(mgp::ref_type, mgp::list_at(relationships, i));
    if (!value.IsRelationship())
      throw mgp::ValueException(fmt::format("Argument '{}' must contain only relationships", name));
    writer.AddRelationship(value.ValueRelationship());
  }
}

// The relationship half of a graph map. `relationships` is the documented key; `edges` is what Memgraph's own
// project() produces, and reading only the former turned that into a silent, complete loss of the relationships.
mgp::Value GraphRelationships(mgp_map *graph_map, const char *&name) {
  const auto at = [graph_map](const char *key) {
    auto *value = mgp::map_at(graph_map, key);
    return value != nullptr ? mgp::Value(mgp::ref_type, value) : mgp::Value();
  };
  if (auto relationships = at(kGraphKeyRelationships); !relationships.IsNull()) {
    name = kGraphKeyRelationships;
    return relationships;
  }
  name = kGraphKeyEdges;
  return at(kGraphKeyEdges);
}

// mgp::Record::Insert has no Type::Null case, and `file`/`data` are both nullable columns, so those two go in through
// the low-level insert.
void InsertNullable(mgp_result_record *record, const char *field, const std::optional<std::string> &value) {
  // Value(std::string_view) copies into the mgp_value, and result_record_insert copies again into a TypedValue, so the
  // temporary is safe to drop here.
  const auto wrapped = value ? mgp::Value(*value) : mgp::Value();
  mgp::result_record_insert(record, field, wrapped.ptr());
}

// Renders the payload to its sink and emits the 12-column row. `data` carries the payload only when there is no file
// and streaming was asked for; a file argument wins over `stream` (measured).
void EmitResult(mgp_result *result, JsonWriter &writer, const Options &options, std::string_view source_prefix,
                std::chrono::steady_clock::time_point started_at) {
  const bool streaming = !options.file && options.stream;
  // Rendered only when it has somewhere to go: with neither a file nor streaming the row carries counters alone, and
  // those do not come from the payload.
  std::optional<std::string> payload;
  if (options.file || streaming) payload = std::move(writer).Dump();
  if (options.file) WriteFile(*options.file, *payload);

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
  // `data` carries the payload only when it was streamed; a file sink reports null.
  if (!streaming) payload.reset();
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
    JsonWriter writer(options.write);
    AddNodes(writer, graph, OptionalList(Argument(args, 0), kArgumentNodes), kArgumentNodes);
    AddRelationships(writer, graph, OptionalList(Argument(args, 1), kArgumentRelationships), kArgumentRelationships);
    EmitResult(result, writer, options, kSourcePrefixData, started_at);
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
    JsonWriter writer(options.write);
    // Unlike json_data/json_graph the input here is unbounded, so honour cancellation and the query timeout.
    for (const auto node : graph.Nodes()) {
      graph.CheckMustAbort();
      writer.AddNode(node);
    }
    for (const auto relationship : graph.Relationships()) {
      graph.CheckMustAbort();
      writer.AddRelationship(relationship);
    }
    EmitResult(result, writer, options, kSourcePrefixDatabase, started_at);
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
    const auto graph_arg = Argument(args, 0);
    if (!graph_arg.IsMap()) throw mgp::ValueException("Argument 'graph' must be a map");
    auto *graph_map = mgp::value_get_map(graph_arg.ptr());
    const mgp::Graph graph{memgraph_graph};
    JsonWriter writer(options.write);
    const char *relationships_key = nullptr;
    const auto relationships = GraphRelationships(graph_map, relationships_key);
    auto *nodes = mgp::map_at(graph_map, kGraphKeyNodes);
    // Named for the map keys, not the json_data argument names: the offending thing here is graph['relationships'].
    AddNodes(writer,
             graph,
             OptionalList(nodes != nullptr ? mgp::Value(mgp::ref_type, nodes) : mgp::Value(), kGraphKeyNodes),
             kGraphKeyNodes);
    AddRelationships(writer, graph, OptionalList(relationships, relationships_key), relationships_key);
    EmitResult(result, writer, options, kSourcePrefixGraph, started_at);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

}  // namespace Export
