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

#include <chrono>
#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>

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
  // `file` is written verbatim: Memgraph has no import-directory concept to resolve a relative path against.
  std::optional<std::string> file;
  bool stream{false};
  WriteConfig write;
};

JsonFormat ParseJsonFormat(std::string_view name) {
  if (name == "JSON_LINES") return JsonFormat::kJsonLines;
  if (name == "JSON") return JsonFormat::kJson;
  if (name == "JSON_ID_AS_KEYS") return JsonFormat::kJsonIdAsKeys;
  throw mgp::ValueException(
      fmt::format("Unknown jsonFormat '{}'; expected one of JSON_LINES, JSON, JSON_ID_AS_KEYS", name));
}

bool ConfigBool(const mgp::Map &config, const char *key, bool fallback) {
  const auto value = config.At(key);
  if (value.IsNull()) return fallback;
  if (!value.IsBool()) throw mgp::ValueException(fmt::format("Config '{}' must be a boolean", key));
  return value.ValueBool();
}

// Unrecognized keys are ignored, matching the reference's leniency — `useTypes` in particular is measured to be a
// no-op for these procedures, so rejecting it would break otherwise-portable queries.
Options ParseOptions(const mgp::Value &file_arg, const mgp::Map &config) {
  for (const auto *unsupported : {kConfigCompression, kConfigCharset}) {
    // Rejected rather than ignored: a caller asking for gzip must not silently receive plaintext.
    if (!config.At(unsupported).IsNull()) {
      throw mgp::ValueException(fmt::format("Config '{}' is not supported by the export module", unsupported));
    }
  }

  Options options;
  if (!file_arg.IsNull()) {
    if (!file_arg.IsString()) throw mgp::ValueException("Argument 'file' must be a string or null");
    options.file = std::string{file_arg.ValueString()};
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
  if (!out) throw mgp::ValueException(fmt::format("Failed writing to '{}'", path));
}

// Reads a nodes/relationships argument, tolerating null (the reference coerces it to an empty list).
mgp::List OptionalList(const mgp::Value &value, const char *name) {
  if (value.IsNull()) return mgp::List{};
  if (!value.IsList()) throw mgp::ValueException(fmt::format("Argument '{}' must be a list or null", name));
  return value.ValueList();
}

void AddNodes(JsonWriter &writer, const mgp::List &nodes) {
  for (const auto value : nodes) {
    if (!value.IsNode()) throw mgp::ValueException("Argument 'nodes' must contain only nodes");
    writer.AddNode(value.ValueNode());
  }
}

void AddRelationships(JsonWriter &writer, const mgp::List &relationships) {
  for (const auto value : relationships) {
    if (!value.IsRelationship()) throw mgp::ValueException("Argument 'rels' must contain only relationships");
    writer.AddRelationship(value.ValueRelationship());
  }
}

// mgp::Record::Insert has no Type::Null case, and `file`/`data` are both nullable columns, so those two go in through
// the low-level insert.
void InsertNullable(mgp_result_record *record, const char *field, const std::optional<std::string> &value) {
  const auto wrapped = value ? mgp::Value(*value) : mgp::Value();
  mgp::result_record_insert(record, field, wrapped.ptr());
}

// Renders the payload to its sink and emits the 12-column row. `data` carries the payload only when there is no file
// and streaming was asked for; a file argument wins over `stream` (measured).
void EmitResult(mgp_result *result, const JsonWriter &writer, const Options &options, std::string_view source_prefix,
                std::chrono::steady_clock::time_point started_at) {
  const auto payload = writer.Dump();
  if (options.file) WriteFile(*options.file, payload);

  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at);

  auto *raw_record = mgp::result_new_record(result);
  mgp::Record record{raw_record};
  InsertNullable(raw_record, kReturnFile, options.file);
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
  const bool streaming = !options.file && options.stream;
  InsertNullable(raw_record, kReturnData, streaming ? std::optional{payload} : std::nullopt);
}

}  // namespace

void JsonData(mgp_list *args, mgp_graph * /*memgraph_graph*/, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto started_at = std::chrono::steady_clock::now();
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto options = ParseOptions(arguments[2], arguments[3].ValueMap());
    JsonWriter writer(options.write);
    AddNodes(writer, OptionalList(arguments[0], kArgumentNodes));
    AddRelationships(writer, OptionalList(arguments[1], kArgumentRelationships));
    EmitResult(result, writer, options, kSourcePrefixData, started_at);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void JsonAll(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto started_at = std::chrono::steady_clock::now();
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto options = ParseOptions(arguments[0], arguments[1].ValueMap());
    const mgp::Graph graph{memgraph_graph};
    JsonWriter writer(options.write);
    for (const auto node : graph.Nodes()) {
      writer.AddNode(node);
    }
    for (const auto relationship : graph.Relationships()) {
      writer.AddRelationship(relationship);
    }
    EmitResult(result, writer, options, kSourcePrefixDatabase, started_at);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

void JsonGraph(mgp_list *args, mgp_graph * /*memgraph_graph*/, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto started_at = std::chrono::steady_clock::now();
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto options = ParseOptions(arguments[1], arguments[2].ValueMap());
    const auto graph_map = arguments[0].ValueMap();
    JsonWriter writer(options.write);
    AddNodes(writer, OptionalList(graph_map.At(kGraphKeyNodes), kArgumentNodes));
    AddRelationships(writer, OptionalList(graph_map.At(kGraphKeyRelationships), kArgumentRelationships));
    EmitResult(result, writer, options, kSourcePrefixGraph, started_at);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
  }
}

}  // namespace Export
