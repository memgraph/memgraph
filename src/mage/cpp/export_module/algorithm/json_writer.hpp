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

#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include <mgp.hpp>
#include <nlohmann/json.hpp>

namespace Export {

// ordered_json, never nlohmann::json: the default is std::map-backed and sorts keys, which would emit
// {"id","labels","properties","type"} where the reference emits {"type","id","labels","properties"}.
using Json = nlohmann::ordered_json;

enum class JsonFormat : std::uint8_t { kJsonLines, kJson, kJsonIdAsKeys };

struct WriteConfig {
  JsonFormat format{JsonFormat::kJsonLines};
  bool write_node_properties{true};
  bool write_relationship_properties{true};
};

// What mgp::Node::Properties() / mgp::Relationship::Properties() hand back. Passed around by pointer so callers can
// fetch once and share the result between the counter and the serializer; the fetch deep-copies every value.
using Properties = std::unordered_map<std::string, mgp::Value>;

// Scalar/container property value -> JSON. Temporals become ISO-8601 strings, points become {crs, coords}.
Json ValueToJson(const mgp::Value &value);

// {"type":"node","id":"<id>","labels":[...sorted...],"properties":{...}}. `labels` and `properties` are each omitted
// when empty; a null `properties` suppresses the key entirely, and a non-null one is consumed.
Json NodeToJson(const mgp::Node &node, Properties *properties);

// {"type":"relationship","id":"<id>","label":"T","properties":{...},"start":{...},"end":{...}}. Endpoints are always
// inlined in full, whether or not they appear in the exported node set, and carry {id, labels, properties} only.
// `properties` is the relationship's own; null suppresses it, non-null is consumed.
Json RelationshipToJson(const mgp::Relationship &relationship, Properties *properties, const WriteConfig &config);

std::string_view SridToCrs(std::uint16_t srid);

std::string DateToString(const mgp::Date &date);
std::string LocalTimeToString(const mgp::LocalTime &local_time);
std::string LocalDateTimeToString(const mgp::LocalDateTime &local_date_time);
std::string ZonedDateTimeToString(const mgp::ZonedDateTime &zoned_date_time);
std::string DurationToString(const mgp::Duration &duration);

// Serializes elements straight to their sink as they arrive, so peak memory is one element rather than the whole
// export — the accumulated document this replaced measured about 5x the rendered payload.
//
// The two object shapes wrap their elements in "nodes"/"rels" groups, which streaming can only produce because every
// caller adds all of its nodes before any of its relationships. AddRelationship closes the node group, so adding a
// node afterwards is rejected rather than silently producing malformed JSON.
class JsonWriter {
 public:
  // With `file` the payload is streamed to that path, via a temporary renamed into place on success, so a failure
  // part-way through leaves any previous export intact. Otherwise `retain` keeps the payload in memory for the `data`
  // column. With neither, nothing is serialized at all and only the counters are produced.
  JsonWriter(WriteConfig config, std::optional<std::string> file, bool retain);
  JsonWriter(const JsonWriter &) = delete;
  JsonWriter &operator=(const JsonWriter &) = delete;
  JsonWriter(JsonWriter &&) = delete;
  JsonWriter &operator=(JsonWriter &&) = delete;
  ~JsonWriter();

  void AddNode(const mgp::Node &node);
  void AddRelationship(const mgp::Relationship &relationship);

  // Closes the open groups, commits the file if there is one, and returns the retained payload: "" when nothing was
  // retained or when nothing was added under JSON_LINES, an empty wrapper under the two object shapes. Rvalue-
  // qualified because it finishes the sink; the counters stay valid afterwards.
  std::string Finish() &&;

  std::uint64_t NodeCount() const { return node_count_; }

  std::uint64_t RelationshipCount() const { return relationship_count_; }

  std::uint64_t PropertyCount() const { return property_count_; }

 private:
  enum class Group : std::uint8_t { kNone, kNodes, kRelationships };

  // True when the payload has anywhere to go; when it does not, building the elements would be pure waste.
  bool Serializing() const { return out_.is_open() || retain_; }

  void Emit(std::string_view bytes);
  void EnterGroup(Group group);
  void EmitElement(const Json &element);

  WriteConfig config_;
  std::optional<std::string> file_;
  std::string temp_path_;
  std::ofstream out_;
  bool retain_;
  std::string payload_;
  Group group_{Group::kNone};
  bool wrote_any_{false};
  bool group_has_elements_{false};
  // JSON_ID_AS_KEYS only: ids are object keys there, so a duplicated element can only be one entry. Holds ids, not
  // documents, and is cleared between groups because nodes and relationships have separate id spaces.
  std::unordered_set<std::string> emitted_ids_;
  std::uint64_t node_count_{0};
  std::uint64_t relationship_count_{0};
  std::uint64_t property_count_{0};
};

}  // namespace Export
