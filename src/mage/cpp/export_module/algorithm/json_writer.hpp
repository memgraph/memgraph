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
#include <string>
#include <string_view>
#include <unordered_map>

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

// {"type":"node","id":"<id>","labels":[...sorted...],"properties":{...}}. `properties` is omitted when the node has
// none, and null suppresses it entirely; when non-null it is consumed.
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

// Accumulates serialized elements and renders them in the configured shape. Nodes and relationships are kept apart
// because JSON and JSON_ID_AS_KEYS group them under separate "nodes"/"rels" keys.
class JsonWriter {
 public:
  explicit JsonWriter(WriteConfig config) : config_(config) {}

  void AddNode(const mgp::Node &node);
  void AddRelationship(const mgp::Relationship &relationship);

  // Serialized payload in the configured format; "" when nothing was added under JSON_LINES, an empty wrapper under
  // the two object shapes. Consumes the accumulated elements, so call it once; the counters stay valid.
  std::string Dump();

  std::uint64_t NodeCount() const { return node_count_; }

  std::uint64_t RelationshipCount() const { return relationship_count_; }

  std::uint64_t PropertyCount() const { return property_count_; }

 private:
  WriteConfig config_;
  Json nodes_ = Json::array();
  Json relationships_ = Json::array();
  std::uint64_t node_count_{0};
  std::uint64_t relationship_count_{0};
  std::uint64_t property_count_{0};
};

}  // namespace Export
