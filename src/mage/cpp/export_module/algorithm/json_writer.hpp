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

// Serializes elements to their sink as they arrive, so peak memory is one element. The object shapes group nodes
// then relationships, so a node added after a relationship is rejected rather than producing malformed JSON.
class JsonWriter {
 public:
  // With `file`, a new or server-owned single-link file without an ACL goes via a temporary renamed into place, so a
  // failure keeps the previous export; others, or when no temporary can be made, are written in place.
  JsonWriter(WriteConfig config, std::optional<std::string> file, bool retain);
  JsonWriter(const JsonWriter &) = delete;
  JsonWriter &operator=(const JsonWriter &) = delete;
  JsonWriter(JsonWriter &&) = delete;
  JsonWriter &operator=(JsonWriter &&) = delete;
  ~JsonWriter();

  void AddNode(const mgp::Node &node);
  // Counts a node without writing it, for a repeat that JSON_ID_AS_KEYS can only hold as one entry.
  void CountNode(const mgp::Node &node);
  void AddRelationship(const mgp::Relationship &relationship);

  // Closes the open groups, commits the file, and returns the retained payload ("" under JSON_LINES when empty, an
  // empty wrapper otherwise). The counters stay valid afterwards.
  std::string Finish() &&;

  std::uint64_t NodeCount() const { return node_count_; }

  std::uint64_t RelationshipCount() const { return relationship_count_; }

  std::uint64_t PropertyCount() const { return property_count_; }

 private:
  enum class Group : std::uint8_t { kNone, kNodes, kRelationships };

  // Names the file an error is about. The temporary is an implementation detail, but hiding it entirely sends an
  // operator to check permissions on a path that is not the one that failed.
  std::string SinkDescription() const;

  void Emit(std::string_view bytes);
  void EnterGroup(Group group);
  void EmitElement(const Json &element);

  WriteConfig config_;
  std::optional<std::string> file_;
  // Unique per writer, and empty when writing in place. A deterministic name would let two exports to the same
  // target share one file, each truncating, publishing and then deleting the other's work.
  std::string temp_path_;
  std::ofstream out_;
  bool retain_;
  std::string payload_;
  Group group_{Group::kNone};
  bool wrote_any_{false};
  bool group_has_elements_{false};
  std::uint64_t node_count_{0};
  std::uint64_t relationship_count_{0};
  std::uint64_t property_count_{0};
};

}  // namespace Export
