// Copyright 2025 Memgraph Ltd.
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

#include <mgcxx_text_search.hpp>
#include <span>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "mg_procedure.h"
#include "nlohmann/json_fwd.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::storage {

class NameIdMapper;
struct Vertex;
struct Edge;
struct TextIndexData;
struct TextEdgeIndexData;

inline constexpr std::string_view kTextIndicesDirectory = "text_indices";
inline constexpr bool kDoSkipCommit = true;

// Boolean operators that should be preserved in uppercase for Tantivy
inline constexpr std::string_view kBooleanAnd = "AND";
inline constexpr std::string_view kBooleanOr = "OR";
inline constexpr std::string_view kBooleanNot = "NOT";

// Convert text to lowercase while preserving boolean operators
std::string ToLowerCasePreservingBooleanOperators(std::string_view input);

// Make index path from base directory and index name
std::string MakeIndexPath(const std::string &base_path, std::string_view index_name);

// Serialize properties to JSON format
nlohmann::json SerializeProperties(const std::map<PropertyId, PropertyValue> &properties, NameIdMapper *name_id_mapper);

// Convert properties to string representation
std::string StringifyProperties(const std::map<PropertyId, PropertyValue> &properties);

// Extract properties from the property store and return them as a map
std::map<PropertyId, PropertyValue> ExtractProperties(const PropertyStore &property_store,
                                                      std::span<PropertyId const> properties);

// Check if index properties match the given properties
bool IndexPropertiesMatch(std::span<const PropertyId> index_properties,
                          std::span<const PropertyId> properties_to_check);

// Text index change tracking
enum class TextIndexOp { ADD, UPDATE, REMOVE };
struct TextIndexSpec {
  bool operator==(const TextIndexSpec &other) const = default;

  std::string index_name;
  LabelId label;
  std::vector<PropertyId> properties;
};

struct TextEdgeIndexSpec {
  bool operator==(const TextEdgeIndexSpec &other) const = default;

  std::string index_name;
  EdgeTypeId edge_type;
  std::vector<PropertyId> properties;
};

struct TextIndexPending {
  absl::flat_hash_set<const Vertex *> to_add;
  absl::flat_hash_set<const Vertex *> to_remove;
};
struct EdgeWithVertices {
  const Edge *edge;
  const Vertex *from_vertex;
  const Vertex *to_vertex;

  EdgeWithVertices(const Edge *e, const Vertex *from, const Vertex *to) : edge(e), from_vertex(from), to_vertex(to) {}

  friend bool operator==(const EdgeWithVertices &a, const EdgeWithVertices &b) { return a.edge == b.edge; }

  template <typename H>
  friend H AbslHashValue(H h, const EdgeWithVertices &edge_with_vertices) {
    return H::combine(std::move(h), edge_with_vertices.edge);
  }
};
struct TextEdgeIndexPending {
  absl::flat_hash_set<EdgeWithVertices> to_add;
  absl::flat_hash_set<const Edge *> to_remove;
};

// Text index change collector for transaction-level batching
using TextIndexChangeCollector = absl::flat_hash_map<TextIndexData *, TextIndexPending>;
using TextEdgeIndexChangeCollector = absl::flat_hash_map<TextEdgeIndexData *, TextEdgeIndexPending>;

void TrackTextIndexChange(TextIndexChangeCollector &collector, std::span<TextIndexData *> indices, const Vertex *vertex,
                          TextIndexOp op);
void TrackTextEdgeIndexChange(TextEdgeIndexChangeCollector &collector, std::span<TextEdgeIndexData *> indices,
                              const Edge *edge, const Vertex *from_vertex, const Vertex *to_vertex, TextIndexOp op);

// Text search utility functions
mgcxx::text_search::SearchOutput PerformTextSearch(mgcxx::text_search::Context &context,
                                                   const std::string &search_query, text_search_mode search_mode,
                                                   std::size_t limit);

}  // namespace memgraph::storage
