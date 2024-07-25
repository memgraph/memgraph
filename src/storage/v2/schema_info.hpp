// Copyright 2024 Memgraph Ltd.
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
#include <set>
#include <string>
#include <unordered_map>

#include <boost/functional/hash.hpp>

namespace memgraph::storage {

enum struct SchemaPropertyType : uint8_t {
  Null,
  String,
  Boolean,
  Integer,
  Float,
  List,
  Map,
  Duration,
  Date,
  LocalTime,
  LocalDateTime,
  ZonedDateTime
};

struct PropertyInfo {
  std::unordered_map<SchemaPropertyType, int64_t> property_types;
  int64_t number_of_property_occurrences = 0;

  PropertyInfo() = default;

  void MergeStats(PropertyInfo &&info);
};

struct LabelsInfo {
  std::unordered_map<std::string, PropertyInfo> properties;  // key is a property name
  int64_t number_of_label_occurrences = 0;

  void MergeStats(LabelsInfo &&info);
};

struct LabelsHash {
  std::size_t operator()(const std::set<std::string> &s) const { return boost::hash_range(s.begin(), s.end()); }
};

struct LabelsComparator {
  bool operator()(const std::set<std::string> &lhs, const std::set<std::string> &rhs) const { return lhs == rhs; }
};

struct NodesInfo {
  using LabelsSet = std::set<std::string>;
  std::unordered_map<LabelsSet, LabelsInfo, LabelsHash, LabelsComparator> node_types_properties;

  void MergeStats(NodesInfo &&info);

  void AddVertex();
};

struct SchemaInfo {
  NodesInfo nodes;

  void MergeStats(SchemaInfo &&info);

  void AddVertex();
};

}  // namespace memgraph::storage
