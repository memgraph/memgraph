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

#include <atomic>
#include <cstdint>
#include <set>

#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

// NOTE: MetadataDeltas are not supported in a multi command transaction (single query only)
struct MetadataDelta {
  enum class Action {
    LABEL_INDEX_CREATE,
    LABEL_INDEX_DROP,
    LABEL_INDEX_STATS_SET,
    LABEL_INDEX_STATS_CLEAR,
    LABEL_PROPERTY_INDEX_CREATE,
    LABEL_PROPERTY_INDEX_DROP,
    LABEL_PROPERTY_INDEX_STATS_SET,
    LABEL_PROPERTY_INDEX_STATS_CLEAR,
    EDGE_INDEX_CREATE,
    EDGE_INDEX_DROP,
    EXISTENCE_CONSTRAINT_CREATE,
    EXISTENCE_CONSTRAINT_DROP,
    UNIQUE_CONSTRAINT_CREATE,
    UNIQUE_CONSTRAINT_DROP,
  };

  static constexpr struct LabelIndexCreate {
  } label_index_create;
  static constexpr struct LabelIndexDrop {
  } label_index_drop;
  static constexpr struct LabelIndexStatsSet {
  } label_index_stats_set;
  static constexpr struct LabelIndexStatsClear {
  } label_index_stats_clear;
  static constexpr struct LabelPropertyIndexCreate {
  } label_property_index_create;
  static constexpr struct LabelPropertyIndexDrop {
  } label_property_index_drop;
  static constexpr struct LabelPropertyIndexStatsSet {
  } label_property_index_stats_set;
  static constexpr struct LabelPropertyIndexStatsClear {
  } label_property_index_stats_clear;
  static constexpr struct EdgeIndexCreate {
  } edge_index_create;
  static constexpr struct EdgeIndexDrop {
  } edge_index_drop;
  static constexpr struct ExistenceConstraintCreate {
  } existence_constraint_create;
  static constexpr struct ExistenceConstraintDrop {
  } existence_constraint_drop;
  static constexpr struct UniqueConstraintCreate {
  } unique_constraint_create;
  static constexpr struct UniqueConstraintDrop {
  } unique_constraint_drop;

  MetadataDelta(LabelIndexCreate /*tag*/, LabelId label) : action(Action::LABEL_INDEX_CREATE), label(label) {}

  MetadataDelta(LabelIndexDrop /*tag*/, LabelId label) : action(Action::LABEL_INDEX_DROP), label(label) {}

  MetadataDelta(LabelIndexStatsSet /*tag*/, LabelId label, LabelIndexStats stats)
      : action(Action::LABEL_INDEX_STATS_SET), label_stats{label, stats} {}

  MetadataDelta(LabelIndexStatsClear /*tag*/, LabelId label) : action(Action::LABEL_INDEX_STATS_CLEAR), label{label} {}

  MetadataDelta(LabelPropertyIndexCreate /*tag*/, LabelId label, PropertyId property)
      : action(Action::LABEL_PROPERTY_INDEX_CREATE), label_property{label, property} {}

  MetadataDelta(LabelPropertyIndexDrop /*tag*/, LabelId label, PropertyId property)
      : action(Action::LABEL_PROPERTY_INDEX_DROP), label_property{label, property} {}

  MetadataDelta(LabelPropertyIndexStatsSet /*tag*/, LabelId label, PropertyId property, LabelPropertyIndexStats stats)
      : action(Action::LABEL_PROPERTY_INDEX_STATS_SET), label_property_stats{label, property, stats} {}

  MetadataDelta(LabelPropertyIndexStatsClear /*tag*/, LabelId label)
      : action(Action::LABEL_PROPERTY_INDEX_STATS_CLEAR), label{label} {}

  MetadataDelta(EdgeIndexCreate /*tag*/, EdgeTypeId edge_type)
      : action(Action::EDGE_INDEX_CREATE), edge_type(edge_type) {}

  MetadataDelta(EdgeIndexDrop /*tag*/, EdgeTypeId edge_type) : action(Action::EDGE_INDEX_DROP), edge_type(edge_type) {}

  MetadataDelta(ExistenceConstraintCreate /*tag*/, LabelId label, PropertyId property)
      : action(Action::EXISTENCE_CONSTRAINT_CREATE), label_property{label, property} {}

  MetadataDelta(ExistenceConstraintDrop /*tag*/, LabelId label, PropertyId property)
      : action(Action::EXISTENCE_CONSTRAINT_DROP), label_property{label, property} {}

  MetadataDelta(UniqueConstraintCreate /*tag*/, LabelId label, std::set<PropertyId> properties)
      : action(Action::UNIQUE_CONSTRAINT_CREATE), label_properties{label, std::move(properties)} {}

  MetadataDelta(UniqueConstraintDrop /*tag*/, LabelId label, std::set<PropertyId> properties)
      : action(Action::UNIQUE_CONSTRAINT_DROP), label_properties{label, std::move(properties)} {}

  MetadataDelta(const MetadataDelta &) = delete;
  MetadataDelta(MetadataDelta &&) = delete;
  MetadataDelta &operator=(const MetadataDelta &) = delete;
  MetadataDelta &operator=(MetadataDelta &&) = delete;

  ~MetadataDelta() {
    switch (action) {
      case Action::LABEL_INDEX_CREATE:
      case Action::LABEL_INDEX_DROP:
      case Action::LABEL_INDEX_STATS_SET:
      case Action::LABEL_INDEX_STATS_CLEAR:
      case Action::LABEL_PROPERTY_INDEX_CREATE:
      case Action::LABEL_PROPERTY_INDEX_DROP:
      case Action::LABEL_PROPERTY_INDEX_STATS_SET:
      case Action::LABEL_PROPERTY_INDEX_STATS_CLEAR:
      case Action::EDGE_INDEX_CREATE:
      case Action::EDGE_INDEX_DROP:
      case Action::EXISTENCE_CONSTRAINT_CREATE:
      case Action::EXISTENCE_CONSTRAINT_DROP:
        break;
      case Action::UNIQUE_CONSTRAINT_CREATE:
      case Action::UNIQUE_CONSTRAINT_DROP:
        label_properties.properties.~set<PropertyId>();
        break;
    }
  }

  Action action;

  union {
    LabelId label;

    EdgeTypeId edge_type;

    struct {
      LabelId label;
      PropertyId property;
    } label_property;

    struct {
      LabelId label;
      std::set<PropertyId> properties;
    } label_properties;

    struct {
      LabelId label;
      LabelIndexStats stats;
    } label_stats;

    struct {
      LabelId label;
      PropertyId property;
      LabelPropertyIndexStats stats;
    } label_property_stats;
  };
};

static_assert(alignof(MetadataDelta) >= 8, "The Delta should be aligned to at least 8!");

}  // namespace memgraph::storage
