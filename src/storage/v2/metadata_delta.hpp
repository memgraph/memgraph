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

#include <set>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"

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
    EDGE_PROPERTY_INDEX_CREATE,
    EDGE_PROPERTY_INDEX_DROP,
    TEXT_INDEX_CREATE,
    TEXT_INDEX_DROP,
    EXISTENCE_CONSTRAINT_CREATE,
    EXISTENCE_CONSTRAINT_DROP,
    UNIQUE_CONSTRAINT_CREATE,
    UNIQUE_CONSTRAINT_DROP,
    ENUM_CREATE,
    ENUM_ALTER_ADD,
    ENUM_ALTER_UPDATE,
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
  static constexpr struct EdgePropertyIndexCreate {
  } edge_property_index_create;
  static constexpr struct EdgePropertyIndexDrop {
  } edge_property_index_drop;
  static constexpr struct TextIndexCreate {
  } text_index_create;
  static constexpr struct TextIndexDrop {
  } text_index_drop;
  static constexpr struct ExistenceConstraintCreate {
  } existence_constraint_create;
  static constexpr struct ExistenceConstraintDrop {
  } existence_constraint_drop;
  static constexpr struct UniqueConstraintCreate {
  } unique_constraint_create;
  static constexpr struct UniqueConstraintDrop {
  } unique_constraint_drop;
  static constexpr struct EnumCreate {
  } enum_create;
  static constexpr struct EnumAlterAdd {
  } enum_alter_add;
  static constexpr struct EnumAlterUpdate {
  } enum_alter_update;

  MetadataDelta(LabelIndexCreate /*tag*/, LabelId label) : action(Action::LABEL_INDEX_CREATE), label(label) {}

  MetadataDelta(LabelIndexDrop /*tag*/, LabelId label) : action(Action::LABEL_INDEX_DROP), label(label) {}

  MetadataDelta(LabelIndexStatsSet /*tag*/, LabelId label, LabelIndexStats stats)
      : action(Action::LABEL_INDEX_STATS_SET), label_stats{label, stats} {}

  MetadataDelta(LabelIndexStatsClear /*tag*/, LabelId label) : action(Action::LABEL_INDEX_STATS_CLEAR), label{label} {}

  MetadataDelta(LabelPropertyIndexCreate /*tag*/, LabelId label, PropertyId property)
      : action(Action::LABEL_PROPERTY_INDEX_CREATE), label_property{label, property} {}

  MetadataDelta(LabelPropertyIndexDrop /*tag*/, LabelId label, PropertyId property)
      : action(Action::LABEL_PROPERTY_INDEX_DROP), label_property{label, property} {}

  MetadataDelta(LabelPropertyIndexStatsSet /*tag*/, LabelId label, PropertyId property,
                LabelPropertyIndexStats const &stats)
      : action(Action::LABEL_PROPERTY_INDEX_STATS_SET), label_property_stats{label, property, stats} {}

  MetadataDelta(LabelPropertyIndexStatsClear /*tag*/, LabelId label)
      : action(Action::LABEL_PROPERTY_INDEX_STATS_CLEAR), label{label} {}

  MetadataDelta(EdgeIndexCreate /*tag*/, EdgeTypeId edge_type)
      : action(Action::EDGE_INDEX_CREATE), edge_type(edge_type) {}

  MetadataDelta(EdgeIndexDrop /*tag*/, EdgeTypeId edge_type) : action(Action::EDGE_INDEX_DROP), edge_type(edge_type) {}

  MetadataDelta(EdgePropertyIndexCreate /*tag*/, EdgeTypeId edge_type, PropertyId property)
      : action(Action::EDGE_PROPERTY_INDEX_CREATE), edge_type_property{edge_type, property} {}

  MetadataDelta(EdgePropertyIndexDrop /*tag*/, EdgeTypeId edge_type, PropertyId property)
      : action(Action::EDGE_PROPERTY_INDEX_DROP), edge_type_property{edge_type, property} {}

  MetadataDelta(TextIndexCreate /*tag*/, std::string index_name, LabelId label)
      : action(Action::TEXT_INDEX_CREATE), text_index{std::move(index_name), label} {}

  MetadataDelta(TextIndexDrop /*tag*/, std::string index_name, LabelId label)
      : action(Action::TEXT_INDEX_DROP), text_index{std::move(index_name), label} {}

  MetadataDelta(ExistenceConstraintCreate /*tag*/, LabelId label, PropertyId property)
      : action(Action::EXISTENCE_CONSTRAINT_CREATE), label_property{label, property} {}

  MetadataDelta(ExistenceConstraintDrop /*tag*/, LabelId label, PropertyId property)
      : action(Action::EXISTENCE_CONSTRAINT_DROP), label_property{label, property} {}

  MetadataDelta(UniqueConstraintCreate /*tag*/, LabelId label, std::set<PropertyId> properties)
      : action(Action::UNIQUE_CONSTRAINT_CREATE), label_properties{label, std::move(properties)} {}

  MetadataDelta(UniqueConstraintDrop /*tag*/, LabelId label, std::set<PropertyId> properties)
      : action(Action::UNIQUE_CONSTRAINT_DROP), label_properties{label, std::move(properties)} {}

  MetadataDelta(EnumCreate /*tag*/, EnumTypeId etype) : action(Action::ENUM_CREATE), enum_create_info{.etype = etype} {}

  MetadataDelta(EnumAlterAdd /*tag*/, Enum value)
      : action(Action::ENUM_ALTER_ADD), enum_alter_add_info{.value = value} {}

  MetadataDelta(EnumAlterUpdate /*tag*/, Enum value, std::string old_value)
      : action(Action::ENUM_ALTER_UPDATE), enum_alter_update_info{.value = value, .old_value = std::move(old_value)} {}

  MetadataDelta(const MetadataDelta &) = delete;
  MetadataDelta(MetadataDelta &&) = delete;
  MetadataDelta &operator=(const MetadataDelta &) = delete;
  MetadataDelta &operator=(MetadataDelta &&) = delete;

  ~MetadataDelta() {
    switch (action) {
      using enum memgraph::storage::MetadataDelta::Action;
      case LABEL_INDEX_CREATE:
      case LABEL_INDEX_DROP:
      case LABEL_INDEX_STATS_SET:
      case LABEL_INDEX_STATS_CLEAR:
      case LABEL_PROPERTY_INDEX_CREATE:
      case LABEL_PROPERTY_INDEX_DROP:
      case LABEL_PROPERTY_INDEX_STATS_SET:
      case LABEL_PROPERTY_INDEX_STATS_CLEAR:
      case Action::EDGE_INDEX_CREATE:
      case Action::EDGE_INDEX_DROP:
      case Action::EDGE_PROPERTY_INDEX_CREATE:
      case Action::EDGE_PROPERTY_INDEX_DROP:
      case EXISTENCE_CONSTRAINT_CREATE:
      case EXISTENCE_CONSTRAINT_DROP:
      case ENUM_CREATE:
      case ENUM_ALTER_ADD:
      case ENUM_ALTER_UPDATE:
        break;
      case UNIQUE_CONSTRAINT_CREATE:
      case UNIQUE_CONSTRAINT_DROP: {
        std::destroy_at(&label_properties);
        break;
      }
      case TEXT_INDEX_CREATE:
      case TEXT_INDEX_DROP: {
        std::destroy_at(&text_index);
        break;
      }
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

    struct {
      EdgeTypeId edge_type;
      PropertyId property;
    } edge_type_property;

    struct {
      std::string index_name;
      LabelId label;
    } text_index;

    struct {
      EnumTypeId etype;
    } enum_create_info;

    struct {
      Enum value;
    } enum_alter_add_info;

    struct {
      Enum value;
      std::string old_value;
    } enum_alter_update_info;
  };
};

static_assert(alignof(MetadataDelta) >= 8, "The Delta should be aligned to at least 8!");

}  // namespace memgraph::storage
