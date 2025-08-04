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

#include <set>
#include <utility>

#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/indices/property_path.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"

namespace memgraph::storage {

// NOTE: MetadataDeltas are not supported in a multi command transaction (single query only)
struct MetadataDelta {
  enum class Action {
    LABEL_INDEX_CREATE,
    LABEL_INDEX_DROP,
    LABEL_INDEX_STATS_SET,
    LABEL_INDEX_STATS_CLEAR,
    LABEL_PROPERTIES_INDEX_CREATE,
    LABEL_PROPERTIES_INDEX_DROP,
    LABEL_PROPERTIES_INDEX_STATS_SET,
    LABEL_PROPERTIES_INDEX_STATS_CLEAR,
    EDGE_INDEX_CREATE,
    EDGE_INDEX_DROP,
    EDGE_PROPERTY_INDEX_CREATE,
    EDGE_PROPERTY_INDEX_DROP,
    GLOBAL_EDGE_PROPERTY_INDEX_CREATE,
    GLOBAL_EDGE_PROPERTY_INDEX_DROP,
    TEXT_INDEX_CREATE,
    TEXT_INDEX_DROP,
    EXISTENCE_CONSTRAINT_CREATE,
    EXISTENCE_CONSTRAINT_DROP,
    UNIQUE_CONSTRAINT_CREATE,
    UNIQUE_CONSTRAINT_DROP,
    TYPE_CONSTRAINT_CREATE,
    TYPE_CONSTRAINT_DROP,
    ENUM_CREATE,
    ENUM_ALTER_ADD,
    ENUM_ALTER_UPDATE,
    POINT_INDEX_CREATE,
    POINT_INDEX_DROP,
    VECTOR_INDEX_CREATE,
    VECTOR_INDEX_DROP,
    VECTOR_EDGE_INDEX_CREATE,
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
  static constexpr struct PointIndexCreate {
  } point_index_create;
  static constexpr struct PointIndexDrop {
  } point_index_drop;
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
  static constexpr struct GlobalEdgePropertyIndexCreate {
  } global_edge_property_index_create;
  static constexpr struct GlobalEdgePropertyIndexDrop {
  } global_edge_property_index_drop;
  static constexpr struct TextIndexCreate {
  } text_index_create;
  static constexpr struct TextIndexDrop {
  } text_index_drop;
  static constexpr struct VectorIndexCreate {
  } vector_index_create;
  static constexpr struct VectorIndexDrop {
  } vector_index_drop;
  static constexpr struct VectorEdgeIndexCreate {
  } vector_edge_index_create;
  static constexpr struct ExistenceConstraintCreate {
  } existence_constraint_create;
  static constexpr struct ExistenceConstraintDrop {
  } existence_constraint_drop;
  static constexpr struct UniqueConstraintCreate {
  } unique_constraint_create;
  static constexpr struct UniqueConstraintDrop {
  } unique_constraint_drop;
  static constexpr struct TypeConstraintCreate {
  } type_constraint_create;
  static constexpr struct TypeConstraintDrop {
  } type_constraint_drop;
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

  MetadataDelta(LabelPropertyIndexCreate /*tag*/, LabelId label, std::vector<storage::PropertyPath> properties)
      : action(Action::LABEL_PROPERTIES_INDEX_CREATE), label_ordered_properties{label, std::move(properties)} {}

  MetadataDelta(LabelPropertyIndexDrop /*tag*/, LabelId label, std::vector<storage::PropertyPath> properties)
      : action(Action::LABEL_PROPERTIES_INDEX_DROP), label_ordered_properties{label, std::move(properties)} {}

  MetadataDelta(LabelPropertyIndexStatsSet /*tag*/, LabelId label, std::vector<PropertyPath> properties,
                LabelPropertyIndexStats const &stats)
      : action(Action::LABEL_PROPERTIES_INDEX_STATS_SET), label_property_stats{label, std::move(properties), stats} {}

  MetadataDelta(LabelPropertyIndexStatsClear /*tag*/, LabelId label)
      : action(Action::LABEL_PROPERTIES_INDEX_STATS_CLEAR), label{label} {}

  MetadataDelta(EdgeIndexCreate /*tag*/, EdgeTypeId edge_type)
      : action(Action::EDGE_INDEX_CREATE), edge_type(edge_type) {}

  MetadataDelta(EdgeIndexDrop /*tag*/, EdgeTypeId edge_type) : action(Action::EDGE_INDEX_DROP), edge_type(edge_type) {}

  MetadataDelta(EdgePropertyIndexCreate /*tag*/, EdgeTypeId edge_type, PropertyId property)
      : action(Action::EDGE_PROPERTY_INDEX_CREATE), edge_type_property{edge_type, property} {}

  MetadataDelta(EdgePropertyIndexDrop /*tag*/, EdgeTypeId edge_type, PropertyId property)
      : action(Action::EDGE_PROPERTY_INDEX_DROP), edge_type_property{edge_type, property} {}

  MetadataDelta(GlobalEdgePropertyIndexCreate /*tag*/, PropertyId property)
      : action(Action::GLOBAL_EDGE_PROPERTY_INDEX_CREATE), edge_property{property} {}

  MetadataDelta(GlobalEdgePropertyIndexDrop /*tag*/, PropertyId property)
      : action(Action::GLOBAL_EDGE_PROPERTY_INDEX_DROP), edge_property{property} {}

  MetadataDelta(TextIndexCreate /*tag*/, TextIndexInfo text_index_info)
      : action(Action::TEXT_INDEX_CREATE), text_index(std::move(text_index_info)) {}

  MetadataDelta(TextIndexDrop /*tag*/, std::string index_name)
      : action(Action::TEXT_INDEX_DROP), index_name{std::move(index_name)} {}

  MetadataDelta(PointIndexCreate /*tag*/, LabelId label, PropertyId property)
      : action(Action::POINT_INDEX_CREATE), label_property{label, property} {}

  MetadataDelta(PointIndexDrop /*tag*/, LabelId label, PropertyId property)
      : action(Action::POINT_INDEX_DROP), label_property{label, property} {}

  MetadataDelta(VectorIndexCreate /*tag*/, VectorIndexSpec spec)
      : action(Action::VECTOR_INDEX_CREATE), vector_index_spec(std::move(spec)) {}

  MetadataDelta(VectorIndexDrop /*tag*/, std::string_view index_name)
      : action(Action::VECTOR_INDEX_DROP), index_name{index_name} {}

  MetadataDelta(VectorEdgeIndexCreate /*tag*/, VectorEdgeIndexSpec spec)
      : action(Action::VECTOR_EDGE_INDEX_CREATE), vector_edge_index_spec(std::move(spec)) {}

  MetadataDelta(ExistenceConstraintCreate /*tag*/, LabelId label, PropertyId property)
      : action(Action::EXISTENCE_CONSTRAINT_CREATE), label_property{label, property} {}

  MetadataDelta(ExistenceConstraintDrop /*tag*/, LabelId label, PropertyId property)
      : action(Action::EXISTENCE_CONSTRAINT_DROP), label_property{label, property} {}

  MetadataDelta(UniqueConstraintCreate /*tag*/, LabelId label, std::set<PropertyId> properties)
      : action(Action::UNIQUE_CONSTRAINT_CREATE), label_unordered_properties{label, std::move(properties)} {}

  MetadataDelta(UniqueConstraintDrop /*tag*/, LabelId label, std::set<PropertyId> properties)
      : action(Action::UNIQUE_CONSTRAINT_DROP), label_unordered_properties{label, std::move(properties)} {}

  MetadataDelta(TypeConstraintCreate /*tag*/, LabelId label, PropertyId property, TypeConstraintKind type)
      : action(Action::TYPE_CONSTRAINT_CREATE), label_property_type{label, property, type} {}

  MetadataDelta(TypeConstraintDrop /*tag*/, LabelId label, PropertyId property, TypeConstraintKind type)
      : action(Action::TYPE_CONSTRAINT_DROP), label_property_type{label, property, type} {}

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
      case LABEL_PROPERTIES_INDEX_STATS_CLEAR:
      case LABEL_INDEX_STATS_CLEAR: {
        std::destroy_at(&label);
        break;
      }
      case LABEL_INDEX_STATS_SET: {
        std::destroy_at(&label_stats);
        break;
      }
      case Action::EDGE_INDEX_CREATE:
      case Action::EDGE_INDEX_DROP: {
        std::destroy_at(&edge_type);
        break;
      }
      case Action::EDGE_PROPERTY_INDEX_CREATE:
      case Action::EDGE_PROPERTY_INDEX_DROP: {
        std::destroy_at(&edge_type_property);
        break;
      }
      case Action::GLOBAL_EDGE_PROPERTY_INDEX_CREATE:
      case Action::GLOBAL_EDGE_PROPERTY_INDEX_DROP: {
        std::destroy_at(&edge_property);
        break;
      }
      case EXISTENCE_CONSTRAINT_CREATE:
      case EXISTENCE_CONSTRAINT_DROP:
      case POINT_INDEX_CREATE:
      case POINT_INDEX_DROP: {
        std::destroy_at(&label_property);
        break;
      }
      case TYPE_CONSTRAINT_CREATE:
      case TYPE_CONSTRAINT_DROP: {
        std::destroy_at(&label_property_type);
        break;
      }
      case ENUM_CREATE: {
        std::destroy_at(&enum_create_info);
        break;
      }
      case ENUM_ALTER_ADD: {
        std::destroy_at(&enum_alter_add_info);
        break;
      }
      case ENUM_ALTER_UPDATE: {
        std::destroy_at(&enum_alter_update_info);
        break;
      }

      case LABEL_PROPERTIES_INDEX_CREATE:
      case LABEL_PROPERTIES_INDEX_DROP: {
        std::destroy_at(&label_ordered_properties);
        break;
      }
      case LABEL_PROPERTIES_INDEX_STATS_SET: {
        std::destroy_at(&label_property_stats);
        break;
      }
      case VECTOR_INDEX_CREATE: {
        std::destroy_at(&vector_index_spec);
        break;
      }
      case VECTOR_EDGE_INDEX_CREATE: {
        std::destroy_at(&vector_edge_index_spec);
        break;
      }
      case VECTOR_INDEX_DROP: {
        std::destroy_at(&index_name);
        break;
      }
      case UNIQUE_CONSTRAINT_CREATE:
      case UNIQUE_CONSTRAINT_DROP: {
        std::destroy_at(&label_unordered_properties);
        break;
      }
      case TEXT_INDEX_CREATE: {
        std::destroy_at(&text_index);
        break;
      }
      case TEXT_INDEX_DROP: {
        std::destroy_at(&index_name);
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
      std::vector<PropertyPath> properties;
    } label_ordered_properties;

    struct {
      LabelId label;
      std::set<PropertyId> properties;
    } label_unordered_properties;

    struct {
      LabelId label;
      LabelIndexStats stats;
    } label_stats;

    struct {
      LabelId label;
      std::vector<PropertyPath> properties;
      LabelPropertyIndexStats stats;
    } label_property_stats;

    struct {
      LabelId label;
      PropertyId property;
      TypeConstraintKind type;
    } label_property_type;

    struct {
      EdgeTypeId edge_type;
      PropertyId property;
    } edge_type_property;

    struct {
      PropertyId property;
    } edge_property;

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

    TextIndexInfo text_index;
    VectorIndexSpec vector_index_spec;
    VectorEdgeIndexSpec vector_edge_index_spec;
    std::string index_name;
  };
};

static_assert(alignof(MetadataDelta) >= 8, "The Delta should be aligned to at least 8!");

}  // namespace memgraph::storage
