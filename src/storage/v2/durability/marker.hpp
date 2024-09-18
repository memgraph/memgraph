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
#include "storage/v2/enum_store.hpp"

namespace memgraph::storage::durability {

/// Markers that are used to indicate crucial parts of the snapshot/WAL.
/// IMPORTANT: Don't forget to update the list of all markers `kMarkersAll` when
/// you add a new Marker.
enum class Marker : uint8_t {
  TYPE_NULL = 0x10,
  TYPE_BOOL = 0x11,
  TYPE_INT = 0x12,
  TYPE_DOUBLE = 0x13,
  TYPE_STRING = 0x14,
  TYPE_LIST = 0x15,
  TYPE_MAP = 0x16,
  TYPE_PROPERTY_VALUE = 0x17,
  TYPE_TEMPORAL_DATA = 0x18,
  TYPE_ZONED_TEMPORAL_DATA = 0x19,
  TYPE_ENUM = 0x1a,
  TYPE_POINT_2D = 0x1b,
  TYPE_POINT_3D = 0x1c,

  SECTION_VERTEX = 0x20,
  SECTION_EDGE = 0x21,
  SECTION_MAPPER = 0x22,
  SECTION_METADATA = 0x23,
  SECTION_INDICES = 0x24,
  SECTION_CONSTRAINTS = 0x25,
  SECTION_DELTA = 0x26,
  SECTION_EPOCH_HISTORY = 0x27,
  SECTION_EDGE_INDICES = 0x28,
  SECTION_ENUMS = 0x29,

  SECTION_OFFSETS = 0x42,

  DELTA_VERTEX_CREATE = 0x50,
  DELTA_VERTEX_DELETE = 0x51,
  DELTA_VERTEX_ADD_LABEL = 0x52,
  DELTA_VERTEX_REMOVE_LABEL = 0x53,
  DELTA_VERTEX_SET_PROPERTY = 0x54,
  DELTA_EDGE_CREATE = 0x55,
  DELTA_EDGE_DELETE = 0x56,
  DELTA_EDGE_SET_PROPERTY = 0x57,
  DELTA_TRANSACTION_END = 0x58,
  DELTA_LABEL_INDEX_CREATE = 0x59,
  DELTA_LABEL_INDEX_DROP = 0x5a,
  DELTA_LABEL_PROPERTY_INDEX_CREATE = 0x5b,
  DELTA_LABEL_PROPERTY_INDEX_DROP = 0x5c,
  DELTA_EXISTENCE_CONSTRAINT_CREATE = 0x5d,
  DELTA_EXISTENCE_CONSTRAINT_DROP = 0x5e,
  DELTA_UNIQUE_CONSTRAINT_CREATE = 0x5f,
  DELTA_UNIQUE_CONSTRAINT_DROP = 0x60,
  DELTA_LABEL_INDEX_STATS_SET = 0x61,
  DELTA_LABEL_INDEX_STATS_CLEAR = 0x62,
  DELTA_LABEL_PROPERTY_INDEX_STATS_SET = 0x63,
  DELTA_LABEL_PROPERTY_INDEX_STATS_CLEAR = 0x64,
  DELTA_EDGE_INDEX_CREATE = 0x65,
  DELTA_EDGE_INDEX_DROP = 0x66,
  DELTA_TEXT_INDEX_CREATE = 0x67,
  DELTA_TEXT_INDEX_DROP = 0x68,
  DELTA_ENUM_CREATE = 0x69,
  DELTA_ENUM_ALTER_ADD = 0x6a,
  DELTA_ENUM_ALTER_UPDATE = 0x6b,
  DELTA_EDGE_PROPERTY_INDEX_CREATE = 0x6c,
  DELTA_EDGE_PROPERTY_INDEX_DROP = 0x6d,
  DELTA_POINT_INDEX_CREATE = 0x6e,
  DELTA_POINT_INDEX_DROP = 0x6f,
  DELTA_TYPE_CONSTRAINT_CREATE = 0x70,
  DELTA_TYPE_CONSTRAINT_DROP = 0x71,

  VALUE_FALSE = 0x00,
  VALUE_TRUE = 0xff,
};

/// List of all available markers.
/// IMPORTANT: Don't forget to update this list when you add a new Marker.
static const Marker kMarkersAll[] = {
    Marker::TYPE_NULL,
    Marker::TYPE_BOOL,
    Marker::TYPE_INT,
    Marker::TYPE_DOUBLE,
    Marker::TYPE_STRING,
    Marker::TYPE_LIST,
    Marker::TYPE_MAP,
    Marker::TYPE_TEMPORAL_DATA,
    Marker::TYPE_ZONED_TEMPORAL_DATA,
    Marker::TYPE_PROPERTY_VALUE,
    Marker::TYPE_ENUM,
    Marker::TYPE_POINT_2D,
    Marker::TYPE_POINT_3D,
    Marker::SECTION_VERTEX,
    Marker::SECTION_EDGE,
    Marker::SECTION_MAPPER,
    Marker::SECTION_METADATA,
    Marker::SECTION_INDICES,
    Marker::SECTION_CONSTRAINTS,
    Marker::SECTION_DELTA,
    Marker::SECTION_EPOCH_HISTORY,
    Marker::SECTION_EDGE_INDICES,
    Marker::SECTION_OFFSETS,
    Marker::SECTION_ENUMS,
    Marker::DELTA_VERTEX_CREATE,
    Marker::DELTA_VERTEX_DELETE,
    Marker::DELTA_VERTEX_ADD_LABEL,
    Marker::DELTA_VERTEX_REMOVE_LABEL,
    Marker::DELTA_VERTEX_SET_PROPERTY,
    Marker::DELTA_EDGE_CREATE,
    Marker::DELTA_EDGE_DELETE,
    Marker::DELTA_EDGE_SET_PROPERTY,
    Marker::DELTA_TRANSACTION_END,
    Marker::DELTA_LABEL_INDEX_CREATE,
    Marker::DELTA_LABEL_INDEX_DROP,
    Marker::DELTA_LABEL_INDEX_STATS_SET,
    Marker::DELTA_LABEL_INDEX_STATS_CLEAR,
    Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_SET,
    Marker::DELTA_LABEL_PROPERTY_INDEX_STATS_CLEAR,
    Marker::DELTA_LABEL_PROPERTY_INDEX_CREATE,
    Marker::DELTA_LABEL_PROPERTY_INDEX_DROP,
    Marker::DELTA_EDGE_INDEX_CREATE,
    Marker::DELTA_EDGE_INDEX_DROP,
    Marker::DELTA_EDGE_PROPERTY_INDEX_CREATE,
    Marker::DELTA_EDGE_PROPERTY_INDEX_DROP,
    Marker::DELTA_TEXT_INDEX_CREATE,
    Marker::DELTA_TEXT_INDEX_DROP,
    Marker::DELTA_EXISTENCE_CONSTRAINT_CREATE,
    Marker::DELTA_EXISTENCE_CONSTRAINT_DROP,
    Marker::DELTA_UNIQUE_CONSTRAINT_CREATE,
    Marker::DELTA_UNIQUE_CONSTRAINT_DROP,
    Marker::DELTA_ENUM_CREATE,
    Marker::DELTA_ENUM_ALTER_ADD,
    Marker::DELTA_ENUM_ALTER_UPDATE,
    Marker::DELTA_POINT_INDEX_CREATE,
    Marker::DELTA_POINT_INDEX_DROP,
    Marker::DELTA_TYPE_CONSTRAINT_CREATE,
    Marker::DELTA_TYPE_CONSTRAINT_DROP,
    Marker::VALUE_FALSE,
    Marker::VALUE_TRUE,
};

}  // namespace memgraph::storage::durability
