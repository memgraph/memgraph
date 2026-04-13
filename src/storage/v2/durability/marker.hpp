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

// Canonical Marker definition is in wire_format/marker.hpp.
// Re-exported here for backward compatibility with durability code.
#include <array>

#include "wire_format/marker.hpp"

namespace memgraph::storage::durability {
using wire_format::Marker;

/// List of all valid markers for snapshot/WAL validation.
/// IMPORTANT: Update this list when you add a new Marker value.
static constexpr std::array kMarkersAll = {
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
    Marker::TYPE_VECTOR_INDEX_ID,
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
    Marker::SECTION_TTL,
    Marker::SECTION_DESCRIPTIONS,
    Marker::DELTA_VERTEX_CREATE,
    Marker::DELTA_VERTEX_DELETE,
    Marker::DELTA_VERTEX_ADD_LABEL,
    Marker::DELTA_VERTEX_REMOVE_LABEL,
    Marker::DELTA_VERTEX_SET_PROPERTY,
    Marker::DELTA_EDGE_CREATE,
    Marker::DELTA_EDGE_DELETE,
    Marker::DELTA_EDGE_SET_PROPERTY,
    Marker::DELTA_TRANSACTION_END,
    Marker::DELTA_TRANSACTION_START,
    Marker::DELTA_LABEL_INDEX_CREATE,
    Marker::DELTA_LABEL_INDEX_DROP,
    Marker::DELTA_LABEL_INDEX_STATS_SET,
    Marker::DELTA_LABEL_INDEX_STATS_CLEAR,
    Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_SET,
    Marker::DELTA_LABEL_PROPERTIES_INDEX_STATS_CLEAR,
    Marker::DELTA_LABEL_PROPERTIES_INDEX_CREATE,
    Marker::DELTA_LABEL_PROPERTIES_INDEX_DROP,
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
    Marker::DELTA_VECTOR_INDEX_CREATE,
    Marker::DELTA_VECTOR_EDGE_INDEX_CREATE,
    Marker::DELTA_VECTOR_INDEX_DROP,
    Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_CREATE,
    Marker::DELTA_GLOBAL_EDGE_PROPERTY_INDEX_DROP,
    Marker::DELTA_TTL_OPERATION,
    Marker::DELTA_TEXT_EDGE_INDEX_CREATE,
    Marker::DELTA_DESCRIPTION_SET,
    Marker::DELTA_DESCRIPTION_DELETE,
    Marker::VALUE_FALSE,
    Marker::VALUE_TRUE,
};

}  // namespace memgraph::storage::durability
