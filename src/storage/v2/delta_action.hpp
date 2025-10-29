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

#include <cstdint>

namespace memgraph::storage {
enum class DeltaAction : std::uint8_t {
  /// Use for Vertex and Edge
  /// Used for disk storage for modifying MVCC logic and storing old key. Storing old key is necessary for
  /// deleting old-data (compaction).
  DELETE_DESERIALIZED_OBJECT,
  DELETE_OBJECT,
  RECREATE_OBJECT,
  SET_PROPERTY,
  SET_VECTOR_PROPERTY,

  // Used only for Vertex
  ADD_LABEL,
  REMOVE_LABEL,
  ADD_IN_EDGE,
  ADD_OUT_EDGE,
  REMOVE_IN_EDGE,
  REMOVE_OUT_EDGE,
};
}
