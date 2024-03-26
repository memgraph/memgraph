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

namespace memgraph::storage::durability {

/// Enum used to indicate a global database operation that isn't transactional.
enum class StorageMetadataOperation {
  LABEL_INDEX_CREATE,
  LABEL_INDEX_DROP,
  LABEL_INDEX_STATS_SET,
  LABEL_INDEX_STATS_CLEAR,
  LABEL_PROPERTY_INDEX_CREATE,
  LABEL_PROPERTY_INDEX_DROP,
  LABEL_PROPERTY_INDEX_STATS_SET,
  LABEL_PROPERTY_INDEX_STATS_CLEAR,
  EDGE_TYPE_INDEX_CREATE,
  EDGE_TYPE_INDEX_DROP,
  TEXT_INDEX_CREATE,
  TEXT_INDEX_DROP,
  EXISTENCE_CONSTRAINT_CREATE,
  EXISTENCE_CONSTRAINT_DROP,
  UNIQUE_CONSTRAINT_CREATE,
  UNIQUE_CONSTRAINT_DROP,
};

}  // namespace memgraph::storage::durability
