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

#include "storage_test_utils.hpp"

size_t CountVertices(memgraph::storage::Storage::Accessor &storage_accessor, memgraph::storage::View view) {
  auto vertices = storage_accessor.Vertices(view);
  size_t count = 0U;
  for (auto it = vertices.begin(); it != vertices.end(); ++it, ++count)
    ;
  return count;
}

std::string_view StorageModeToString(memgraph::storage::StorageMode storage_mode) {
  switch (storage_mode) {
    case memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL:
      return "IN_MEMORY_ANALYTICAL";
    case memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL:
      return "IN_MEMORY_TRANSACTIONAL";
    case memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL:
      return "ON_DISK_TRANSACTIONAL";
  }
}
