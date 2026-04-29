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

#include <cstdint>

#include "storage/v2/storage.hpp"

namespace memgraph::dbms {

struct DatabaseInfo {
  storage::StorageInfo storage_info;
  uint64_t triggers;
  uint64_t streams;
  int64_t db_memory_tracked{0};
  int64_t db_peak_memory_tracked{0};
  int64_t db_storage_memory_tracked{0};
  int64_t db_embedding_memory_tracked{0};
  int64_t db_query_memory_tracked{0};
};

static inline nlohmann::json ToJson(const DatabaseInfo &info) {
  auto res = ToJson(info.storage_info);
  res["triggers"] = info.triggers;
  res["streams"] = info.streams;
  res["db_memory_tracked"] = info.db_memory_tracked;
  res["db_peak_memory_tracked"] = info.db_peak_memory_tracked;
  res["db_storage_memory_tracked"] = info.db_storage_memory_tracked;
  res["db_embedding_memory_tracked"] = info.db_embedding_memory_tracked;
  res["db_query_memory_tracked"] = info.db_query_memory_tracked;
  return res;
}

}  // namespace memgraph::dbms
