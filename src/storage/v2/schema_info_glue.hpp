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

#include "storage/v2/schema_info.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"

namespace memgraph::storage {
inline std::optional<SchemaInfo::ModifyingAccessor> SchemaInfoAccessor(Storage *storage, Transaction *transaction) {
  if (!storage->config_.salient.items.enable_schema_info) return std::nullopt;
  const auto prop_on_edges = storage->config_.salient.items.properties_on_edges;
  if (storage->GetStorageMode() == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    return SchemaInfo::CreateVertexModifyingAccessor(transaction->schema_diff_, transaction->post_process_,
                                                     transaction->start_timestamp, transaction->transaction_id,
                                                     prop_on_edges);
  }
  return storage->schema_info_.CreateVertexModifyingAccessor(prop_on_edges);
}

inline std::optional<SchemaInfo::ModifyingAccessor> SchemaInfoUniqueAccessor(Storage *storage,
                                                                             Transaction *transaction) {
  if (!storage->config_.salient.items.enable_schema_info) return std::nullopt;
  const auto prop_on_edges = storage->config_.salient.items.properties_on_edges;
  if (storage->GetStorageMode() == StorageMode::IN_MEMORY_TRANSACTIONAL) {
    return SchemaInfo::CreateEdgeModifyingAccessor(transaction->schema_diff_, &transaction->post_process_,
                                                   transaction->start_timestamp, transaction->transaction_id,
                                                   prop_on_edges);
  }
  return storage->schema_info_.CreateEdgeModifyingAccessor(prop_on_edges);
}
}  // namespace memgraph::storage
