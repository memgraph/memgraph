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
#include <functional>

#include "storage/v2/access_type.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/pipeline_budget.hpp"

namespace memgraph::storage {

class Storage;
struct Vertex;
struct Edge;
struct MetadataDelta;

/// One MVCC delta resolved by the commit thread's traversal: the delta, the object it belongs to (exactly one of
/// vertex/edge set), and the edge lookup data workers cannot pull from the transaction.
struct TxnDataCommand {
  Delta const *delta;
  Vertex *vertex;
  Edge *edge;
  Gid in_vertex_gid;
  EdgeTypeId edge_type_id;
};

/// Everything a transaction writes, in encode order. Built once on the commit thread; the WAL encoder and every
/// replica worker encode from it concurrently, so it must outlive all of their tasks. The vectors allocate through
/// the policy they are given: a budget-charging one for a pipelined commit, the plain allocator otherwise.
struct TxnCommands {
  explicit TxnCommands(TxnAllocPolicy policy)
      : metadata{BudgetAllocator<MetadataDelta const *>{policy}}, data{BudgetAllocator<TxnDataCommand>{policy}} {}

  BudgetVector<MetadataDelta const *> metadata;
  BudgetVector<TxnDataCommand> data;
};

/// Encodes a whole transaction (start frame, metadata, data, end frame) from materialized commands into `encoder`,
/// emitting exactly the frames the inline WAL path emits and invoking `progress` where that path invokes the
/// replica progress callback. Positions in the result are the encoder's native positions, not adjusted: absolute
/// file offsets on a file encoder, buffer-relative on a fresh buffer that starts at zero.
/// WalFile::AppendEncodedTransaction adds the destination file base exactly once.
auto EncodeTxnCommandsTo(durability::BaseEncoder &encoder, TxnCommands const &commands, Storage *storage,
                         SalientConfig::Items const &items, uint64_t durability_ts, bool commit,
                         StorageAccessType access_type, std::function<void()> const &progress)
    -> durability::TxnEncodeResult;

}  // namespace memgraph::storage
