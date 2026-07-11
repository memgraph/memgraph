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

#include "versioning/branch_log.hpp"

#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "utils/logging.hpp"

namespace memgraph::versioning {

namespace {
// A branch log is always a single, self-contained file for this chunk: it never needs to agree
// with -- or even be aware of -- main's sequence numbering (R21), so it always starts at 0.
constexpr uint64_t kBranchLogSeqNum{0};
}  // namespace

BranchLog::BranchLog(std::filesystem::path branch_log_directory, storage::SalientConfig::Items items,
                     storage::NameIdMapper *name_id_mapper)
    : uuid_{},
      file_retainer_{},
      epoch_id_{utils::GenerateUUID()},
      wal_file_(std::move(branch_log_directory), uuid_, epoch_id_, items, name_id_mapper, kBranchLogSeqNum,
                &file_retainer_) {}

void BranchLog::AppendDelta(const storage::Delta &delta, storage::Vertex *vertex, uint64_t timestamp,
                            storage::Storage *storage) {
  wal_file_.AppendDelta(delta, vertex, timestamp, storage);
}

void BranchLog::AppendDelta(const storage::Delta &delta, storage::Edge *edge, uint64_t timestamp,
                            storage::Storage *storage, storage::Gid in_vertex_gid, storage::EdgeTypeId edge_type_id) {
  wal_file_.AppendDelta(delta, edge, timestamp, storage, in_vertex_gid, edge_type_id);
}

storage::durability::WalTxnEndPos BranchLog::AppendTransactionEnd(uint64_t timestamp) {
  return wal_file_.AppendTransactionEnd(timestamp);
}

void BranchLog::Finalize() { wal_file_.FinalizeWal(); }

std::vector<storage::durability::WalDeltaData> BranchLog::ReadAll(const std::filesystem::path &branch_log_file) {
  auto const info = storage::durability::ReadWalInfo(branch_log_file);

  storage::durability::Decoder decoder;
  MG_ASSERT(decoder.Initialize(branch_log_file, storage::durability::kWalMagic),
            "Failed to open branch log file {} for read-back",
            branch_log_file);
  decoder.SetPosition(info.offset_deltas);

  std::vector<storage::durability::WalDeltaData> result;
  result.reserve(info.num_deltas);
  for (uint64_t i = 0; i < info.num_deltas; ++i) {
    storage::durability::ReadWalDeltaHeader(&decoder);
    result.push_back(storage::durability::ReadWalDeltaData(&decoder));
  }
  return result;
}

}  // namespace memgraph::versioning
