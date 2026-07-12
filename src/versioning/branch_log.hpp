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
#include <filesystem>
#include <string>
#include <vector>

#include "storage/v2/durability/wal.hpp"
#include "utils/file_locker.hpp"
#include "utils/uuid.hpp"

namespace memgraph::versioning {

// A standalone per-branch WAL-format change-log.
//
// Reuses the existing WAL format/writer/reader unchanged (storage::durability::WalFile for
// append, storage::durability::ReadWalDeltaHeader/ReadWalDeltaData for read-back) -- there is no
// new on-disk record format here (spec A.1/R3). BranchLog is a thin owner of a per-branch WAL
// file living in its OWN directory with its OWN fresh uuid/seq numbering: it is a SEPARATE stream
// from main's shared, sequence-checked WAL and must never share or perturb it (spec R21).
//
// This is chunk 3a: a standalone write -> read round-trip utility only. No branch transaction is
// wired into it yet -- that lands in chunk 5b, once the branch overlay's write target exists. The
// forwarding Append* members below are the future capture surface: chunk 5b will feed the
// branch's own committed deltas through them as they are produced.
class BranchLog {
 public:
  // `branch_log_directory` must be the branch's OWN directory (never main's WAL directory --
  // R21). Creates a fresh WalFile there with a fresh UUID and epoch -- it never needs to agree
  // with -- or even be aware of -- main's sequence numbering.
  //
  // `seq_num`: durable-capture MULTI-COMMIT fix (2026-07-12) -- each `BranchLog` now covers
  // EXACTLY ONE captured commit (see `versioning::BranchContext::CreateCommitLog`, branch_engine.
  // hpp, for why: a single BranchLog file holding MULTIPLE transactions does not round-trip --
  // `storage::durability::ReadWalInfo`'s per-transaction scan (wal.cpp) stops counting deltas the
  // moment it sees a SECOND transaction's differing timestamp, so `BranchLog::ReadAll` silently
  // returned only the FIRST commit's records for a multi-commit file). The caller passes its own
  // monotonic per-session commit counter here (`BranchContext::NextBranchCommitTs()`) -- embedded
  // in the underlying WalFile's own metadata (readable back via `storage::durability::ReadWalInfo`'s
  // `seq_num` field) so `CollectBranchChangelog` (interpreter.cpp) can recover the correct COMMIT
  // ORDER across the (now many) per-commit files in one session directory even if their wall-clock
  // filename prefixes ever collide (see that function's own doc-comment for the full ordering
  // contract). Distinct across commits within one session by construction (a strictly-increasing
  // counter); distinctness across DIFFERENT sessions is not relied upon (each session's counter
  // restarts from 0 -- ordering across sessions still comes from the wall-clock filename prefix).
  BranchLog(std::filesystem::path branch_log_directory, storage::SalientConfig::Items items,
            storage::NameIdMapper *name_id_mapper, uint64_t seq_num);

  BranchLog(const BranchLog &) = delete;
  BranchLog &operator=(const BranchLog &) = delete;
  BranchLog(BranchLog &&) = delete;
  BranchLog &operator=(BranchLog &&) = delete;
  ~BranchLog() = default;

  // Forwarding passthroughs to the internal WalFile. Pure capture -- no interpretation happens
  // here, so the branch log is byte-for-byte whatever main's WAL writer would have produced for
  // the same deltas (structural completeness, spec R8).
  void AppendDelta(const storage::Delta &delta, storage::Vertex *vertex, uint64_t timestamp, storage::Storage *storage);
  void AppendDelta(const storage::Delta &delta, storage::Edge *edge, uint64_t timestamp, storage::Storage *storage,
                   storage::Gid in_vertex_gid, storage::EdgeTypeId edge_type_id);
  storage::durability::WalTxnEndPos AppendTransactionEnd(uint64_t timestamp);

  // Finalizes (renames to its final from/to-timestamp name and closes) the underlying WalFile so
  // it becomes readable by `ReadAll`. At most one call is supported; nothing may be appended
  // afterwards.
  void Finalize();

  // The on-disk path of the branch log file. Only meaningful as the FINAL path after Finalize()
  // has been called (WalFile renames the file on finalize).
  const std::filesystem::path &Path() const { return wal_file_.Path(); }

  // Reads back every forward WalDeltaData record from a finalized branch log file, in the order
  // they were written, using the same decode path main's WAL recovery uses (R20) -- no new decode
  // logic. `branch_log_file` must be a path previously returned by Path() after Finalize().
  static std::vector<storage::durability::WalDeltaData> ReadAll(const std::filesystem::path &branch_log_file);

 private:
  utils::UUID uuid_;
  utils::FileRetainer file_retainer_;
  // Stored (rather than passed as a transient temporary) so WalFile's std::string_view epoch_id
  // parameter is backed by a stable object for the full lifetime of the constructor call.
  std::string epoch_id_;
  storage::durability::WalFile wal_file_;
};

}  // namespace memgraph::versioning
