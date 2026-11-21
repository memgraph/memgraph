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

#ifdef MG_ENTERPRISE

#include "dbms/inmemory/history_replay.hpp"

#include <algorithm>
#include <exception>
#include <filesystem>
#include <functional>
#include <limits>
#include <mutex>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "memory/db_arena_fwd.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/transaction_constants.hpp"

namespace memgraph::dbms {

namespace {
void RecordWalEpoch(storage::InMemoryStorage &storage, std::string const &epoch_id, uint64_t ldt) {
  // CreateSnapshot reads the epoch and history under engine_lock_, and its runner starts with the storage.
  auto const engine_guard = std::scoped_lock{storage.engine_lock_};
  auto &state = storage.repl_storage_state_;
  if (!state.history.empty() && state.history.back().first == epoch_id) {
    state.history.back().second = ldt;
  } else {
    state.history.emplace_back(epoch_id, ldt);
    state.epoch_.SetEpoch(epoch_id);
  }
}

uint64_t LastDurableTimestamp(storage::InMemoryStorage const &storage) {
  return storage.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
}
}  // namespace

bool ReplayWalFile(storage::InMemoryStorage &storage, std::filesystem::path const &wal_path,
                   storage::durability::WalInfo const &wal_info, std::function<void(uint64_t)> on_txn_begin) {
  storage::durability::Decoder decoder;
  const auto version = decoder.Initialize(wal_path, storage::durability::kWalMagic);
  if (!version || !storage::durability::IsVersionSupported(*version)) {
    spdlog::error("WAL file {} has no valid magic and version", wal_path);
    return false;
  }
  decoder.SetPosition(wal_info.offset_deltas);
  const storage::WalApplyOptions options{
      .mode = storage::WalApplyMode::kWalFile, .on_txn_begin = std::move(on_txn_begin), .persist_wal = false};
  uint64_t delta_idx{0};
  while (delta_idx < wal_info.num_deltas) {
    const auto res = storage.ApplyWalTransaction(&decoder, *version, options);
    if (!res) return false;
    delta_idx += res->current_delta_idx;
  }
  // Overrunning the stated count means a transaction-end marker was lost.
  return delta_idx == wal_info.num_deltas;
}

std::optional<uint64_t> BranchRecoveryBound(const versioning::LedgerContents &ledger) {
  if (ledger.branches.empty()) return std::nullopt;
  const auto earliest =
      std::ranges::min(ledger.branches, std::less{}, [](const auto &b) { return b.info.fork.durable_fork_ts; });
  return versioning::RecoverableBelow(earliest.info);
}

std::expected<std::vector<uint64_t>, std::string> ReplayHistoryTail(storage::InMemoryStorage &storage,
                                                                    const storage::durability::RecoveryTail &tail,
                                                                    std::span<const uint64_t> fork_ts) {
  // Pins are taken positionally as replay crosses each fork: unsorted input would pin a later fork too late.
  DMG_ASSERT(std::ranges::is_sorted(fork_ts), "fork timestamps come from the ledger and must be ascending");
  // The recovered state already holds the commit at reached_timestamp, which such a fork must not see. Commit
  // timestamps start at 1, so 0 means no commit and a fork at 0 is past it.
  if (!fork_ts.empty() && tail.reached_timestamp != storage::kTimestampInitialId &&
      fork_ts.front() <= tail.reached_timestamp) {
    return std::unexpected(
        fmt::format("fork at {} is not past the recovered state at {}", fork_ts.front(), tail.reached_timestamp));
  }

  const memory::DbArenaScope db_arena_scope{storage.DbArenaPool()};

  std::vector<uint64_t> pins;
  pins.reserve(fork_ts.size());
  // Runs before the txn committed at `durable_ts` is replayed: every fork at or below it already sees its full history.
  const std::function<void(uint64_t)> pin_due = [&](uint64_t durable_ts) {
    while (pins.size() < fork_ts.size() && fork_ts[pins.size()] <= durable_ts) {
      pins.push_back(storage.RegisterForkPin());
    }
  };

  const auto failed = [&](std::string reason) -> std::unexpected<std::string> {
    for (const auto pin : pins) storage.ReleaseForkPin(pin);
    return std::unexpected(std::move(reason));
  };

  const storage::InMemoryStorage::PauseSnapshotsScope no_snapshots{storage};

  try {
    pin_due(tail.reached_timestamp + 1);
    for (const auto &file : tail.wal_files) {
      const auto info = storage::durability::ReadWalContents(file);
      const auto ldt_before = LastDurableTimestamp(storage);
      if (info.to_timestamp <= ldt_before) continue;
      if (!ReplayWalFile(storage, file, info, pin_due)) {
        return failed(fmt::format("replaying WAL file '{}' failed", file.string()));
      }
      if (const auto ldt_after = LastDurableTimestamp(storage); ldt_after != ldt_before) {
        RecordWalEpoch(storage, info.epoch_id, ldt_after);
      }
    }
    pin_due(std::numeric_limits<uint64_t>::max());
  } catch (const std::exception &e) {
    return failed(fmt::format("replaying the WAL tail threw: {}", e.what()));
  }

  // Later commits must not fall below a recorded fork, or a bounded recovery at that fork would include them.
  storage.BumpClockPastDurable(fork_ts.empty() ? 0 : fork_ts.back());
  return pins;
}

}  // namespace memgraph::dbms
#endif
