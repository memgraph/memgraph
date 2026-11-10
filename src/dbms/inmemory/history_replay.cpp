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
#include <functional>
#include <limits>

#include <fmt/format.h>

#include "dbms/inmemory/replication_handlers.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/transaction_constants.hpp"

namespace memgraph::dbms {

namespace {
void RecordWalEpoch(storage::ReplicationStorageState &state, std::string const &epoch_id, uint64_t ldt) {
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

std::optional<uint64_t> BranchRecoveryBound(const versioning::LedgerContents &ledger) {
  if (ledger.branches.empty()) return std::nullopt;
  const auto earliest = std::ranges::min(ledger.branches, std::less{}, [](const auto &b) { return b.info.fork_ts; });
  return versioning::RecoverableBelow(earliest.info);
}

std::expected<std::vector<uint64_t>, std::string> ReplayHistoryTail(storage::InMemoryStorage &storage,
                                                                    const storage::durability::RecoveryTail &tail,
                                                                    std::span<const uint64_t> fork_ts) {
  // Pins are taken positionally as replay crosses each fork: unsorted input would pin a later fork too late.
  if (!std::ranges::is_sorted(fork_ts)) return std::unexpected("fork timestamps must be ascending");
  // The recovered state already holds the commit at reached_timestamp, which such a fork must not see. Commit
  // timestamps start at 1, so 0 means no commit and a fork at 0 is past it.
  if (!fork_ts.empty() && tail.reached_timestamp != storage::kTimestampInitialId &&
      fork_ts.front() <= tail.reached_timestamp) {
    return std::unexpected(
        fmt::format("fork at {} is not past the recovered state at {}", fork_ts.front(), tail.reached_timestamp));
  }

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

  try {
    pin_due(tail.reached_timestamp + 1);
    {
      const storage::InMemoryStorage::SuppressWalScope suppress_wal{storage};
      for (const auto &file : tail.wal_files) {
        const auto info = storage::durability::ReadWalContents(file);
        const auto ldt_before = LastDurableTimestamp(storage);
        if (info.to_timestamp <= ldt_before) continue;
        if (!InMemoryReplicationHandlers::ReplayWalFile(file, info, &storage, pin_due)) {
          return failed(fmt::format("replaying WAL file '{}' failed", file.string()));
        }
        if (const auto ldt_after = LastDurableTimestamp(storage); ldt_after != ldt_before) {
          RecordWalEpoch(storage.repl_storage_state_, info.epoch_id, ldt_after);
        }
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
