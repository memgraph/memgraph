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

#include "dbms/inmemory/history_replay.hpp"

#include <algorithm>
#include <chrono>
#include <exception>
#include <functional>
#include <limits>

#include <fmt/format.h>

#include "dbms/inmemory/replication_handlers.hpp"
#include "rpc/progress_heartbeat.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace memgraph::dbms {

std::optional<uint64_t> BranchRecoveryBound(const versioning::LedgerContents &ledger) {
  if (ledger.branches.empty()) return std::nullopt;
  const auto earliest = std::ranges::min(ledger.branches, std::less{}, [](const auto &b) { return b.info.fork_ts; });
  return earliest.info.fork_ts == 0 ? 0 : earliest.info.fork_ts - 1;
}

std::expected<std::vector<uint64_t>, std::string> ReplayHistoryTail(storage::InMemoryStorage &storage,
                                                                    const storage::durability::RecoveryTail &tail,
                                                                    std::span<const uint64_t> fork_ts) {
  std::vector<uint64_t> pins;
  pins.reserve(fork_ts.size());
  const auto release_all = [&] {
    for (const auto pin : pins) storage.ReleaseForkPin(pin);
    pins.clear();
  };

  // Runs before the txn committed at `durable_ts` is replayed: every fork at or below it already sees its full history.
  const auto pin_due = [&](uint64_t durable_ts) {
    while (pins.size() < fork_ts.size() && fork_ts[pins.size()] <= durable_ts) {
      pins.push_back(storage.RegisterForkPin());
    }
  };

  try {
    // Pins for forks at or before the recovered state must exist before the first replayed txn.
    pin_due(tail.reached_timestamp + 1);
    {
      const storage::InMemoryStorage::SuppressWalScope suppress_wal{storage};
      rpc::ProgressHeartbeat heartbeat{nullptr, std::chrono::hours{24}};
      const std::function<void(uint64_t)> on_txn_begin = pin_due;
      for (const auto &file : tail.wal_files) {
        if (!InMemoryReplicationHandlers::ReplayWalFile(file, &storage, heartbeat, &on_txn_begin)) {
          release_all();
          return std::unexpected(fmt::format("replaying WAL file '{}' failed", file.string()));
        }
      }
    }
    pin_due(std::numeric_limits<uint64_t>::max());
  } catch (const std::exception &e) {
    release_all();
    return std::unexpected(fmt::format("replaying the WAL tail threw: {}", e.what()));
  }

  storage.BumpClockPastDurable();
  return pins;
}

}  // namespace memgraph::dbms
