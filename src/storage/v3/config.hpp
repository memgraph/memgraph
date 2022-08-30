// Copyright 2022 Memgraph Ltd.
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

#include <chrono>
#include <cstdint>
#include <filesystem>
#include "storage/v3/isolation_level.hpp"
#include "storage/v3/transaction.hpp"

namespace memgraph::storage::v3 {

/// Pass this class to the \ref Storage constructor to change the behavior of
/// the storage. This class also defines the default behavior.
struct Config {
  struct Gc {
    // TODO(antaljanosbenjamin): How to handle garbage collection?
    enum class Type { NONE };

    Type type{Type::NONE};
    std::chrono::milliseconds interval{std::chrono::milliseconds(1000)};
  } gc;

  struct Items {
    bool properties_on_edges{true};
  } items;

  struct Durability {
    enum class SnapshotWalMode { DISABLED, PERIODIC_SNAPSHOT, PERIODIC_SNAPSHOT_WITH_WAL };

    std::filesystem::path storage_directory{"storage"};

    bool recover_on_startup{false};

    SnapshotWalMode snapshot_wal_mode{SnapshotWalMode::DISABLED};

    std::chrono::milliseconds snapshot_interval{std::chrono::minutes(2)};
    uint64_t snapshot_retention_count{3};

    uint64_t wal_file_size_kibibytes{static_cast<uint64_t>(20 * 1024)};
    uint64_t wal_file_flush_every_n_tx{100000};

    bool snapshot_on_exit{false};

  } durability;

  struct Transaction {
    IsolationLevel isolation_level{IsolationLevel::SNAPSHOT_ISOLATION};
  } transaction;
};

}  // namespace memgraph::storage::v3
