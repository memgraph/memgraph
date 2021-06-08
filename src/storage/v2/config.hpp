#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>
#include "storage/v2/transaction.hpp"

namespace storage {

/// Pass this class to the \ref Storage constructor to change the behavior of
/// the storage. This class also defines the default behavior.
struct Config {
  struct Gc {
    enum class Type { NONE, PERIODIC };

    Type type{Type::PERIODIC};
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

    uint64_t wal_file_size_kibibytes{20 * 1024};
    uint64_t wal_file_flush_every_n_tx{100000};

    bool snapshot_on_exit{false};

  } durability;
};

}  // namespace storage
