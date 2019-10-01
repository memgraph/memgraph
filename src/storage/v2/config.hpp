#pragma once

#include <chrono>
#include <cstdint>
#include <filesystem>

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
    enum class SnapshotType { NONE, PERIODIC };

    std::filesystem::path storage_directory{"storage"};

    bool recover_on_startup{false};

    SnapshotType snapshot_type{SnapshotType::NONE};
    std::chrono::milliseconds snapshot_interval{std::chrono::minutes(2)};
    uint64_t snapshot_retention_count{3};
    bool snapshot_on_exit{false};
  } durability;
};

}  // namespace storage
