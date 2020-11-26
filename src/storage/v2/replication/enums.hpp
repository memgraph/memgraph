#pragma once
#include <cstdint>

namespace storage {
enum class ReplicationMode : std::uint8_t { SYNC, ASYNC };

enum class ReplicaState : std::uint8_t {
  READY,
  REPLICATING,
  RECOVERY,
  INVALID
};
}  // namespace storage
