/// @file

#pragma once

#include "durability/single_node_ha/state_delta.hpp"

namespace raft {

/// Exposes only functionality that other parts of Memgraph can interact with,
/// emplacing a state delta into the appropriate Raft log entry.
class RaftInterface {
 public:
  /// Add StateDelta to the appropriate Raft log entry.
  virtual void Emplace(const database::StateDelta &) = 0;

 protected:
  ~RaftInterface() {}
};

}  // namespace raft
