/// @file
#pragma once

#include <unordered_map>
#include <vector>

#include "durability/single_node_ha/state_delta.hpp"
#include "transactions/type.hpp"

namespace database {

class GraphDb;

/// Interface for accessing transactions and applying StateDeltas on machines in
/// Raft follower mode.
class StateDeltaApplier final {
 public:
  explicit StateDeltaApplier(GraphDb *db);

  void Apply(const std::vector<StateDelta> &deltas);

 private:
  void Begin(const tx::TransactionId &tx_id);

  void Abort(const tx::TransactionId &tx_id);

  void Commit(const tx::TransactionId &tx_id);

  GraphDbAccessor *GetAccessor(const tx::TransactionId &tx_id);

  GraphDb *db_;
  std::unordered_map<tx::TransactionId, std::unique_ptr<GraphDbAccessor>>
      accessors_;
};

}  // namespace database
