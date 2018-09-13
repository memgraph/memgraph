#pragma once

#include <experimental/optional>
#include <mutex>
#include <set>
#include <unordered_map>

#include "distributed/coordination.hpp"
#include "durability/recovery.hpp"
#include "io/network/endpoint.hpp"

namespace distributed {
using Endpoint = io::network::Endpoint;

/** Handles worker registration, getting of other workers' endpoints and
 * coordinated shutdown in a distributed memgraph. Master side. */
class MasterCoordination final : public Coordination {
 public:
  explicit MasterCoordination(const Endpoint &master_endpoint);

  /** Shuts down all the workers and this master server. */
  ~MasterCoordination();

  /** Registers a new worker with this master coordination.
   *
   * @param desired_worker_id - The ID the worker would like to have.
   * @return True if the desired ID for the worker is available, or false
   * if the desired ID is already taken.
   */
  bool RegisterWorker(int desired_worker_id, Endpoint endpoint);

  /*
   * Worker `worker_id` finished with recovering, adds it to the set of
   * recovered workers alongside with its recovery_info.
   */
  void WorkerRecoveredSnapshot(
      int worker_id, const std::experimental::optional<durability::RecoveryInfo>
                         &recovery_info);

  Endpoint GetEndpoint(int worker_id);

  /// Sets the recovery info. nullopt indicates nothing was recovered.
  void SetRecoveredSnapshot(
      std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
          recovered_snapshot);

  std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
  RecoveredSnapshotTx() const;

  int CountRecoveredWorkers() const;

  std::vector<tx::TransactionId> CommonWalTransactions(
      const durability::RecoveryInfo &master_info) const;

 private:
  // Most master functions aren't thread-safe.
  mutable std::mutex lock_;

  /// Durabilility recovery info.
  /// Indicates if the recovery phase is done.
  bool recovery_done_{false};
  /// Set of workers that finished sucesfully recovering snapshot
  std::map<int, std::experimental::optional<durability::RecoveryInfo>>
      recovered_workers_;
  /// If nullopt nothing was recovered.
  std::experimental::optional<std::pair<int64_t, tx::TransactionId>>
      recovered_snapshot_tx_;
};

}  // namespace distributed
