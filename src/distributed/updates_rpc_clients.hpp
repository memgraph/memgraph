#pragma once

#include <unordered_map>
#include <vector>

#include "database/state_delta.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "distributed/updates_rpc_messages.hpp"
#include "query/typed_value.hpp"
#include "storage/address_types.hpp"
#include "storage/gid.hpp"
#include "storage/types.hpp"
#include "transactions/type.hpp"
#include "utils/future.hpp"

namespace distributed {

/// Exposes the functionality to send updates to other workers (that own the
/// graph element we are updating). Also enables us to call for a worker to
/// apply the accumulated deferred updates, or discard them.
class UpdatesRpcClients {
 public:
  explicit UpdatesRpcClients(RpcWorkerClients &clients)
      : worker_clients_(clients) {}

  /// Sends an update delta to the given worker.
  UpdateResult Update(int worker_id, const database::StateDelta &delta);

  /// Creates a vertex on the given worker and returns it's id.
  CreatedVertexInfo CreateVertex(
      int worker_id, tx::TransactionId tx_id,
      const std::vector<storage::Label> &labels,
      const std::unordered_map<storage::Property, PropertyValue>
          &properties,
      std::experimental::optional<int64_t> cypher_id =
          std::experimental::nullopt);

  /// Creates an edge on the given worker and returns it's address. If the `to`
  /// vertex is on the same worker as `from`, then all remote CRUD will be
  /// handled by a call to this function. Otherwise a separate call to
  /// `AddInEdge` might be necessary. Throws all the exceptions that can
  /// occur remotely as a result of updating a vertex.
  CreatedEdgeInfo CreateEdge(tx::TransactionId tx_id, VertexAccessor &from,
                             VertexAccessor &to, storage::EdgeType edge_type,
                             std::experimental::optional<int64_t> cypher_id =
                                 std::experimental::nullopt);
  // TODO (buda): Another machine in the cluster is asked to create an edge.
  // cypher_id should be generated in that process. It probably doesn't make
  // sense to have optional cypher id here. Maybe for the recovery purposes.

  /// Adds the edge with the given address to the `to` vertex as an incoming
  /// edge. Only used when `to` is remote and not on the same worker as `from`.
  void AddInEdge(tx::TransactionId tx_id, VertexAccessor &from,
                 storage::EdgeAddress edge_address, VertexAccessor &to,
                 storage::EdgeType edge_type);

  /// Removes a vertex from the other worker.
  void RemoveVertex(int worker_id, tx::TransactionId tx_id, gid::Gid gid,
                    bool check_empty);

  /// Removes an edge on another worker. This also handles the `from` vertex
  /// outgoing edge, as that vertex is on the same worker as the edge. If the
  /// `to` vertex is on the same worker, then that side is handled too by the
  /// single RPC call, otherwise a separate call has to be made to
  /// RemoveInEdge.
  void RemoveEdge(tx::TransactionId tx_id, int worker_id, gid::Gid edge_gid,
                  gid::Gid vertex_from_id,
                  storage::VertexAddress vertex_to_addr);

  void RemoveInEdge(tx::TransactionId tx_id, int worker_id, gid::Gid vertex_id,
                    storage::EdgeAddress edge_address);

  /// Calls for all the workers (except the given one) to apply their updates
  /// and returns the future results.
  std::vector<utils::Future<UpdateResult>> UpdateApplyAll(
      int skip_worker_id, tx::TransactionId tx_id);

 private:
  RpcWorkerClients &worker_clients_;
};

}  // namespace distributed
