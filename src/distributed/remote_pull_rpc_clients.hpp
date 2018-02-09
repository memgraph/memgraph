#pragma once

#include <functional>
#include <vector>

#include "database/graph_db_accessor.hpp"
#include "distributed/remote_data_manager.hpp"
#include "distributed/remote_pull_produce_rpc_messages.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "query/frontend/semantic/symbol.hpp"
#include "query/parameters.hpp"
#include "transactions/type.hpp"

namespace distributed {

/** Provides means of calling for the execution of a plan on some remote worker,
 * and getting the results of that execution. The results are returned in
 * batches and are therefore accompanied with an enum indicator of the state of
 * remote execution. */
class RemotePullRpcClients {
  using ClientPool = communication::rpc::ClientPool;

 public:
  RemotePullRpcClients(Coordination &coordination)
      : clients_(coordination, kRemotePullProduceRpcName) {}

  /// Calls a remote pull asynchroniously. IMPORTANT: take care not to call this
  /// function for the same (tx_id, worker_id, plan_id) before the previous call
  /// has ended.
  std::future<RemotePullData> RemotePull(
      database::GraphDbAccessor &dba, int worker_id, int64_t plan_id,
      const Parameters &params, const std::vector<query::Symbol> &symbols,
      int batch_size = kDefaultBatchSize) {
    return clients_.ExecuteOnWorker<RemotePullData>(
        worker_id,
        [&dba, plan_id, params, symbols, batch_size](ClientPool &client_pool) {
          auto result = client_pool.Call<RemotePullRpc>(
              dba.transaction_id(), plan_id, params, symbols, batch_size, true,
              true);

          auto handle_vertex = [&dba](auto &v) {
            dba.db()
                .remote_data_manager()
                .Vertices(dba.transaction_id())
                .emplace(v.global_address.gid(), std::move(v.old_record),
                         std::move(v.new_record));
            if (v.element_in_frame) {
              VertexAccessor va(v.global_address, dba);
              *v.element_in_frame = va;
            }
          };
          auto handle_edge = [&dba](auto &e) {
            dba.db()
                .remote_data_manager()
                .Edges(dba.transaction_id())
                .emplace(e.global_address.gid(), std::move(e.old_record),
                         std::move(e.new_record));
            if (e.element_in_frame) {
              EdgeAccessor ea(e.global_address, dba);
              *e.element_in_frame = ea;
            }
          };
          for (auto &v : result->data.vertices) handle_vertex(v);
          for (auto &e : result->data.edges) handle_edge(e);
          for (auto &p : result->data.paths) {
            handle_vertex(p.vertices[0]);
            p.path_in_frame =
                query::Path(VertexAccessor(p.vertices[0].global_address, dba));
            query::Path &path_in_frame = p.path_in_frame.ValuePath();
            for (size_t i = 0; i < p.edges.size(); ++i) {
              handle_edge(p.edges[i]);
              path_in_frame.Expand(
                  EdgeAccessor(p.edges[i].global_address, dba));
              handle_vertex(p.vertices[i + 1]);
              path_in_frame.Expand(
                  VertexAccessor(p.vertices[i + 1].global_address, dba));
            }
          }

          return std::move(result->data.state_and_frames);
        });
  }

  auto GetWorkerIds() { return clients_.GetWorkerIds(); }

  // Notifies a worker that the given transaction/plan is done. Otherwise the
  // server is left with potentially unconsumed Cursors that never get deleted.
  //
  // TODO - maybe this needs to be done with hooks into the transactional
  // engine, so that the Worker discards it's stuff when the relevant
  // transaction are done.
  std::future<void> EndRemotePull(int worker_id, tx::transaction_id_t tx_id,
                                  int64_t plan_id) {
    return clients_.ExecuteOnWorker<void>(
        worker_id, [tx_id, plan_id](ClientPool &client_pool) {
          return client_pool.Call<EndRemotePullRpc>(
              EndRemotePullReqData{tx_id, plan_id});
        });
  }

  void EndAllRemotePulls(tx::transaction_id_t tx_id, int64_t plan_id) {
    std::vector<std::future<void>> futures;
    for (auto worker_id : clients_.GetWorkerIds()) {
      if (worker_id == 0) continue;
      futures.emplace_back(EndRemotePull(worker_id, tx_id, plan_id));
    }
    for (auto &future : futures) future.wait();
  }

 private:
  RpcWorkerClients clients_;
};
}  // namespace distributed
