#include <functional>

#include "distributed/data_manager.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "storage/edge.hpp"
#include "storage/vertex.hpp"

namespace distributed {

utils::Future<PullData> PullRpcClients::Pull(
    database::GraphDbAccessor &dba, int worker_id, int64_t plan_id,
    tx::CommandId command_id, const Parameters &params,
    const std::vector<query::Symbol> &symbols, bool accumulate,
    int batch_size) {
  return clients_.ExecuteOnWorker<PullData>(
      worker_id, [&dba, plan_id, command_id, params, symbols, accumulate,
                  batch_size](int worker_id, ClientPool &client_pool) {
        auto result = client_pool.Call<PullRpc>(
            dba.transaction_id(), dba.transaction().snapshot(), plan_id,
            command_id, params, symbols, accumulate, batch_size, true, true);

        auto handle_vertex = [&dba](auto &v) {
          dba.db()
              .data_manager()
              .Elements<Vertex>(dba.transaction_id())
              .emplace(v.global_address.gid(), std::move(v.old_record),
                       std::move(v.new_record));
          if (v.element_in_frame) {
            VertexAccessor va(v.global_address, dba);
            *v.element_in_frame = va;
          }
        };
        auto handle_edge = [&dba](auto &e) {
          dba.db()
              .data_manager()
              .Elements<Edge>(dba.transaction_id())
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
            path_in_frame.Expand(EdgeAccessor(p.edges[i].global_address, dba));
            handle_vertex(p.vertices[i + 1]);
            path_in_frame.Expand(
                VertexAccessor(p.vertices[i + 1].global_address, dba));
          }
        }

        return std::move(result->data.state_and_frames);
      });
}

std::vector<utils::Future<void>>
PullRpcClients::NotifyAllTransactionCommandAdvanced(tx::TransactionId tx_id) {
  return clients_.ExecuteOnWorkers<void>(
      0, [tx_id](int worker_id, auto &client) {
        auto res = client.template Call<TransactionCommandAdvancedRpc>(tx_id);
        CHECK(res) << "TransactionCommandAdvanceRpc failed";
      });
}

}  // namespace distributed
