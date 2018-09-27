#include "bfs_rpc_clients.hpp"

#include "database/distributed_graph_db.hpp"
#include "distributed/bfs_rpc_messages.hpp"
#include "distributed/data_manager.hpp"

namespace distributed {

BfsRpcClients::BfsRpcClients(database::DistributedGraphDb *db,
                             BfsSubcursorStorage *subcursor_storage,
                             Coordination *coordination,
                             DataManager *data_manager)
    : db_(db),
      subcursor_storage_(subcursor_storage),
      coordination_(coordination),
      data_manager_(data_manager) {}

std::unordered_map<int16_t, int64_t> BfsRpcClients::CreateBfsSubcursors(
    tx::TransactionId tx_id, query::EdgeAtom::Direction direction,
    const std::vector<storage::EdgeType> &edge_types,
    query::GraphView graph_view) {
  auto futures = coordination_->ExecuteOnWorkers<std::pair<int16_t, int64_t>>(
      db_->WorkerId(),
      [tx_id, direction, &edge_types, graph_view](int worker_id, auto &client) {
        auto res = client.template Call<CreateBfsSubcursorRpc>(
            tx_id, direction, edge_types, graph_view);
        return std::make_pair(worker_id, res.member);
      });
  std::unordered_map<int16_t, int64_t> subcursor_ids;
  subcursor_ids.emplace(
      db_->WorkerId(),
      subcursor_storage_->Create(tx_id, direction, edge_types, graph_view));
  for (auto &future : futures) {
    auto got = subcursor_ids.emplace(future.get());
    CHECK(got.second) << "CreateBfsSubcursors failed: duplicate worker id";
  }
  return subcursor_ids;
}

void BfsRpcClients::RegisterSubcursors(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids) {
  auto futures = coordination_->ExecuteOnWorkers<void>(
      db_->WorkerId(), [&subcursor_ids](int worker_id, auto &client) {
        client.template Call<RegisterSubcursorsRpc>(subcursor_ids);
      });
  subcursor_storage_->Get(subcursor_ids.at(db_->WorkerId()))
      ->RegisterSubcursors(subcursor_ids);
  // Wait and get all of the replies.
  for (auto &future : futures) {
    if (future.valid()) future.get();
  }
}

void BfsRpcClients::ResetSubcursors(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids) {
  auto futures = coordination_->ExecuteOnWorkers<void>(
      db_->WorkerId(), [&subcursor_ids](int worker_id, auto &client) {
        client.template Call<ResetSubcursorRpc>(
            subcursor_ids.at(worker_id));
      });
  subcursor_storage_->Get(subcursor_ids.at(db_->WorkerId()))->Reset();
  // Wait and get all of the replies.
  for (auto &future : futures) {
    if (future.valid()) future.get();
  }
}

void BfsRpcClients::RemoveBfsSubcursors(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids) {
  auto futures = coordination_->ExecuteOnWorkers<void>(
      db_->WorkerId(), [&subcursor_ids](int worker_id, auto &client) {
        client.template Call<RemoveBfsSubcursorRpc>(
            subcursor_ids.at(worker_id));
      });
  subcursor_storage_->Erase(subcursor_ids.at(db_->WorkerId()));
  // Wait and get all of the replies.
  for (auto &future : futures) {
    if (future.valid()) future.get();
  }
}

std::experimental::optional<VertexAccessor> BfsRpcClients::Pull(
    int16_t worker_id, int64_t subcursor_id, database::GraphDbAccessor *dba) {
  if (worker_id == db_->WorkerId()) {
    return subcursor_storage_->Get(subcursor_id)->Pull();
  }

  auto res = coordination_->GetClientPool(worker_id)->CallWithLoad<SubcursorPullRpc>(
      [this, dba](const auto &reader) {
        SubcursorPullRes res;
        res.Load(reader, dba, this->data_manager_);
        return res;
      },
      subcursor_id);
  return res.vertex;
}

bool BfsRpcClients::ExpandLevel(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids) {
  auto futures = coordination_->ExecuteOnWorkers<bool>(
      db_->WorkerId(), [&subcursor_ids](int worker_id, auto &client) {
        auto res =
            client.template Call<ExpandLevelRpc>(subcursor_ids.at(worker_id));
        return res.member;
      });
  bool expanded =
      subcursor_storage_->Get(subcursor_ids.at(db_->WorkerId()))->ExpandLevel();
  for (auto &future : futures) {
    expanded |= future.get();
  }
  return expanded;
}

void BfsRpcClients::SetSource(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids,
    storage::VertexAddress source_address) {
  CHECK(source_address.is_remote())
      << "SetSource should be called with global address";

  int worker_id = source_address.worker_id();
  if (worker_id == db_->WorkerId()) {
    subcursor_storage_->Get(subcursor_ids.at(db_->WorkerId()))
        ->SetSource(source_address);
  } else {
    coordination_->GetClientPool(worker_id)->Call<SetSourceRpc>(
        subcursor_ids.at(worker_id), source_address);
  }
}

bool BfsRpcClients::ExpandToRemoteVertex(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids,
    EdgeAccessor edge, VertexAccessor vertex) {
  CHECK(!vertex.is_local())
      << "ExpandToRemoteVertex should not be called with local vertex";
  int worker_id = vertex.address().worker_id();
  auto res = coordination_->GetClientPool(worker_id)->Call<ExpandToRemoteVertexRpc>(
      subcursor_ids.at(worker_id), edge.GlobalAddress(),
      vertex.GlobalAddress());
  return res.member;
}

PathSegment BfsRpcClients::ReconstructPath(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids,
    storage::VertexAddress vertex, database::GraphDbAccessor *dba) {
  int worker_id = vertex.worker_id();
  if (worker_id == db_->WorkerId()) {
    return subcursor_storage_->Get(subcursor_ids.at(worker_id))
        ->ReconstructPath(vertex);
  }

  auto res =
      coordination_->GetClientPool(worker_id)->CallWithLoad<ReconstructPathRpc>(
          [this, dba](const auto &reader) {
            ReconstructPathRes res;
            res.Load(reader, dba, this->data_manager_);
            return res;
          },
          subcursor_ids.at(worker_id), vertex);
  return PathSegment{res.edges, res.next_vertex, res.next_edge};
}

PathSegment BfsRpcClients::ReconstructPath(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids,
    storage::EdgeAddress edge, database::GraphDbAccessor *dba) {
  int worker_id = edge.worker_id();
  if (worker_id == db_->WorkerId()) {
    return subcursor_storage_->Get(subcursor_ids.at(worker_id))
        ->ReconstructPath(edge);
  }
  auto res =
      coordination_->GetClientPool(worker_id)->CallWithLoad<ReconstructPathRpc>(
          [this, dba](const auto &reader) {
            ReconstructPathRes res;
            res.Load(reader, dba, this->data_manager_);
            return res;
          },
          subcursor_ids.at(worker_id), edge);
  return PathSegment{res.edges, res.next_vertex, res.next_edge};
}

void BfsRpcClients::PrepareForExpand(
    const std::unordered_map<int16_t, int64_t> &subcursor_ids, bool clear) {
  auto futures = coordination_->ExecuteOnWorkers<void>(
      db_->WorkerId(), [clear, &subcursor_ids](int worker_id, auto &client) {
        client.template Call<PrepareForExpandRpc>(
            subcursor_ids.at(worker_id), clear);
      });
  subcursor_storage_->Get(subcursor_ids.at(db_->WorkerId()))
      ->PrepareForExpand(clear);
  // Wait and get all of the replies.
  for (auto &future : futures) {
    if (future.valid()) future.get();
  }
}

}  // namespace distributed
