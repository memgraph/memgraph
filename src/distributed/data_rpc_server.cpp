#include "distributed/data_rpc_server.hpp"

#include <memory>

#include "database/distributed/graph_db.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/updates_rpc_server.hpp"
#include "distributed/data_rpc_messages.hpp"

namespace distributed {

DataRpcServer::DataRpcServer(database::GraphDb *db,
                             distributed::Coordination *coordination)
    : db_(db) {
  coordination->Register<VertexRpc>(
      [this](auto *req_reader, auto *res_builder) {
        VertexReq req;
        slk::Load(&req, req_reader);
        auto dba = db_->Access(req.member.tx_id);
        auto vertex = dba->FindVertexRaw(req.member.gid);

        auto *old = vertex.GetOld();
        auto *newr = vertex.GetNew() ? vertex.GetNew()->CloneData() : nullptr;
        db_->updates_server().ApplyDeltasToRecord(
            dba->transaction().id_, req.member.gid,
            req.member.from_worker_id, &old, &newr);

        VertexRes response(vertex.CypherId(), old, newr, db_->WorkerId());
        slk::Save(response, res_builder);
        delete newr;
      });

  coordination->Register<EdgeRpc>(
      [this](auto *req_reader, auto *res_builder) {
        EdgeReq req;
        slk::Load(&req, req_reader);
        auto dba = db_->Access(req.member.tx_id);
        auto edge = dba->FindEdgeRaw(req.member.gid);

        auto *old = edge.GetOld();
        auto *newr = edge.GetNew() ? edge.GetNew()->CloneData() : nullptr;
        db_->updates_server().ApplyDeltasToRecord(
            dba->transaction().id_, req.member.gid,
            req.member.from_worker_id, &old, &newr);

        EdgeRes response(edge.CypherId(), old, newr, db_->WorkerId());
        slk::Save(response, res_builder);
        delete newr;
      });

  coordination->Register<VertexCountRpc>(
      [this](auto *req_reader, auto *res_builder) {
        VertexCountReq req;
        slk::Load(&req, req_reader);
        auto dba = db_->Access(req.member);
        int64_t size = 0;
        for (auto vertex : dba->Vertices(false)) ++size;
        VertexCountRes res(size);
        slk::Save(res, res_builder);
      });
}

}  // namespace distributed
