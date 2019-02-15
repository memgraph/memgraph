#include "distributed/data_rpc_server.hpp"

#include <memory>

#include "database/distributed/graph_db.hpp"
#include "database/distributed/graph_db_accessor.hpp"
#include "distributed/data_rpc_messages.hpp"

namespace distributed {

DataRpcServer::DataRpcServer(database::GraphDb *db,
                             distributed::Coordination *coordination)
    : db_(db) {
  coordination->Register<VertexRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        auto dba = db_->Access(req_reader.getMember().getTxId());
        auto vertex = dba->FindVertexRaw(req_reader.getMember().getGid());
        VertexRes response(vertex.CypherId(), vertex.GetOld(),
                           vertex.GetNew(), db_->WorkerId());
        Save(response, res_builder);
      });

  coordination->Register<EdgeRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        auto dba = db_->Access(req_reader.getMember().getTxId());
        auto edge = dba->FindEdgeRaw(req_reader.getMember().getGid());
        EdgeRes response(edge.CypherId(), edge.GetOld(),
                         edge.GetNew(), db_->WorkerId());
        Save(response, res_builder);
      });

  coordination->Register<VertexCountRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        VertexCountReq req;
        Load(&req, req_reader);
        auto dba = db_->Access(req.member);
        int64_t size = 0;
        for (auto vertex : dba->Vertices(false)) ++size;
        VertexCountRes res(size);
        Save(res, res_builder);
      });
}

}  // namespace distributed
