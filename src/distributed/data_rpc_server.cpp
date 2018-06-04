#include <memory>

#include "data_rpc_server.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/data_rpc_messages.hpp"

namespace distributed {

DataRpcServer::DataRpcServer(database::GraphDb &db,
                             communication::rpc::Server &server)
    : db_(db), rpc_server_(server) {
  rpc_server_.Register<VertexRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        database::GraphDbAccessor dba(db_, req_reader.getMember().getTxId());
        auto vertex = dba.FindVertex(req_reader.getMember().getGid(), false);
        CHECK(vertex.GetOld())
            << "Old record must exist when sending vertex by RPC";
        VertexRes response(vertex.GetOld(), db_.WorkerId());
        response.Save(res_builder);
      });

  rpc_server_.Register<EdgeRpc>([this](const auto &req_reader,
                                       auto *res_builder) {
    database::GraphDbAccessor dba(db_, req_reader.getMember().getTxId());
    auto edge = dba.FindEdge(req_reader.getMember().getGid(), false);
    CHECK(edge.GetOld()) << "Old record must exist when sending edge by RPC";
    EdgeRes response(edge.GetOld(), db_.WorkerId());
    response.Save(res_builder);
  });

  rpc_server_.Register<VertexCountRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        VertexCountReq req;
        req.Load(req_reader);
        database::GraphDbAccessor dba(db_, req.member);
        int64_t size = 0;
        for (auto vertex : dba.Vertices(false)) ++size;
        VertexCountRes res(size);
        res.Save(res_builder);
      });
}

}  // namespace distributed
