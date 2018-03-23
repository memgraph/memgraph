#include <memory>

#include "data_rpc_server.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/data_rpc_messages.hpp"

namespace distributed {

DataRpcServer::DataRpcServer(database::GraphDb &db,
                             communication::rpc::Server &server)
    : db_(db), rpc_server_(server) {
  rpc_server_.Register<VertexRpc>(
      [this](const VertexReq &req) {
        database::GraphDbAccessor dba(db_, req.member.tx_id);
        auto vertex = dba.FindVertex(req.member.gid, false);
        CHECK(vertex.GetOld())
            << "Old record must exist when sending vertex by RPC";
        return std::make_unique<VertexRes>(vertex.GetOld(), db_.WorkerId());
      });

  rpc_server_.Register<EdgeRpc>([this](const EdgeReq &req) {
    database::GraphDbAccessor dba(db_, req.member.tx_id);
    auto edge = dba.FindEdge(req.member.gid, false);
    CHECK(edge.GetOld()) << "Old record must exist when sending edge by RPC";
    return std::make_unique<EdgeRes>(edge.GetOld(), db_.WorkerId());
  });
}

}  // namespace distributed
