#include <memory>

#include "database/graph_db_accessor.hpp"
#include "distributed/remote_data_rpc_messages.hpp"
#include "remote_data_rpc_server.hpp"

namespace distributed {

RemoteDataRpcServer::RemoteDataRpcServer(database::GraphDb &db,
                                         communication::rpc::Server &server)
    : db_(db), rpc_server_(server) {
  rpc_server_.Register<RemoteVertexRpc>([this](const RemoteVertexReq &req) {
    database::GraphDbAccessor dba(db_, req.member.tx_id);
    auto vertex = dba.FindVertexChecked(req.member.gid, false);
    CHECK(vertex.GetOld())
        << "Old record must exist when sending vertex by RPC";
    return std::make_unique<RemoteVertexRes>(vertex.GetOld(), db_.WorkerId());
  });

  rpc_server_.Register<RemoteEdgeRpc>([this](const RemoteEdgeReq &req) {
    database::GraphDbAccessor dba(db_, req.member.tx_id);
    auto edge = dba.FindEdgeChecked(req.member.gid, false);
    CHECK(edge.GetOld()) << "Old record must exist when sending edge by RPC";
    return std::make_unique<RemoteEdgeRes>(edge.GetOld(), db_.WorkerId());
  });
}

}  // namespace distributed
