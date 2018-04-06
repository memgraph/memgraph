#include "distributed/durability_rpc_server.hpp"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/durability_rpc_messages.hpp"

namespace distributed {

DurabilityRpcServer::DurabilityRpcServer(database::GraphDb &db,
                                         communication::rpc::Server &server)
    : db_(db), rpc_server_(server) {
  rpc_server_.Register<MakeSnapshotRpc>([this](const MakeSnapshotReq &req) {
    database::GraphDbAccessor dba(this->db_, req.member);
    return std::make_unique<MakeSnapshotRes>(this->db_.MakeSnapshot(dba));
  });
}

}  // namespace distributed
