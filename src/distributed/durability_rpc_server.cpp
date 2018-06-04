#include "distributed/durability_rpc_server.hpp"

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/durability_rpc_messages.hpp"

namespace distributed {

DurabilityRpcServer::DurabilityRpcServer(database::GraphDb &db,
                                         communication::rpc::Server &server)
    : db_(db), rpc_server_(server) {
  rpc_server_.Register<MakeSnapshotRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        database::GraphDbAccessor dba(this->db_, req_reader.getMember());
        MakeSnapshotRes res(this->db_.MakeSnapshot(dba));
        res.Save(res_builder);
      });
}

}  // namespace distributed
