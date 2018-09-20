#include "distributed/index_rpc_server.hpp"

#include "communication/rpc/server.hpp"
#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/index_rpc_messages.hpp"

namespace distributed {

IndexRpcServer::IndexRpcServer(database::GraphDb &db,
                               communication::rpc::Server &server)
    : db_(db), rpc_server_(server) {
  rpc_server_.Register<CreateIndexRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        CreateIndexReq req;
        Load(&req, req_reader);
        database::LabelPropertyIndex::Key key{req.label, req.property};
        db_.storage().label_property_index_.CreateIndex(key);
      });

  rpc_server_.Register<PopulateIndexRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        PopulateIndexReq req;
        Load(&req, req_reader);
        database::LabelPropertyIndex::Key key{req.label, req.property};
        auto dba = db_.Access(req.tx_id);
        dba->PopulateIndex(key);
        dba->EnableIndex(key);
      });
}

}  // namespace distributed
