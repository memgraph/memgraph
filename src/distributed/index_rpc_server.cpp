#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/index_rpc_server.hpp"

namespace distributed {

IndexRpcServer::IndexRpcServer(database::GraphDb &db,
                               communication::rpc::Server &server)
    : db_(db), rpc_server_(server) {
  rpc_server_.Register<BuildIndexRpc>(
      [this](const auto &req_reader, auto *res_builder) {
        BuildIndexReq req;
        req.Load(req_reader);
        database::LabelPropertyIndex::Key key{req.label, req.property};
        database::GraphDbAccessor dba(db_, req.tx_id);

        if (db_.storage().label_property_index_.CreateIndex(key) == false) {
          // If we are a distributed worker we just have to wait till the index
          // (which should be in progress of being created) is created so that
          // our return guarantess that the index has been built - this assumes
          // that no worker thread that is creating an index will fail
          while (!dba.LabelPropertyIndexExists(key.label_, key.property_)) {
            // TODO reconsider this constant, currently rule-of-thumb chosen
            std::this_thread::sleep_for(std::chrono::microseconds(100));
          }
        } else {
          dba.PopulateIndex(key);
          dba.EnableIndex(key);
        }
      });
}

}  // namespace distributed
