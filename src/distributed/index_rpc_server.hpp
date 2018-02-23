#pragma once

#include "database/graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "distributed/index_rpc_messages.hpp"

using namespace database;

namespace distributed {

class IndexRpcServer {
 public:
  IndexRpcServer(database::GraphDb &db, communication::rpc::Server &server)
      : db_(db), rpc_server_(server) {
    rpc_server_.Register<BuildIndexRpc>([this](const BuildIndexReq &req) {

      LabelPropertyIndex::Key key{req.member.label, req.member.property};
      GraphDbAccessor dba(db_, req.member.tx_id);

      if (db_.storage().label_property_index_.CreateIndex(key) == false) {
        // If we are a distributed worker we just have to wait till the index
        // (which should be in progress of being created) is created so that our
        // return guarantess that the index has been built - this assumes that
        // no worker thread that is creating an index will fail
        while (!dba.LabelPropertyIndexExists(key.label_, key.property_)) {
          // TODO reconsider this constant, currently rule-of-thumb chosen
          std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
      } else {
        dba.PopulateIndex(key);
        dba.EnableIndex(key);
      }
      return std::make_unique<BuildIndexRes>();
    });
  }

 private:
  database::GraphDb &db_;
  communication::rpc::Server &rpc_server_;
};

}  // namespace distributed
