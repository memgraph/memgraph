#pragma once

namespace communication::rpc {
class Server;
}

namespace database {
class GraphDb;
}

namespace distributed {

class IndexRpcServer {
 public:
  IndexRpcServer(database::GraphDb &db, communication::rpc::Server &server);

 private:
  database::GraphDb &db_;
  communication::rpc::Server &rpc_server_;
};

}  // namespace distributed
