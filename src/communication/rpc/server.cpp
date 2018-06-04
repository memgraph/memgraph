#include "communication/rpc/server.hpp"

namespace communication::rpc {

Server::Server(const io::network::Endpoint &endpoint,
               size_t workers_count)
    : server_(endpoint, *this, -1, "RPC", workers_count) {}

void Server::StopProcessingCalls() {
  server_.Shutdown();
  server_.AwaitShutdown();
}

const io::network::Endpoint &Server::endpoint() const {
  return server_.endpoint();
}
}  // namespace communication::rpc
