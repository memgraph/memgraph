#include "communication/rpc/server.hpp"

namespace communication::rpc {

Server::Server(const io::network::Endpoint &endpoint,
               size_t workers_count)
    : server_(endpoint, this, &context_, -1, "RPC", workers_count) {}

bool Server::Start() {
  return server_.Start();
}

void Server::Shutdown() {
  server_.Shutdown();
}

void Server::AwaitShutdown() {
  server_.AwaitShutdown();
}

const io::network::Endpoint &Server::endpoint() const {
  return server_.endpoint();
}
}  // namespace communication::rpc
