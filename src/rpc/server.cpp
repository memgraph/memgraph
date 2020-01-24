#include "rpc/server.hpp"

namespace rpc {

Server::Server(const io::network::Endpoint &endpoint,
               communication::ServerContext *context, size_t workers_count)
    : server_(endpoint, this, context, -1, context->use_ssl() ? "RPCS" : "RPC",
              workers_count) {}

bool Server::Start() { return server_.Start(); }

void Server::Shutdown() { server_.Shutdown(); }

void Server::AwaitShutdown() { server_.AwaitShutdown(); }

const io::network::Endpoint &Server::endpoint() const {
  return server_.endpoint();
}
}  // namespace rpc
