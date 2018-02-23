#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "boost/serialization/unique_ptr.hpp"

#include "communication/rpc/server.hpp"

namespace communication::rpc {

Server::Server(const io::network::Endpoint &endpoint,
               size_t workers_count)
    : server_(endpoint, *this, false, workers_count) {}

void Server::StopProcessingCalls() {
  server_.Shutdown();
  server_.AwaitShutdown();
}

const io::network::Endpoint &Server::endpoint() const {
  return server_.endpoint();
}
}  // namespace communication::rpc
