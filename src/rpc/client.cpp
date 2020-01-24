#include "rpc/client.hpp"

namespace rpc {

Client::Client(const io::network::Endpoint &endpoint,
               communication::ClientContext *context)
    : endpoint_(endpoint), context_(context) {}

void Client::Abort() {
  if (!client_) return;
  // We need to call Shutdown on the client to abort any pending read or
  // write operations.
  client_->Shutdown();
  client_ = std::nullopt;
}

}  // namespace rpc
