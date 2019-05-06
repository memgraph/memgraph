#include "communication/rpc/client.hpp"

namespace communication::rpc {

Client::Client(const io::network::Endpoint &endpoint) : endpoint_(endpoint) {}

void Client::Abort() {
  if (!client_) return;
  // We need to call Shutdown on the client to abort any pending read or
  // write operations.
  client_->Shutdown();
  client_ = std::nullopt;
}

}  // namespace communication::rpc
