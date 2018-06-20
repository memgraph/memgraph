#include <chrono>
#include <thread>

#include "gflags/gflags.h"

#include "communication/rpc/client.hpp"

DEFINE_HIDDEN_bool(rpc_random_latency, false,
                   "If a random wait should happen on each RPC call, to "
                   "simulate network latency.");

namespace communication::rpc {

Client::Client(const io::network::Endpoint &endpoint) : endpoint_(endpoint) {}

std::experimental::optional<::capnp::FlatArrayMessageReader> Client::Send(
    ::capnp::MessageBuilder *message) {
  std::lock_guard<std::mutex> guard(mutex_);

  if (FLAGS_rpc_random_latency) {
    auto microseconds = (int)(1000 * rand_(gen_));
    std::this_thread::sleep_for(std::chrono::microseconds(microseconds));
  }

  // Check if the connection is broken (if we haven't used the client for a
  // long time the server could have died).
  if (client_ && client_->ErrorStatus()) {
    client_ = std::experimental::nullopt;
  }

  // Connect to the remote server.
  if (!client_) {
    client_.emplace(&context_);
    if (!client_->Connect(endpoint_)) {
      LOG(ERROR) << "Couldn't connect to remote address " << endpoint_;
      client_ = std::experimental::nullopt;
      return std::experimental::nullopt;
    }
  }

  // Serialize and send request.
  auto request_words = ::capnp::messageToFlatArray(*message);
  auto request_bytes = request_words.asBytes();
  CHECK(request_bytes.size() <= std::numeric_limits<MessageSize>::max())
      << fmt::format(
             "Trying to send message of size {}, max message size is {}",
             request_bytes.size(), std::numeric_limits<MessageSize>::max());

  MessageSize request_data_size = request_bytes.size();
  if (!client_->Write(reinterpret_cast<uint8_t *>(&request_data_size),
                      sizeof(MessageSize), true)) {
    LOG(ERROR) << "Couldn't send request size to " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return std::experimental::nullopt;
  }

  if (!client_->Write(request_bytes.begin(), request_bytes.size())) {
    LOG(ERROR) << "Couldn't send request data to " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return std::experimental::nullopt;
  }

  // Receive response data size.
  if (!client_->Read(sizeof(MessageSize))) {
    LOG(ERROR) << "Couldn't get response from " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return std::experimental::nullopt;
  }
  MessageSize response_data_size =
      *reinterpret_cast<MessageSize *>(client_->GetData());
  client_->ShiftData(sizeof(MessageSize));

  // Receive response data.
  if (!client_->Read(response_data_size)) {
    LOG(ERROR) << "Couldn't get response from " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return std::experimental::nullopt;
  }

  // Read the response message.
  auto data = ::kj::arrayPtr(client_->GetData(), response_data_size);
  // Our data is word aligned and padded to 64bit because we use regular
  // (non-packed) serialization of Cap'n Proto. So we can use reinterpret_cast.
  auto data_words =
      ::kj::arrayPtr(reinterpret_cast<::capnp::word *>(data.begin()),
                     reinterpret_cast<::capnp::word *>(data.end()));
  ::capnp::FlatArrayMessageReader response_message(data_words.asConst());
  client_->ShiftData(response_data_size);
  return std::experimental::make_optional(std::move(response_message));
}

void Client::Abort() {
  if (!client_) return;
  // We need to call Shutdown on the client to abort any pending read or
  // write operations.
  client_->Shutdown();
  client_ = std::experimental::nullopt;
}

}  // namespace communication::rpc
