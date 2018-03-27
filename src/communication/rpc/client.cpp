#include <chrono>
#include <thread>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "boost/serialization/unique_ptr.hpp"
#include "gflags/gflags.h"

#include "communication/rpc/client.hpp"

DEFINE_HIDDEN_bool(rpc_random_latency, false,
                   "If a random wait should happen on each RPC call, to "
                   "simulate network latency.");

namespace communication::rpc {

Client::Client(const io::network::Endpoint &endpoint) : endpoint_(endpoint) {}

std::unique_ptr<Message> Client::Call(const Message &request) {
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
    client_.emplace();
    if (!client_->Connect(endpoint_)) {
      LOG(ERROR) << "Couldn't connect to remote address " << endpoint_;
      client_ = std::experimental::nullopt;
      return nullptr;
    }
  }

  // Serialize and send request.
  std::stringstream request_stream(std::ios_base::out | std::ios_base::binary);
  {
    boost::archive::binary_oarchive request_archive(request_stream);
    // Serialize reference as pointer (to serialize the derived class). The
    // request is read in protocol.cpp.
    request_archive << &request;
    // Archive destructor ensures everything is written.
  }

  const std::string &request_buffer = request_stream.str();
  CHECK(request_buffer.size() <= std::numeric_limits<MessageSize>::max())
      << fmt::format(
             "Trying to send message of size {}, max message size is {}",
             request_buffer.size(), std::numeric_limits<MessageSize>::max());

  MessageSize request_data_size = request_buffer.size();
  if (!client_->Write(reinterpret_cast<uint8_t *>(&request_data_size),
                      sizeof(MessageSize), true)) {
    LOG(ERROR) << "Couldn't send request size to " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return nullptr;
  }

  if (!client_->Write(request_buffer)) {
    LOG(ERROR) << "Couldn't send request data to " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return nullptr;
  }

  // Receive response data size.
  if (!client_->Read(sizeof(MessageSize))) {
    LOG(ERROR) << "Couldn't get response from " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return nullptr;
  }
  MessageSize response_data_size =
      *reinterpret_cast<MessageSize *>(client_->GetData());
  client_->ShiftData(sizeof(MessageSize));

  // Receive response data.
  if (!client_->Read(response_data_size)) {
    LOG(ERROR) << "Couldn't get response from " << client_->endpoint();
    client_ = std::experimental::nullopt;
    return nullptr;
  }

  std::unique_ptr<Message> response;
  {
    std::stringstream response_stream(std::ios_base::in |
                                      std::ios_base::binary);
    response_stream.str(std::string(reinterpret_cast<char *>(client_->GetData()),
                                    response_data_size));
    boost::archive::binary_iarchive response_archive(response_stream);
    response_archive >> response;
  }

  client_->ShiftData(response_data_size);

  return response;
}

void Client::Abort() {
  if (!client_) return;
  // We need to call Shutdown on the client to abort any pending read or
  // write operations.
  client_->Shutdown();
  client_ = std::experimental::nullopt;
}

}  // namespace communication::rpc
