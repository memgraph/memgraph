#include <sstream>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/unique_ptr.hpp"
#include "fmt/format.h"

#include "communication/rpc/messages-inl.hpp"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/protocol.hpp"
#include "communication/rpc/server.hpp"
#include "utils/demangle.hpp"

namespace communication::rpc {

Session::Session(Socket &&socket, Server &server)
    : socket_(std::move(socket)), server_(server) {}

void Session::Execute() {
  if (buffer_.size() < sizeof(MessageSize)) return;
  MessageSize request_len = *reinterpret_cast<MessageSize *>(buffer_.data());
  uint64_t request_size = sizeof(MessageSize) + request_len;
  buffer_.Resize(request_size);
  if (buffer_.size() < request_size) return;

  // Read the request message.
  std::unique_ptr<Message> request([this, request_len]() {
    Message *req_ptr = nullptr;
    std::stringstream stream(std::ios_base::in | std::ios_base::binary);
    stream.str(std::string(
        reinterpret_cast<char *>(buffer_.data() + sizeof(MessageSize)),
        request_len));
    boost::archive::binary_iarchive archive(stream);
    // Sent from client.cpp
    archive >> req_ptr;
    return req_ptr;
  }());
  buffer_.Shift(sizeof(MessageSize) + request_len);

  auto callbacks_accessor = server_.callbacks_.access();
  auto it = callbacks_accessor.find(request->type_index());
  if (it == callbacks_accessor.end()) {
    // Throw exception to close the socket and cleanup the session.
    throw SessionException(
        "Session trying to execute an unregistered RPC call!");
  }

  if (VLOG_IS_ON(12)) {
    auto req_type = utils::Demangle(request->type_index().name());
    LOG(INFO) << "[RpcServer] received " << (req_type ? req_type.value() : "");
  }

  std::unique_ptr<Message> response = it->second(*(request.get()));

  if (!response) {
    throw SessionException("Trying to send nullptr instead of message");
  }

  // Serialize and send response
  std::stringstream stream(std::ios_base::out | std::ios_base::binary);
  {
    boost::archive::binary_oarchive archive(stream);
    archive << response;
    // Archive destructor ensures everything is written.
  }

  const std::string &buffer = stream.str();
  if (buffer.size() > std::numeric_limits<MessageSize>::max()) {
    throw SessionException(fmt::format(
        "Trying to send response of size {}, max response size is {}",
        buffer.size(), std::numeric_limits<MessageSize>::max()));
  }

  MessageSize buffer_size = buffer.size();
  if (!socket_.Write(reinterpret_cast<uint8_t *>(&buffer_size),
                     sizeof(MessageSize), true)) {
    throw SessionException("Couldn't send response size!");
  }
  if (!socket_.Write(buffer)) {
    throw SessionException("Couldn't send response data!");
  }

  if (VLOG_IS_ON(12)) {
    auto res_type = utils::Demangle(response->type_index().name());
    LOG(INFO) << "[RpcServer] sent " << (res_type ? res_type.value() : "");
  }
}

StreamBuffer Session::Allocate() { return buffer_.Allocate(); }

void Session::Written(size_t len) { buffer_.Written(len); }
}  // namespace communication::rpc
