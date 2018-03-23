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

Session::Session(Server &server, communication::InputStream &input_stream,
                 communication::OutputStream &output_stream)
    : server_(server),
      input_stream_(input_stream),
      output_stream_(output_stream) {}

void Session::Execute() {
  if (input_stream_.size() < sizeof(MessageSize)) return;
  MessageSize request_len =
      *reinterpret_cast<MessageSize *>(input_stream_.data());
  uint64_t request_size = sizeof(MessageSize) + request_len;
  input_stream_.Resize(request_size);
  if (input_stream_.size() < request_size) return;

  // Read the request message.
  std::unique_ptr<Message> request([this, request_len]() {
    Message *req_ptr = nullptr;
    std::stringstream stream(std::ios_base::in | std::ios_base::binary);
    stream.str(std::string(
        reinterpret_cast<char *>(input_stream_.data() + sizeof(MessageSize)),
        request_len));
    boost::archive::binary_iarchive archive(stream);
    // Sent from client.cpp
    archive >> req_ptr;
    return req_ptr;
  }());
  input_stream_.Shift(sizeof(MessageSize) + request_len);

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

  MessageSize input_stream_size = buffer.size();
  if (!output_stream_.Write(reinterpret_cast<uint8_t *>(&input_stream_size),
                            sizeof(MessageSize), true)) {
    throw SessionException("Couldn't send response size!");
  }
  if (!output_stream_.Write(buffer)) {
    throw SessionException("Couldn't send response data!");
  }

  if (VLOG_IS_ON(12)) {
    auto res_type = utils::Demangle(response->type_index().name());
    LOG(INFO) << "[RpcServer] sent " << (res_type ? res_type.value() : "");
  }
}
}  // namespace communication::rpc
