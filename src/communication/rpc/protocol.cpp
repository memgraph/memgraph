#include <sstream>

#include "capnp/message.h"
#include "capnp/serialize.h"
#include "fmt/format.h"

#include "communication/rpc/messages.capnp.h"
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
  auto data =
      ::kj::arrayPtr(input_stream_.data() + sizeof(request_len), request_len);
  // Our data is word aligned and padded to 64bit because we use regular
  // (non-packed) serialization of Cap'n Proto. So we can use reinterpret_cast.
  auto data_words =
      ::kj::arrayPtr(reinterpret_cast<::capnp::word *>(data.begin()),
                     reinterpret_cast<::capnp::word *>(data.end()));
  ::capnp::FlatArrayMessageReader request_message(data_words.asConst());
  auto request = request_message.getRoot<capnp::Message>();
  input_stream_.Shift(sizeof(MessageSize) + request_len);

  auto callbacks_accessor = server_.callbacks_.access();
  auto it = callbacks_accessor.find(request.getTypeId());
  if (it == callbacks_accessor.end()) {
    // Throw exception to close the socket and cleanup the session.
    throw SessionException(
        "Session trying to execute an unregistered RPC call!");
  }

  VLOG(12) << "[RpcServer] received " << it->second.req_type.name;

  ::capnp::MallocMessageBuilder response_message;
  // callback fills the message data
  auto response_builder = response_message.initRoot<capnp::Message>();
  it->second.callback(request, &response_builder);

  // Serialize and send response
  auto response_words = ::capnp::messageToFlatArray(response_message);
  auto response_bytes = response_words.asBytes();
  if (response_bytes.size() > std::numeric_limits<MessageSize>::max()) {
    throw SessionException(fmt::format(
        "Trying to send response of size {}, max response size is {}",
        response_bytes.size(), std::numeric_limits<MessageSize>::max()));
  }

  MessageSize input_stream_size = response_bytes.size();
  if (!output_stream_.Write(reinterpret_cast<uint8_t *>(&input_stream_size),
                            sizeof(MessageSize), true)) {
    throw SessionException("Couldn't send response size!");
  }
  if (!output_stream_.Write(response_bytes.begin(), response_bytes.size())) {
    throw SessionException("Couldn't send response data!");
  }

  VLOG(12) << "[RpcServer] sent " << it->second.res_type.name;
}

}  // namespace communication::rpc
