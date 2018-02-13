#include <sstream>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/unique_ptr.hpp"
#include "fmt/format.h"
#include "glog/logging.h"

#include "communication/rpc/messages-inl.hpp"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/protocol.hpp"
#include "communication/rpc/server.hpp"

namespace communication::rpc {

Session::Session(Socket &&socket, System &system)
    : socket_(std::make_shared<Socket>(std::move(socket))), system_(system) {}

bool Session::Alive() const { return alive_; }

void Session::Execute() {
  if (!handshake_done_) {
    if (buffer_.size() < sizeof(MessageSize)) return;
    MessageSize service_len = *reinterpret_cast<MessageSize *>(buffer_.data());
    if (buffer_.size() < sizeof(MessageSize) + service_len) return;
    service_name_ = std::string(
        reinterpret_cast<char *>(buffer_.data() + sizeof(MessageSize)),
        service_len);
    buffer_.Shift(sizeof(MessageSize) + service_len);
    handshake_done_ = true;
  }

  if (buffer_.size() < sizeof(uint32_t) + sizeof(MessageSize)) return;
  uint32_t message_id = *reinterpret_cast<uint32_t *>(buffer_.data());
  MessageSize message_len =
      *reinterpret_cast<MessageSize *>(buffer_.data() + sizeof(uint32_t));
  if (buffer_.size() < sizeof(uint32_t) + sizeof(MessageSize) + message_len)
    return;

  // TODO (mferencevic): check for exceptions
  std::unique_ptr<Message> message;
  {
    std::stringstream stream(std::ios_base::in | std::ios_base::binary);
    stream.str(
        std::string(reinterpret_cast<char *>(buffer_.data() + sizeof(uint32_t) +
                                             sizeof(MessageSize)),
                    message_len));
    boost::archive::binary_iarchive archive(stream);
    archive >> message;
  }
  buffer_.Shift(sizeof(uint32_t) + sizeof(MessageSize) + message_len);

  system_.AddTask(socket_, service_name_, message_id, std::move(message));
}

StreamBuffer Session::Allocate() { return buffer_.Allocate(); }

void Session::Written(size_t len) { buffer_.Written(len); }

void Session::Close() {
  DLOG(INFO) << "Closing session";
  // We explicitly close the socket here to remove the socket from the epoll
  // event loop. The response message send will fail but that is OK and
  // intended because the remote side closed the connection.
  socket_.get()->Close();
}

void SendMessage(Socket &socket, uint32_t message_id,
                 std::unique_ptr<Message> &message) {
  CHECK(message) << "Trying to send nullptr instead of message";

  // Serialize and send message
  std::stringstream stream(std::ios_base::out | std::ios_base::binary);
  {
    boost::archive::binary_oarchive archive(stream);
    archive << message;
    // Archive destructor ensures everything is written.
  }

  const std::string &buffer = stream.str();
  uint64_t message_size = sizeof(MessageSize) + buffer.size();
  CHECK(message_size <= kMaxMessageSize) << fmt::format(
      "Trying to send message of size {}, max message size is {}", message_size,
      kMaxMessageSize);

  if (!socket.Write(reinterpret_cast<uint8_t *>(&message_id), sizeof(uint32_t),
                    true)) {
    LOG(WARNING) << "Couldn't send message id!";
    return;
  }

  MessageSize buffer_size = buffer.size();
  if (!socket.Write(reinterpret_cast<uint8_t *>(&buffer_size),
                    sizeof(MessageSize), true)) {
    LOG(WARNING) << "Couldn't send message size!";
    return;
  }
  if (!socket.Write(buffer)) {
    LOG(WARNING) << "Couldn't send message data!";
    return;
  }
}
}  // namespace communication::rpc
