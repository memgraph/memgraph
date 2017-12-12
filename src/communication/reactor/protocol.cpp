#include <sstream>

#include "communication/reactor/protocol.hpp"
#include "communication/reactor/reactor_distributed.hpp"
#include "communication/reactor/reactor_local.hpp"

#include "glog/logging.h"

namespace communication::reactor {

Session::Session(Socket &&socket, SessionData &data)
    : socket_(std::move(socket)), system_(data.system) {
  event_.data.ptr = this;
}

bool Session::Alive() const { return alive_; }

std::string Session::GetStringAndShift(SizeT len) {
  std::string ret(reinterpret_cast<char *>(buffer_.data()), len);
  buffer_.Shift(len);
  return ret;
}

void Session::Execute() {
  if (!handshake_done_) {
    // Note: this function can be multiple times before the buffer has the full
    // packet.
    //   We currently have to check for this case and return without shifting
    //   the buffer.
    //   In other words, only shift anything from the buffer if you can read the
    //   entire (sub)message.

    if (buffer_.size() < 2 * sizeof(SizeT)) return;
    SizeT len_reactor = GetLength();
    SizeT len_channel = GetLength(sizeof(SizeT));

    if (buffer_.size() < 2 * sizeof(SizeT) + len_reactor + len_channel) return;

    // remove the length bytes from the buffer
    buffer_.Shift(2 * sizeof(SizeT));

    reactor_ = GetStringAndShift(len_reactor);
    channel_ = GetStringAndShift(len_channel);

    DLOG(INFO) << "Reactor: " << reactor_ << "; Channel: " << channel_
               << std::endl;

    LocalChannelWriter channel(reactor_, channel_, system_);
    SendSuccess(true);

    handshake_done_ = true;
  }

  if (buffer_.size() < sizeof(SizeT)) return;
  SizeT len_data = GetLength();
  if (buffer_.size() < sizeof(SizeT) + len_data) return;

  // remove the length bytes from the buffer
  buffer_.Shift(sizeof(SizeT));

  // TODO: check for exceptions
  std::istringstream stream;
  stream.str(std::string(reinterpret_cast<char *>(buffer_.data()), len_data));
  ::cereal::BinaryInputArchive iarchive{stream};
  std::unique_ptr<Message> message{nullptr};
  iarchive(message);
  buffer_.Shift(len_data);

  LocalChannelWriter channel(reactor_, channel_, system_);
  channel.Send(std::move(message));
}

StreamBuffer Session::Allocate() { return buffer_.Allocate(); }

void Session::Written(size_t len) { buffer_.Written(len); }

void Session::Close() {
  DLOG(INFO) << "Closing session";
  this->socket_.Close();
}

SizeT Session::GetLength(int offset) {
  SizeT ret = *reinterpret_cast<SizeT *>(buffer_.data() + offset);
  return ret;
}

bool Session::SendSuccess(bool success) {
  if (success) {
    return socket_.Write("\x80");
  }
  return socket_.Write("\x40");
}

bool SendLength(Socket &socket, SizeT length) {
  return socket.Write(reinterpret_cast<uint8_t *>(&length), sizeof(SizeT));
}

void SendMessage(std::string address, uint16_t port, std::string reactor,
                 std::string channel, std::unique_ptr<Message> message) {
  // Initialize endpoint.
  Endpoint endpoint(address.c_str(), port);

  // Initialize socket.
  Socket socket;
  if (!socket.Connect(endpoint)) {
    LOG(INFO) << "Couldn't connect to remote address: " << address << ":"
              << port;
    return;
  }

  // Send data
  if (!SendLength(socket, reactor.size())) {
    LOG(INFO) << "Couldn't send reactor size!";
    return;
  }
  if (!SendLength(socket, channel.size())) {
    LOG(INFO) << "Couldn't send channel size!";
    return;
  }
  if (!socket.Write(reactor)) {
    LOG(INFO) << "Couldn't send reactor data!";
    return;
  }
  if (!socket.Write(channel)) {
    LOG(INFO) << "Couldn't send channel data!";
    return;
  }

  if (message == nullptr) return;

  // Serialize and send message
  std::ostringstream stream;
  ::cereal::BinaryOutputArchive oarchive(stream);
  oarchive(message);

  const std::string &buffer = stream.str();
  if (!SendLength(socket, buffer.size())) {
    LOG(INFO) << "Couldn't send message size!";
    return;
  }
  if (!socket.Write(buffer)) {
    LOG(INFO) << "Couldn't send message data!";
    return;
  }
}
}
