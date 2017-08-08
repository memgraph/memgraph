#include <sstream>

#include "protocol.hpp"
#include "communication.hpp"

#include "glog/logging.h"

namespace Protocol {

Session::Session(Socket &&socket, Data &data)
    : socket_(std::move(socket)), system_(data.system) {
  event_.data.ptr = this;
}

bool Session::Alive() const { return alive_; }

std::string Session::GetString(SizeT len) {
  std::string ret(reinterpret_cast<char *>(buffer_.data()), len);
  buffer_.Shift(len);
  return ret;
}

void Session::Execute() {
  if (!handshake_done_) {
    SizeT len_reactor = GetLength();
    SizeT len_channel = GetLength(2);

    if (len_reactor == 0 || len_channel == 0) return;
    if (buffer_.size() < len_reactor + len_channel) return;

    // remove the length bytes from the buffer
    buffer_.Shift(2 * sizeof(SizeT));

    reactor_ = GetString(len_reactor);
    channel_ = GetString(len_channel);

    std::cout << "Reactor: " << reactor_ << "; Channel: " << channel_
              << std::endl;

    auto channel = system_->FindChannel(reactor_, channel_);
    SendSuccess(channel != nullptr);

    handshake_done_ = true;
  }

  SizeT len_data = GetLength();
  if (len_data == 0) return;
  if (buffer_.size() < len_data) return;

  // remove the length bytes from the buffer
  buffer_.Shift(sizeof(SizeT));

  // TODO: check for exceptions
  std::istringstream stream;
  stream.str(std::string(reinterpret_cast<char*>(buffer_.data()), len_data));
  cereal::BinaryInputArchive iarchive{stream};
  std::unique_ptr<Message> message{nullptr};
  iarchive(message);
  buffer_.Shift(len_data);

  auto channel = system_->FindChannel(reactor_, channel_);
  if (channel == nullptr) {
    SendSuccess(false);
    return;
  }

  channel->SendHelper(typeid(nullptr), std::move(message));

  SendSuccess(true);
}

StreamBuffer Session::Allocate() { return buffer_.Allocate(); }

void Session::Written(size_t len) { buffer_.Written(len); }

void Session::Close() {
  DLOG(INFO) << "Closing session";
  this->socket_.Close();
}

SizeT Session::GetLength(int offset) {
  if (buffer_.size() - offset < sizeof(SizeT)) return 0;
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

bool GetSuccess(Socket &socket) {
  uint8_t val;
  if (socket.Read(&val, 1) != 1) {
    return false;
  }
  return val == 0x80;
}

bool SendMessage(std::string address, uint16_t port, std::string reactor,
                 std::string channel, std::unique_ptr<Message> message) {
  // Initialize endpoint.
  Endpoint endpoint;
  try {
    endpoint = Endpoint(address.c_str(), port);
  } catch (io::network::NetworkEndpointException &e) {
    LOG(INFO) << "Address is invalid!";
    return false;
  }

  // Initialize socket.
  Socket socket;
  if (!socket.Connect(endpoint)) {
    LOG(INFO) << "Couldn't connect to remote address: " << address << ":"
              << port;
    return false;
  }

  // Send data
  if (!SendLength(socket, reactor.size())) {
    LOG(INFO) << "Couldn't send reactor size!";
    return false;
  }
  if (!SendLength(socket, channel.size())) {
    LOG(INFO) << "Couldn't send channel size!";
    return false;
  }
  if (!socket.Write(reactor)) {
    LOG(INFO) << "Couldn't send reactor data!";
    return false;
  }
  if (!socket.Write(channel)) {
    LOG(INFO) << "Couldn't send channel data!";
    return false;
  }

  bool success = GetSuccess(socket);
  if (message == nullptr or !success) {
    return success;
  }

  // Serialize and send message
  std::ostringstream stream;
  cereal::BinaryOutputArchive oarchive(stream);
  oarchive(message);

  const std::string &buffer = stream.str();
  if (!SendLength(socket, buffer.size())) {
    LOG(INFO) << "Couldn't send message size!";
    return false;
  }
  if (!socket.Write(buffer.data(), buffer.size())) {
    LOG(INFO) << "Couldn't send message data!";
    return false;
  }

  return GetSuccess(socket);
}
}
