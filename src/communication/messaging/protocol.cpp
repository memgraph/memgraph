#include <sstream>
#include <unordered_map>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/unique_ptr.hpp"
#include "fmt/format.h"
#include "glog/logging.h"

#include "communication/messaging/distributed.hpp"
#include "communication/messaging/local.hpp"
#include "communication/messaging/protocol.hpp"
#include "communication/rpc/messages-inl.hpp"

namespace communication::messaging {

Session::Session(Socket &&socket, SessionData &data)
    : socket_(std::move(socket)), system_(data.system) {}

bool Session::Alive() const { return alive_; }

std::string Session::GetStringAndShift(SizeT len) {
  std::string ret(reinterpret_cast<char *>(buffer_.data()), len);
  buffer_.Shift(len);
  return ret;
}

void Session::Execute() {
  if (buffer_.size() < sizeof(SizeT)) return;
  SizeT len_channel = GetLength();
  if (buffer_.size() < 2 * sizeof(SizeT) + len_channel) return;
  SizeT len_data = GetLength(sizeof(SizeT) + len_channel);
  if (buffer_.size() < 2 * sizeof(SizeT) + len_data + len_channel) return;

  // Remove the length bytes from the buffer.
  buffer_.Shift(sizeof(SizeT));
  auto channel = GetStringAndShift(len_channel);
  buffer_.Shift(sizeof(SizeT));

  // TODO: check for exceptions
  std::stringstream stream;
  stream.str(std::string(reinterpret_cast<char *>(buffer_.data()), len_data));
  boost::archive::binary_iarchive archive(stream);
  std::unique_ptr<Message> message{nullptr};
  archive >> message;
  buffer_.Shift(len_data);

  LocalWriter writer(system_, channel);
  writer.Send(std::move(message));
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

bool SendLength(Socket &socket, SizeT length) {
  return socket.Write(reinterpret_cast<uint8_t *>(&length), sizeof(SizeT));
}

struct PairHash {
 public:
  template <typename T, typename U>
  std::size_t operator()(const std::pair<T, U> &x) const {
    return std::hash<T>()(x.first) ^ std::hash<U>()(x.second);
  }
};

void SendMessage(const Endpoint &endpoint, const std::string &channel,
                 std::unique_ptr<Message> message) {
  static thread_local std::unordered_map<std::pair<std::string, uint16_t>,
                                         Socket, PairHash>
      cache;
  CHECK(message) << "Trying to send nullptr instead of message";

  auto it = cache.find({endpoint.address(), endpoint.port()});
  if (it == cache.end()) {
    Socket socket;
    if (!socket.Connect(endpoint)) {
      LOG(INFO) << "Couldn't connect to endpoint: " << endpoint;
      return;
    }

    it =
        cache
            .emplace(std::piecewise_construct,
                     std::forward_as_tuple(endpoint.address(), endpoint.port()),
                     std::forward_as_tuple(std::move(socket)))
            .first;
  }

  auto &socket = it->second;
  if (!SendLength(socket, channel.size())) {
    LOG(INFO) << "Couldn't send channel size!";
    return;
  }
  if (!socket.Write(channel)) {
    LOG(INFO) << "Couldn't send channel data!";
    return;
  }

  // Serialize and send message
  std::stringstream stream;
  boost::archive::binary_oarchive archive(stream);
  archive << message;

  const std::string &buffer = stream.str();
  int64_t message_size = 2 * sizeof(SizeT) + buffer.size() + channel.size();
  CHECK(message_size <= kMaxMessageSize) << fmt::format(
      "Trying to send message of size {}, max message size is {}", message_size,
      kMaxMessageSize);

  if (!SendLength(socket, buffer.size())) {
    LOG(INFO) << "Couldn't send message size!";
    return;
  }
  if (!socket.Write(buffer)) {
    LOG(INFO) << "Couldn't send message data!";
    return;
  }
}
}  // namespace communication::messaging
