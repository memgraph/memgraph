#include <iterator>
#include <random>

#include "communication/rpc/rpc.hpp"
#include "utils/string.hpp"

namespace communication::rpc {

const char kProtocolStreamPrefix[] = "rpc-";

class Request : public messaging::Message {
 public:
  Request(const std::string &address, uint16_t port, const std::string &stream,
          std::unique_ptr<Message> message)
      : address_(address),
        port_(port),
        stream_(stream),
        message_id_(utils::RandomString(20)),
        message_(std::move(message)) {}

  const std::string &address() const { return address_; }
  uint16_t port() const { return port_; }
  const std::string &stream() const { return stream_; }
  const std::string &message_id() const { return message_id_; }
  const messaging::Message &message() const { return *message_; }

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<messaging::Message>(this), address_, port_,
       stream_, message_id_, message_);
  }

 protected:
  friend class cereal::access;
  Request() {}  // Cereal needs access to a default constructor.

  std::string address_;
  uint16_t port_;
  std::string stream_;
  std::string message_id_;
  std::unique_ptr<messaging::Message> message_;
};

class Response : public messaging::Message {
 public:
  explicit Response(const std::string &message_id,
                    std::unique_ptr<messaging::Message> message)
      : message_id_(message_id), message_(std::move(message)) {}

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<messaging::Message>(this), message_id_,
       message_);
  }

  const auto &message_id() const { return message_id_; }
  auto &message() { return message_; }

 protected:
  Response() {}  // Cereal needs access to a default constructor.
  friend class cereal::access;
  std::string message_id_;
  std::unique_ptr<messaging::Message> message_;
};

Client::Client(messaging::System &system, const std::string &address,
               uint16_t port, const std::string &name)
    : system_(system),
      writer_(system, address, port, kProtocolStreamPrefix + name),
      stream_(system.Open(utils::RandomString(20))) {}

// Because of the way Call is implemented it can fail without reporting (it will
// just block indefinately). This is why you always need to provide reasonable
// timeout when calling it.
// TODO: Make Call use same connection for request and respone and as soon as
// connection drop return nullptr.
std::unique_ptr<messaging::Message> Client::Call(
    std::chrono::system_clock::duration timeout,
    std::unique_ptr<messaging::Message> message) {
  auto request = std::make_unique<Request>(system_.address(), system_.port(),
                                           stream_->name(), std::move(message));
  auto message_id = request->message_id();
  writer_.Send(std::move(request));

  auto now = std::chrono::system_clock::now();
  auto until = now + timeout;

  while (true) {
    auto message = stream_->Await(until - std::chrono::system_clock::now());
    if (!message) break;  // Client was either signaled or timeout was reached.
    auto *response = dynamic_cast<Response *>(message.get());
    if (!response) {
      LOG(ERROR) << "Message received by rpc client is not a response";
      continue;
    }
    if (response->message_id() != message_id) {
      // This can happen if some stale response arrives after we issued a new
      // request.
      continue;
    }
    return std::move(response->message());
  }
  return nullptr;
}

Server::Server(messaging::System &system, const std::string &name)
    : system_(system), stream_(system.Open(kProtocolStreamPrefix + name)) {}

void Server::Start() {
  // TODO: Add logging.
  while (alive_) {
    auto message = stream_->Await();
    if (!message) continue;
    auto *request = dynamic_cast<Request *>(message.get());
    if (!request) continue;
    auto &real_request = request->message();
    auto it = callbacks_.find(real_request.type_index());
    if (it == callbacks_.end()) continue;
    auto response = it->second(real_request);
    messaging::Writer writer(system_, request->address(), request->port(),
                             request->stream());
    writer.Send<Response>(request->message_id(), std::move(response));
  }
}

void Server::Shutdown() {
  alive_ = false;
  stream_->Shutdown();
}
}
CEREAL_REGISTER_TYPE(communication::rpc::Request);
CEREAL_REGISTER_TYPE(communication::rpc::Response);
