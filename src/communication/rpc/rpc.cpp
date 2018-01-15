#include <iterator>
#include <random>

#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/access.hpp"
#include "boost/serialization/base_object.hpp"
#include "boost/serialization/export.hpp"
#include "boost/serialization/unique_ptr.hpp"

#include "communication/rpc/rpc.hpp"
#include "io/network/endpoint.hpp"
#include "utils/string.hpp"

namespace communication::rpc {

const char kProtocolStreamPrefix[] = "rpc-";

using Endpoint = io::network::Endpoint;

class Request : public messaging::Message {
 public:
  Request(const Endpoint &endpoint, const std::string &stream,
          std::unique_ptr<Message> message)
      : endpoint_(endpoint),
        stream_(stream),
        message_id_(utils::RandomString(20)),
        message_(std::move(message)) {}

  const Endpoint &endpoint() const { return endpoint_; }
  const std::string &stream() const { return stream_; }
  const std::string &message_id() const { return message_id_; }
  const messaging::Message &message() const { return *message_; }

 private:
  friend class boost::serialization::access;
  Request() {}  // Needed for serialization.

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<messaging::Message>(*this);
    ar &endpoint_;
    ar &stream_;
    ar &message_id_;
    ar &message_;
  }

  io::network::Endpoint endpoint_;
  std::string stream_;
  std::string message_id_;
  std::unique_ptr<messaging::Message> message_;
};

class Response : public messaging::Message {
 public:
  explicit Response(const std::string &message_id,
                    std::unique_ptr<messaging::Message> message)
      : message_id_(message_id), message_(std::move(message)) {}

  const auto &message_id() const { return message_id_; }
  auto &message() { return message_; }

 private:
  friend class boost::serialization::access;
  Response() {}  // Needed for serialization.

  template <class TArchive>
  void serialize(TArchive &ar, unsigned int) {
    ar &boost::serialization::base_object<Message>(*this);
    ar &message_id_;
    ar &message_;
  }

  std::string message_id_;
  std::unique_ptr<messaging::Message> message_;
};

Client::Client(messaging::System &system, const io::network::Endpoint &endpoint,
               const std::string &name)
    : system_(system),
      writer_(system, endpoint, kProtocolStreamPrefix + name),
      stream_(system.Open(utils::RandomString(20))) {}

// Because of the way Call is implemented it can fail without reporting (it will
// just block indefinately). This is why you always need to provide reasonable
// timeout when calling it.
// TODO: Make Call use same connection for request and respone and as soon as
// connection drop return nullptr.
std::unique_ptr<messaging::Message> Client::Call(
    std::chrono::system_clock::duration timeout,
    std::unique_ptr<messaging::Message> message) {
  auto request = std::make_unique<Request>(system_.endpoint(), stream_->name(),
                                           std::move(message));
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
    : system_(system), stream_(system.Open(kProtocolStreamPrefix + name)) {
  // TODO: Add logging.
  running_thread_ = std::thread([this]() {
    while (alive_) {
      auto message = stream_->Await();
      if (!message) continue;
      auto *request = dynamic_cast<Request *>(message.get());
      if (!request) continue;
      auto &real_request = request->message();
      auto callbacks_accessor = callbacks_.access();
      auto it = callbacks_accessor.find(real_request.type_index());
      if (it == callbacks_accessor.end()) continue;
      auto response = it->second(real_request);
      messaging::Writer writer(system_, request->endpoint(), request->stream());
      writer.Send<Response>(request->message_id(), std::move(response));
    }
  });
}

Server::~Server() {
  alive_ = false;
  stream_->Shutdown();
  if (running_thread_.joinable()) running_thread_.join();
}

}  // namespace communication::rpc

BOOST_CLASS_EXPORT(communication::rpc::Request);
BOOST_CLASS_EXPORT(communication::rpc::Response);
