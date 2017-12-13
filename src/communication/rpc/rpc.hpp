#pragma once

#include <type_traits>

#include "communication/messaging/distributed.hpp"

namespace communication::rpc {

template <typename TRequest, typename TResponse>
struct RequestResponse {
  using Request = TRequest;
  using Response = TResponse;
};

// Client is thread safe.
class Client {
 public:
  Client(messaging::System &system, const std::string &address, uint16_t port,
         const std::string &name);

  // Call function can initiate only one request at the time. Function blocks
  // until there is a response or timeout was reached. If timeout was reached
  // nullptr is returned.
  template <typename TRequestResponse, typename... Args>
  std::unique_ptr<typename TRequestResponse::Response> Call(
      std::chrono::system_clock::duration timeout, Args &&... args) {
    using Req = typename TRequestResponse::Request;
    using Res = typename TRequestResponse::Response;
    static_assert(std::is_base_of<messaging::Message, Req>::value,
                  "TRequestResponse::Request must be derived from Message");
    static_assert(std::is_base_of<messaging::Message, Res>::value,
                  "TRequestResponse::Response must be derived from Message");
    std::lock_guard<std::mutex> lock(lock_);
    auto response =
        Call(timeout, std::unique_ptr<messaging::Message>(
                          std::make_unique<Req>(std::forward<Args>(args)...)));
    auto *real_response = dynamic_cast<Res *>(response.get());
    if (!real_response && response) {
      LOG(ERROR) << "Message response was of unexpected type";
      return nullptr;
    }
    response.release();
    return std::unique_ptr<Res>(real_response);
  }

 private:
  std::unique_ptr<messaging::Message> Call(
      std::chrono::system_clock::duration timeout,
      std::unique_ptr<messaging::Message> message);

  messaging::System &system_;
  messaging::Writer writer_;
  std::shared_ptr<messaging::EventStream> stream_;
  std::mutex lock_;
};

class Server {
 public:
  Server(messaging::System &system, const std::string &name);

  template <typename TRequestResponse>
  void Register(
      std::function<std::unique_ptr<typename TRequestResponse::Response>(
          const typename TRequestResponse::Request &)>
          callback) {
    static_assert(std::is_base_of<messaging::Message,
                                  typename TRequestResponse::Request>::value,
                  "TRequestResponse::Request must be derived from Message");
    static_assert(std::is_base_of<messaging::Message,
                                  typename TRequestResponse::Response>::value,
                  "TRequestResponse::Response must be derived from Message");
    auto got = callbacks_.emplace(
        typeid(typename TRequestResponse::Request), [callback = callback](
                                                        const messaging::Message
                                                            &base_message) {
          const auto &message =
              dynamic_cast<const typename TRequestResponse::Request &>(
                  base_message);
          return callback(message);
        });
    CHECK(got.second) << "Callback for that message type already registered";
  }

  void Start();
  void Shutdown();

 private:
  messaging::System &system_;
  std::shared_ptr<messaging::EventStream> stream_;
  std::unordered_map<std::type_index,
                     std::function<std::unique_ptr<messaging::Message>(
                         const messaging::Message &)>>
      callbacks_;
  std::atomic<bool> alive_{true};
};
}  // namespace communication::rpc
