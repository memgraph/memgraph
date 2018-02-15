#pragma once

#include <experimental/optional>
#include <memory>
#include <mutex>

#include <glog/logging.h>

#include "communication/rpc/buffer.hpp"
#include "communication/rpc/messages.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/socket.hpp"

namespace communication::rpc {

// Client is thread safe, but it is recommended to use thread_local clients.
class Client {
 public:
  Client(const io::network::Endpoint &endpoint, const std::string &name);

  // Call function can initiate only one request at the time. Function blocks
  // until there is a response. If there was an error nullptr is returned.
  template <typename TRequestResponse, typename... Args>
  std::unique_ptr<typename TRequestResponse::Response> Call(Args &&... args) {
    using Req = typename TRequestResponse::Request;
    using Res = typename TRequestResponse::Response;
    static_assert(std::is_base_of<Message, Req>::value,
                  "TRequestResponse::Request must be derived from Message");
    static_assert(std::is_base_of<Message, Res>::value,
                  "TRequestResponse::Response must be derived from Message");
    auto response = Call(std::unique_ptr<Message>(
        std::make_unique<Req>(std::forward<Args>(args)...)));
    auto *real_response = dynamic_cast<Res *>(response.get());
    if (!real_response && response) {
      // Since message_id was checked in private Call function, this means
      // something is very wrong (probably on the server side).
      LOG(ERROR) << "Message response was of unexpected type";
      socket_ = std::experimental::nullopt;
      return nullptr;
    }
    response.release();
    return std::unique_ptr<Res>(real_response);
  }

  // Call this function from another thread to abort a pending RPC call.
  void Abort();

 private:
  std::unique_ptr<Message> Call(std::unique_ptr<Message> request);

  io::network::Endpoint endpoint_;
  std::string service_name_;

  std::experimental::optional<io::network::Socket> socket_;

  uint32_t next_message_id_{0};
  Buffer buffer_;

  std::mutex mutex_;
};

}  // namespace communication::rpc
