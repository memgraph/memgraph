#pragma once

#include <experimental/optional>
#include <memory>
#include <mutex>
#include <random>

#include <glog/logging.h>

#include "communication/client.hpp"
#include "communication/rpc/messages.hpp"
#include "io/network/endpoint.hpp"
#include "utils/demangle.hpp"

namespace communication::rpc {

// Client is thread safe, but it is recommended to use thread_local clients.
class Client {
 public:
  Client(const io::network::Endpoint &endpoint);

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
    auto request = Req(std::forward<Args>(args)...);

    if (VLOG_IS_ON(12)) {
      auto req_type = utils::Demangle(request.type_index().name());
      LOG(INFO) << "[RpcClient] sent " << (req_type ? req_type.value() : "");
    }

    std::unique_ptr<Message> response = Call(request);
    auto *real_response = dynamic_cast<Res *>(response.get());
    if (!real_response && response) {
      // Since message_id was checked in private Call function, this means
      // something is very wrong (probably on the server side).
      LOG(ERROR) << "Message response was of unexpected type";
      client_ = std::experimental::nullopt;
      return nullptr;
    }

    if (VLOG_IS_ON(12) && response) {
      auto res_type = utils::Demangle(response->type_index().name());
      LOG(INFO) << "[RpcClient] received "
                << (res_type ? res_type.value() : "");
    }

    response.release();
    return std::unique_ptr<Res>(real_response);
  }

  // Call this function from another thread to abort a pending RPC call.
  void Abort();

 private:
  std::unique_ptr<Message> Call(const Message &request);

  io::network::Endpoint endpoint_;
  std::experimental::optional<communication::Client> client_;

  std::mutex mutex_;

  // Random generator for simulated network latency (enable with a flag).
  // Distribution parameters are rule-of-thumb chosen.
  std::mt19937 gen_{std::random_device{}()};
  std::lognormal_distribution<> rand_{0.0, 1.11};
};

}  // namespace communication::rpc
