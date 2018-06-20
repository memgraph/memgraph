#pragma once

#include <experimental/optional>
#include <memory>
#include <mutex>
#include <random>

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <glog/logging.h>

#include "communication/client.hpp"
#include "communication/rpc/messages.capnp.h"
#include "communication/rpc/messages.hpp"
#include "io/network/endpoint.hpp"
#include "utils/demangle.hpp"

namespace communication::rpc {

/// Client is thread safe, but it is recommended to use thread_local clients.
class Client {
 public:
  explicit Client(const io::network::Endpoint &endpoint);

  /// Call function can initiate only one request at the time. Function blocks
  /// until there is a response. If there was an error nullptr is returned.
  template <class TRequestResponse, class... Args>
  std::experimental::optional<typename TRequestResponse::Response> Call(
      Args &&... args) {
    return CallWithLoad<TRequestResponse>(
        [](const auto &reader) {
          typename TRequestResponse::Response response;
          response.Load(reader);
          return response;
        },
        std::forward<Args>(args)...);
  }

  /// Same as `Call` but the first argument is a response loading function.
  template <class TRequestResponse, class... Args>
  std::experimental::optional<typename TRequestResponse::Response> CallWithLoad(
      std::function<typename TRequestResponse::Response(
          const typename TRequestResponse::Response::Capnp::Reader &)>
          load,
      Args &&... args) {
    typename TRequestResponse::Request request(std::forward<Args>(args)...);
    auto req_type = TRequestResponse::Request::TypeInfo;
    VLOG(12) << "[RpcClient] sent " << req_type.name;
    ::capnp::MallocMessageBuilder req_msg;
    {
      auto builder = req_msg.initRoot<capnp::Message>();
      builder.setTypeId(req_type.id);
      auto data_builder = builder.initData();
      auto req_builder =
          data_builder
              .template initAs<typename TRequestResponse::Request::Capnp>();
      request.Save(&req_builder);
    }
    auto maybe_response = Send(&req_msg);
    if (!maybe_response) {
      return std::experimental::nullopt;
    }
    auto res_msg = maybe_response->getRoot<capnp::Message>();
    auto res_type = TRequestResponse::Response::TypeInfo;
    if (res_msg.getTypeId() != res_type.id) {
      // Since message_id was checked in private Call function, this means
      // something is very wrong (probably on the server side).
      LOG(ERROR) << "Message response was of unexpected type";
      client_ = std::experimental::nullopt;
      return std::experimental::nullopt;
    }

    VLOG(12) << "[RpcClient] received " << res_type.name;

    auto data_reader =
        res_msg.getData()
            .template getAs<typename TRequestResponse::Response::Capnp>();
    return std::experimental::make_optional(load(data_reader));
  }

  /// Call this function from another thread to abort a pending RPC call.
  void Abort();

 private:
  std::experimental::optional<::capnp::FlatArrayMessageReader> Send(
      ::capnp::MessageBuilder *message);

  io::network::Endpoint endpoint_;
  // TODO (mferencevic): currently the RPC client is hardcoded not to use SSL
  communication::ClientContext context_;
  std::experimental::optional<communication::Client> client_;

  std::mutex mutex_;

  // Random generator for simulated network latency (enable with a flag).
  // Distribution parameters are rule-of-thumb chosen.
  std::mt19937 gen_{std::random_device{}()};
  std::lognormal_distribution<> rand_{0.0, 1.11};
};

}  // namespace communication::rpc
