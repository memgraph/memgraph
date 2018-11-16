#pragma once

#include <experimental/optional>
#include <memory>
#include <mutex>

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <glog/logging.h>

#include "communication/client.hpp"
#include "communication/rpc/exceptions.hpp"
#include "communication/rpc/messages.capnp.h"
#include "communication/rpc/messages.hpp"
#include "io/network/endpoint.hpp"
#include "utils/demangle.hpp"

namespace communication::rpc {

/// Client is thread safe, but it is recommended to use thread_local clients.
class Client {
 public:
  explicit Client(const io::network::Endpoint &endpoint);

  /// Call a previously defined and registered RPC call. This function can
  /// initiate only one request at a time. The call blocks until a response is
  /// received.
  ///
  /// @returns TRequestResponse::Response object that was specified to be
  ///                                     returned by the RPC call
  /// @throws RpcFailedException if an error was occurred while executing the
  ///                            RPC call (eg. connection failed, remote end
  ///                            died, etc.)
  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response Call(Args &&... args) {
    return CallWithLoad<TRequestResponse>(
        [](const auto &reader) {
          typename TRequestResponse::Response response;
          Load(&response, reader);
          return response;
        },
        std::forward<Args>(args)...);
  }

  /// Same as `Call` but the first argument is a response loading function.
  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response CallWithLoad(
      std::function<typename TRequestResponse::Response(
          const typename TRequestResponse::Response::Capnp::Reader &)>
          load,
      Args &&... args) {
    typename TRequestResponse::Request request(std::forward<Args>(args)...);
    auto req_type = TRequestResponse::Request::kType;
    VLOG(12) << "[RpcClient] sent " << req_type.name;
    ::capnp::MallocMessageBuilder req_msg;
    {
      auto builder = req_msg.initRoot<capnp::Message>();
      builder.setTypeId(req_type.id);
      auto data_builder = builder.initData();
      auto req_builder =
          data_builder
              .template initAs<typename TRequestResponse::Request::Capnp>();
      Save(request, &req_builder);
    }
    auto response = Send(&req_msg);
    auto res_msg = response.getRoot<capnp::Message>();
    auto res_type = TRequestResponse::Response::kType;
    if (res_msg.getTypeId() != res_type.id) {
      // Since message_id was checked in private Call function, this means
      // something is very wrong (probably on the server side).
      LOG(ERROR) << "Message response was of unexpected type";
      client_ = std::experimental::nullopt;
      throw RpcFailedException(endpoint_);
    }

    VLOG(12) << "[RpcClient] received " << res_type.name;

    auto data_reader =
        res_msg.getData()
            .template getAs<typename TRequestResponse::Response::Capnp>();
    return load(data_reader);
  }

  /// Call this function from another thread to abort a pending RPC call.
  void Abort();

 private:
  ::capnp::FlatArrayMessageReader Send(::capnp::MessageBuilder *message);

  io::network::Endpoint endpoint_;
  // TODO (mferencevic): currently the RPC client is hardcoded not to use SSL
  communication::ClientContext context_;
  std::experimental::optional<communication::Client> client_;

  std::mutex mutex_;
};

}  // namespace communication::rpc
