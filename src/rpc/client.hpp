#pragma once

#include <memory>
#include <mutex>
#include <optional>

#include <glog/logging.h>

#include "communication/client.hpp"
#include "io/network/endpoint.hpp"
#include "rpc/exceptions.hpp"
#include "rpc/messages.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "utils/on_scope_exit.hpp"

namespace rpc {

/// Client is thread safe, but it is recommended to use thread_local clients.
class Client {
 public:
  Client(const io::network::Endpoint &endpoint,
         communication::ClientContext *context);

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
        [](auto *reader) {
          typename TRequestResponse::Response response;
          TRequestResponse::Response::Load(&response, reader);
          return response;
        },
        std::forward<Args>(args)...);
  }

  /// Same as `Call` but the first argument is a response loading function.
  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response CallWithLoad(
      std::function<typename TRequestResponse::Response(slk::Reader *)> load,
      Args &&... args) {
    typename TRequestResponse::Request request(std::forward<Args>(args)...);
    auto req_type = TRequestResponse::Request::kType;
    auto res_type = TRequestResponse::Response::kType;
    VLOG(12) << "[RpcClient] sent " << req_type.name;

    std::lock_guard<std::mutex> guard(mutex_);

    // Check if the connection is broken (if we haven't used the client for a
    // long time the server could have died).
    if (client_ && client_->ErrorStatus()) {
      client_ = std::nullopt;
    }

    // Connect to the remote server.
    if (!client_) {
      client_.emplace(context_);
      if (!client_->Connect(endpoint_)) {
        DLOG(ERROR) << "Couldn't connect to remote address " << endpoint_;
        client_ = std::nullopt;
        throw RpcFailedException(endpoint_);
      }
    }

    // Build and send the request.
    slk::Builder req_builder(
        [&](const uint8_t *data, size_t size, bool have_more) {
          client_->Write(data, size, have_more);
        });
    slk::Save(req_type.id, &req_builder);
    TRequestResponse::Request::Save(request, &req_builder);
    req_builder.Finalize();

    // Receive response.
    uint64_t response_data_size = 0;
    while (true) {
      auto ret =
          slk::CheckStreamComplete(client_->GetData(), client_->GetDataSize());
      if (ret.status == slk::StreamStatus::INVALID) {
        throw RpcFailedException(endpoint_);
      } else if (ret.status == slk::StreamStatus::PARTIAL) {
        if (!client_->Read(ret.stream_size - client_->GetDataSize(),
                           /* exactly_len = */ false)) {
          throw RpcFailedException(endpoint_);
        }
      } else {
        response_data_size = ret.stream_size;
        break;
      }
    }

    // Load the response.
    slk::Reader res_reader(client_->GetData(), response_data_size);
    utils::OnScopeExit res_cleanup(
        [&, response_data_size] { client_->ShiftData(response_data_size); });

    uint64_t res_id = 0;
    slk::Load(&res_id, &res_reader);

    // Check response ID.
    if (res_id != res_type.id) {
      LOG(ERROR) << "Message response was of unexpected type";
      client_ = std::nullopt;
      throw RpcFailedException(endpoint_);
    }

    VLOG(12) << "[RpcClient] received " << res_type.name;

    return load(&res_reader);
  }

  /// Call this function from another thread to abort a pending RPC call.
  void Abort();

 private:
  io::network::Endpoint endpoint_;
  communication::ClientContext *context_;
  std::optional<communication::Client> client_;

  std::mutex mutex_;
};

}  // namespace rpc
