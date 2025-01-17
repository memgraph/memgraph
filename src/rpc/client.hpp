// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "communication/client.hpp"
#include "io/network/endpoint.hpp"
#include "rpc/exceptions.hpp"
#include "rpc/messages.hpp"
#include "rpc/version.hpp"
#include "slk/serialization.hpp"
#include "slk/streams.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/typeinfo.hpp"

#include "io/network/fmt.hpp"

namespace memgraph::rpc {

using namespace std::string_view_literals;

/// Client is thread safe, but it is recommended to use thread_local clients.
class Client {
 public:
  inline static std::unordered_map<std::string_view, int> const default_rpc_timeouts_ms{
      {"ShowInstancesReq"sv, 10000},          // coordinator sending to coordinator
      {"DemoteMainToReplicaReq"sv, 10000},    // coordinator sending to main
      {"PromoteToMainReq"sv, 10000},          // coordinator sending to replica
      {"RegisterReplicaOnMainReq"sv, 10000},  // coordinator sending to main
      {"UnregisterReplicaReq"sv, 10000},      // coordinator sending to main
      {"EnableWritingOnMainReq"sv, 10000},    // coordinator to main
      {"GetInstanceUUIDReq"sv, 10000},        // coordinator to data instances
      {"GetDatabaseHistoriesReq"sv, 10000},   // coordinator to data instances
      {"StateCheckReq"sv, 10000},             // coordinator to data instances
      {"HeartbeatReq"sv, 10000},              // main to replica
      {"TimestampReq"sv, 10000},              // main to replica
      {"SwapMainUUIDReq"sv, 10000},           // coord to data instances
      {"FrequentHeartbeatReq"sv, 10000},      // coord to data instances
  };
  // Dependency injection of rpc_timeouts
  Client(io::network::Endpoint endpoint, communication::ClientContext *context,
         std::unordered_map<std::string_view, int> const &rpc_timeouts_ms = Client::default_rpc_timeouts_ms);

  /// Object used to handle streaming of request data to the RPC server.
  template <class TRequestResponse>
  class StreamHandler {
   private:
    friend class Client;

    StreamHandler(Client *self, std::unique_lock<std::mutex> &&guard,
                  std::function<typename TRequestResponse::Response(slk::Reader *)> res_load,
                  std::optional<int> timeout_ms)
        : self_(self),
          timeout_ms_(timeout_ms),
          guard_(std::move(guard)),
          req_builder_(GenBuilderCallback(self, this, timeout_ms_)),
          res_load_(res_load) {}

   public:
    StreamHandler(StreamHandler &&other) noexcept
        : self_{std::exchange(other.self_, nullptr)},
          timeout_ms_{std::move(other.timeout_ms_)},
          defunct_{std::exchange(other.defunct_, true)},
          guard_{std::move(other.guard_)},
          req_builder_{std::move(other.req_builder_), GenBuilderCallback(self_, this, timeout_ms_)},
          res_load_{std::move(other.res_load_)} {}
    StreamHandler &operator=(StreamHandler &&other) noexcept {
      if (&other != this) {
        self_ = std::exchange(other.self_, nullptr);
        timeout_ms_ = std::move(other.timeout_ms_);
        defunct_ = std::exchange(other.defunct_, true);
        guard_ = std::move(other.guard_);
        req_builder_ = slk::Builder(std::move(other.req_builder_, GenBuilderCallback(self_, this, timeout_ms_)));
        res_load_ = std::move(other.res_load_);
      }
      return *this;
    }

    StreamHandler(const StreamHandler &) = delete;
    StreamHandler &operator=(const StreamHandler &) = delete;

    ~StreamHandler() = default;

    slk::Builder *GetBuilder() { return &req_builder_; }

    typename TRequestResponse::Response AwaitResponse() {
      auto res_type = TRequestResponse::Response::kType;
      auto req_type = TRequestResponse::Request::kType;

      auto req_type_name = std::string_view{req_type.name};
      auto res_type_name = std::string_view{res_type.name};

      // Finalize the request.
      req_builder_.Finalize();

      spdlog::trace("[RpcClient] sent {} to {}", req_type_name, self_->client_->endpoint().SocketAddress());

      // Receive the response.
      uint64_t response_data_size = 0;
      while (true) {
        auto ret = slk::CheckStreamComplete(self_->client_->GetData(), self_->client_->GetDataSize());
        if (ret.status == slk::StreamStatus::INVALID) {
          // Logically invalid state, connection is still up, defunct stream and release
          defunct_ = true;
          guard_.unlock();
          throw GenericRpcFailedException();
        }
        if (ret.status == slk::StreamStatus::PARTIAL) {
          if (!self_->client_->Read(ret.stream_size - self_->client_->GetDataSize(),
                                    /* exactly_len = */ false, /* timeout_ms = */ timeout_ms_)) {
            // Failed connection, abort and let somebody retry in the future.
            defunct_ = true;
            self_->Abort();
            guard_.unlock();
            throw GenericRpcFailedException();
          }
        } else {
          response_data_size = ret.stream_size;
          break;
        }
      }

      // Load the response.
      slk::Reader res_reader(self_->client_->GetData(), response_data_size);
      utils::OnScopeExit res_cleanup([&, response_data_size] { self_->client_->ShiftData(response_data_size); });

      utils::TypeId res_id{utils::TypeId::UNKNOWN};
      // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
      rpc::Version version;

      try {
        slk::Load(&res_id, &res_reader);
        slk::Load(&version, &res_reader);
      } catch (const slk::SlkReaderException &) {
        throw SlkRpcFailedException();
      }

      if (version != rpc::current_version) {
        // V1 we introduced versioning with, absolutely no backwards compatibility,
        // because it's impossible to provide backwards compatibility with pre versioning.
        // Future versions this may require mechanism for graceful version handling.
        throw VersionMismatchRpcFailedException();
      }

      // Check the response ID.
      if (res_id != res_type.id && res_id != utils::TypeId::UNKNOWN) {
        spdlog::error("Message response was of unexpected type");
        // Logically invalid state, connection is still up, defunct stream and release
        defunct_ = true;
        guard_.unlock();
        throw GenericRpcFailedException();
      }

      spdlog::trace("[RpcClient] received {} from endpoint {}:{}.", res_type_name, self_->endpoint_.GetAddress(),
                    self_->endpoint_.GetPort());

      return res_load_(&res_reader);
    }

    bool IsDefunct() const { return defunct_; }

   private:
    static auto GenBuilderCallback(Client *client, StreamHandler *self, std::optional<int> timeout_ms) {
      return [client, self, timeout_ms](const uint8_t *data, size_t size, bool have_more) {
        if (self->defunct_) throw GenericRpcFailedException();
        if (!client->client_->Write(data, size, have_more, timeout_ms)) {
          self->defunct_ = true;
          client->Abort();
          self->guard_.unlock();
          throw GenericRpcFailedException();
        }
      };
    }

    Client *self_;
    std::optional<int> timeout_ms_;
    bool defunct_ = false;
    std::unique_lock<std::mutex> guard_;
    slk::Builder req_builder_;
    std::function<typename TRequestResponse::Response(slk::Reader *)> res_load_;
  };

  /// Stream a previously defined and registered RPC call. This function can
  /// initiate only one request at a time. The call returns a `StreamHandler`
  /// object that can be used to send additional data to the request (with the
  /// automatically sent `TRequestResponse::Request` object) and await until the
  /// response is received from the server.
  ///
  /// @returns StreamHandler<TRequestResponse> object that is used to handle
  ///                                          streaming of additional data to
  ///                                          the client and to await the
  ///                                          response from the server
  /// @throws RpcFailedException if an error was occurred while executing the
  ///                            RPC call (eg. connection failed, remote end
  ///                            died, etc.)
  template <class TRequestResponse, class... Args>
  StreamHandler<TRequestResponse> Stream(Args &&...args) {
    return StreamWithLoad<TRequestResponse>(
        [](auto *reader) {
          typename TRequestResponse::Response response;
          TRequestResponse::Response::Load(&response, reader);
          return response;
        },
        std::forward<Args>(args)...);
  }

  /// Same as `Stream` but the first argument is a response loading function.
  template <class TRequestResponse, class... Args>
  StreamHandler<TRequestResponse> StreamWithLoad(std::function<typename TRequestResponse::Response(slk::Reader *)> load,
                                                 Args &&...args) {
    typename TRequestResponse::Request request(std::forward<Args>(args)...);
    auto req_type = TRequestResponse::Request::kType;

    auto guard = std::unique_lock{mutex_};

    // Check if the connection is broken (if we haven't used the client for a
    // long time the server could have died).
    if (client_ && client_->ErrorStatus()) {
      client_ = std::nullopt;
    }

    // Connect to the remote server.
    if (!client_) {
      client_.emplace(context_);
      if (!client_->Connect(endpoint_)) {
        spdlog::error("Couldn't connect to remote address {}", endpoint_);
        client_ = std::nullopt;
        throw GenericRpcFailedException();
      }
    }

    std::optional<int> timeout_ms{std::nullopt};

    auto req_type_name = std::string_view{req_type.name};
    auto const maybe_timeout = std::ranges::find_if(
        rpc_timeouts_ms_, [req_type_name](auto const &entry) { return entry.first == req_type_name; });
    if (maybe_timeout != rpc_timeouts_ms_.end()) {
      timeout_ms.emplace(maybe_timeout->second);
    }

    // Create the stream handler.
    StreamHandler<TRequestResponse> handler(this, std::move(guard), load, timeout_ms);

    // Build and send the request.
    slk::Save(req_type.id, handler.GetBuilder());
    slk::Save(rpc::current_version, handler.GetBuilder());
    TRequestResponse::Request::Save(request, handler.GetBuilder());

    // Return the handler to the user.
    return handler;
  }

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
  typename TRequestResponse::Response Call(Args &&...args) {
    auto stream = Stream<TRequestResponse>(std::forward<Args>(args)...);
    return stream.AwaitResponse();
  }

  /// Same as `Call` but the first argument is a response loading function.
  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response CallWithLoad(
      std::function<typename TRequestResponse::Response(slk::Reader *)> load, Args &&...args) {
    auto stream = StreamWithLoad(load, std::forward<Args>(args)...);
    return stream.AwaitResponse();
  }

  /// Call this function from another thread to abort a pending RPC call.
  void Abort();

  auto Endpoint() const -> io::network::Endpoint const & { return endpoint_; }

 private:
  io::network::Endpoint endpoint_;
  communication::ClientContext *context_;
  std::optional<communication::Client> client_;
  std::unordered_map<std::string_view, int> rpc_timeouts_ms_;

  mutable std::mutex mutex_;
};

}  // namespace memgraph::rpc
