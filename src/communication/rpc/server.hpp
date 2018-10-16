#pragma once

#include <map>
#include <mutex>
#include <vector>

#include "capnp/any.h"

#include "communication/rpc/messages.capnp.h"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/protocol.hpp"
#include "communication/server.hpp"
#include "io/network/endpoint.hpp"
#include "utils/demangle.hpp"

namespace communication::rpc {

class Server {
 public:
  Server(const io::network::Endpoint &endpoint,
         size_t workers_count = std::thread::hardware_concurrency());
  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  bool Start();
  void Shutdown();
  void AwaitShutdown();

  const io::network::Endpoint &endpoint() const;

  template <class TRequestResponse>
  void Register(std::function<
                void(const typename TRequestResponse::Request::Capnp::Reader &,
                     typename TRequestResponse::Response::Capnp::Builder *)>
                    callback) {
    std::lock_guard<std::mutex> guard(lock_);
    CHECK(!server_.IsRunning()) << "You can't register RPCs when the server is running!";
    RpcCallback rpc;
    rpc.req_type = TRequestResponse::Request::TypeInfo;
    rpc.res_type = TRequestResponse::Response::TypeInfo;
    rpc.callback = [callback = callback](const auto &reader, auto *builder) {
      auto req_data =
          reader.getData()
              .template getAs<typename TRequestResponse::Request::Capnp>();
      builder->setTypeId(TRequestResponse::Response::TypeInfo.id);
      auto data_builder = builder->initData();
      auto res_builder =
          data_builder
              .template initAs<typename TRequestResponse::Response::Capnp>();
      callback(req_data, &res_builder);
    };

    if (extended_callbacks_.find(TRequestResponse::Request::TypeInfo.id) !=
        extended_callbacks_.end()) {
      LOG(FATAL) << "Callback for that message type already registered!";
    }

    auto got = callbacks_.insert({TRequestResponse::Request::TypeInfo.id, rpc});
    CHECK(got.second) << "Callback for that message type already registered";
    VLOG(12) << "[RpcServer] register " << rpc.req_type.name << " -> "
             << rpc.res_type.name;
  }

  template <class TRequestResponse>
  void Register(std::function<
                void(const io::network::Endpoint &,
                     const typename TRequestResponse::Request::Capnp::Reader &,
                     typename TRequestResponse::Response::Capnp::Builder *)>
                    callback) {
    std::lock_guard<std::mutex> guard(lock_);
    CHECK(!server_.IsRunning()) << "You can't register RPCs when the server is running!";
    RpcExtendedCallback rpc;
    rpc.req_type = TRequestResponse::Request::TypeInfo;
    rpc.res_type = TRequestResponse::Response::TypeInfo;
    rpc.callback = [callback = callback](const io::network::Endpoint &endpoint,
                                         const auto &reader, auto *builder) {
      auto req_data =
          reader.getData()
              .template getAs<typename TRequestResponse::Request::Capnp>();
      builder->setTypeId(TRequestResponse::Response::TypeInfo.id);
      auto data_builder = builder->initData();
      auto res_builder =
          data_builder
              .template initAs<typename TRequestResponse::Response::Capnp>();
      callback(endpoint, req_data, &res_builder);
    };

    if (callbacks_.find(TRequestResponse::Request::TypeInfo.id) !=
        callbacks_.end()) {
      LOG(FATAL) << "Callback for that message type already registered!";
    }

    auto got =
        extended_callbacks_.insert({TRequestResponse::Request::TypeInfo.id, rpc});
    CHECK(got.second) << "Callback for that message type already registered";
    VLOG(12) << "[RpcServer] register " << rpc.req_type.name << " -> "
             << rpc.res_type.name;
  }

 private:
  friend class Session;

  struct RpcCallback {
    MessageType req_type;
    std::function<void(const capnp::Message::Reader &,
                       capnp::Message::Builder *)>
        callback;
    MessageType res_type;
  };

  struct RpcExtendedCallback {
    MessageType req_type;
    std::function<void(const io::network::Endpoint &,
                       const capnp::Message::Reader &,
                       capnp::Message::Builder *)>
        callback;
    MessageType res_type;
  };

  std::mutex lock_;
  std::map<uint64_t, RpcCallback> callbacks_;
  std::map<uint64_t, RpcExtendedCallback> extended_callbacks_;

  // TODO (mferencevic): currently the RPC server is hardcoded not to use SSL
  communication::ServerContext context_;
  communication::Server<Session, Server> server_;
};  // namespace communication::rpc

}  // namespace communication::rpc
