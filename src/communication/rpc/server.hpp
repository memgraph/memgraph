#pragma once

#include <unordered_map>
#include <vector>

#include "capnp/any.h"

#include "communication/rpc/messages.capnp.h"
#include "communication/rpc/messages.hpp"
#include "communication/rpc/protocol.hpp"
#include "communication/server.hpp"
#include "data_structures/concurrent/concurrent_map.hpp"
#include "data_structures/queue.hpp"
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

  void StopProcessingCalls();

  const io::network::Endpoint &endpoint() const;

  template <class TRequestResponse>
  void Register(std::function<
                void(const typename TRequestResponse::Request::Capnp::Reader &,
                     typename TRequestResponse::Response::Capnp::Builder *)>
                    callback) {
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
    auto callbacks_accessor = callbacks_.access();
    auto got =
        callbacks_accessor.insert(TRequestResponse::Request::TypeInfo.id, rpc);
    CHECK(got.second) << "Callback for that message type already registered";
    VLOG(12) << "[RpcServer] register " << rpc.req_type.name << " -> "
             << rpc.res_type.name;
  }

  template <typename TRequestResponse>
  void UnRegister() {
    const MessageType &type = TRequestResponse::Request::TypeInfo;
    auto callbacks_accessor = callbacks_.access();
    auto deleted = callbacks_accessor.remove(type.id);
    CHECK(deleted) << "Trying to remove unknown message type callback";
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

  ConcurrentMap<uint64_t, RpcCallback> callbacks_;

  std::mutex mutex_;
  // TODO (mferencevic): currently the RPC server is hardcoded not to use SSL
  communication::ServerContext context_;
  communication::Server<Session, Server> server_;
};  // namespace communication::rpc

}  // namespace communication::rpc
