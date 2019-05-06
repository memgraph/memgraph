#pragma once

#include <map>
#include <mutex>
#include <vector>

#include "communication/rpc/messages.hpp"
#include "communication/rpc/protocol.hpp"
#include "communication/server.hpp"
#include "io/network/endpoint.hpp"
#include "slk/streams.hpp"

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
  void Register(std::function<void(slk::Reader *, slk::Builder *)> callback) {
    std::lock_guard<std::mutex> guard(lock_);
    CHECK(!server_.IsRunning())
        << "You can't register RPCs when the server is running!";
    RpcCallback rpc;
    rpc.req_type = TRequestResponse::Request::kType;
    rpc.res_type = TRequestResponse::Response::kType;
    rpc.callback = callback;

    if (extended_callbacks_.find(TRequestResponse::Request::kType.id) !=
        extended_callbacks_.end()) {
      LOG(FATAL) << "Callback for that message type already registered!";
    }

    auto got = callbacks_.insert({TRequestResponse::Request::kType.id, rpc});
    CHECK(got.second) << "Callback for that message type already registered";
    VLOG(12) << "[RpcServer] register " << rpc.req_type.name << " -> "
             << rpc.res_type.name;
  }

  template <class TRequestResponse>
  void Register(std::function<void(const io::network::Endpoint &, slk::Reader *,
                                   slk::Builder *)>
                    callback) {
    std::lock_guard<std::mutex> guard(lock_);
    CHECK(!server_.IsRunning())
        << "You can't register RPCs when the server is running!";
    RpcExtendedCallback rpc;
    rpc.req_type = TRequestResponse::Request::kType;
    rpc.res_type = TRequestResponse::Response::kType;
    rpc.callback = callback;

    auto got =
        extended_callbacks_.insert({TRequestResponse::Request::kType.id, rpc});
    CHECK(got.second) << "Callback for that message type already registered";
    VLOG(12) << "[RpcServer] register " << rpc.req_type.name << " -> "
             << rpc.res_type.name;
  }

 private:
  friend class Session;

  struct RpcCallback {
    utils::TypeInfo req_type;
    std::function<void(slk::Reader *, slk::Builder *)> callback;
    utils::TypeInfo res_type;
  };

  struct RpcExtendedCallback {
    utils::TypeInfo req_type;
    std::function<void(const io::network::Endpoint &, slk::Reader *,
                       slk::Builder *)>
        callback;
    utils::TypeInfo res_type;
  };

  std::mutex lock_;
  std::map<uint64_t, RpcCallback> callbacks_;
  std::map<uint64_t, RpcExtendedCallback> extended_callbacks_;

  // TODO (mferencevic): currently the RPC server is hardcoded not to use SSL
  communication::ServerContext context_;
  communication::Server<Session, Server> server_;
};  // namespace communication::rpc

}  // namespace communication::rpc
