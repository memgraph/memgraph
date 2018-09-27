#pragma once

#include <mutex>
#include <stack>

#include "communication/rpc/client.hpp"

namespace communication::rpc {

/**
 * A simple client pool that creates new RPC clients on demand. Useful when you
 * want to send RPCs to the same server from multiple threads without them
 * blocking each other.
 */
class ClientPool {
 public:
  explicit ClientPool(const io::network::Endpoint &endpoint)
      : endpoint_(endpoint) {}

  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response Call(Args &&... args) {
    return WithUnusedClient([&](const auto &client) {
      return client->template Call<TRequestResponse>(
          std::forward<Args>(args)...);
    });
  };

  template <class TRequestResponse, class... Args>
  typename TRequestResponse::Response CallWithLoad(
      std::function<typename TRequestResponse::Response(
          const typename TRequestResponse::Response::Capnp::Reader &)>
          load,
      Args &&... args) {
    return WithUnusedClient([&](const auto &client) {
      return client->template CallWithLoad<TRequestResponse>(
          load, std::forward<Args>(args)...);
    });
  };

 private:
  template <class TFun>
  auto WithUnusedClient(const TFun &fun) {
    std::unique_ptr<Client> client;

    std::unique_lock<std::mutex> lock(mutex_);
    if (unused_clients_.empty()) {
      client = std::make_unique<Client>(endpoint_);
    } else {
      client = std::move(unused_clients_.top());
      unused_clients_.pop();
    }
    lock.unlock();

    auto res = fun(client);

    lock.lock();
    unused_clients_.push(std::move(client));
    return res;
  }

  io::network::Endpoint endpoint_;

  std::mutex mutex_;
  std::stack<std::unique_ptr<Client>> unused_clients_;
};

}  // namespace communication::rpc
