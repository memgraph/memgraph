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
  ClientPool(const io::network::Endpoint &endpoint, const std::string &name)
      : endpoint_(endpoint), name_(name) {}

  template <typename TRequestResponse, typename... Args>
  std::unique_ptr<typename TRequestResponse::Response> Call(Args &&... args) {
    std::unique_ptr<Client> client;

    std::unique_lock<std::mutex> lock(mutex_);
    if (unused_clients_.empty()) {
      client = std::make_unique<Client>(endpoint_, name_);
    } else {
      client = std::move(unused_clients_.top());
      unused_clients_.pop();
    }
    lock.unlock();

    auto resp = client->Call<TRequestResponse>(std::forward<Args>(args)...);

    lock.lock();
    unused_clients_.push(std::move(client));
    return resp;
  };

 private:
  io::network::Endpoint endpoint_;
  std::string name_;

  std::mutex mutex_;
  std::stack<std::unique_ptr<Client>> unused_clients_;
};

}  // namespace communication::rpc
