#pragma once

#include <type_traits>
#include <unordered_map>
#include <vector>

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

  template <typename TRequestResponse>
  void Register(
      std::function<std::unique_ptr<typename TRequestResponse::Response>(
          const typename TRequestResponse::Request &)>
          callback) {
    static_assert(
        std::is_base_of<Message, typename TRequestResponse::Request>::value,
        "TRequestResponse::Request must be derived from Message");
    static_assert(
        std::is_base_of<Message, typename TRequestResponse::Response>::value,
        "TRequestResponse::Response must be derived from Message");
    auto callbacks_accessor = callbacks_.access();
    auto got = callbacks_accessor.insert(
        typeid(typename TRequestResponse::Request),
        [callback = callback](const Message &base_message) {
          const auto &message =
              dynamic_cast<const typename TRequestResponse::Request &>(
                  base_message);
          return callback(message);
        });
    CHECK(got.second) << "Callback for that message type already registered";
    if (VLOG_IS_ON(12)) {
      auto req_type =
          utils::Demangle(typeid(typename TRequestResponse::Request).name());
      auto res_type =
          utils::Demangle(typeid(typename TRequestResponse::Response).name());
      LOG(INFO) << "[RpcServer] register " << (req_type ? req_type.value() : "")
                << " -> " << (res_type ? res_type.value() : "");
    }
  }

 private:
  friend class Session;

  ConcurrentMap<std::type_index,
                std::function<std::unique_ptr<Message>(const Message &)>>
      callbacks_;

  std::mutex mutex_;
  communication::Server<Session, Server> server_;
};

}  // namespace communication::rpc
