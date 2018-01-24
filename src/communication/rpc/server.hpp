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

namespace communication::rpc {

// Forward declaration of Server class
class Server;

class System {
 public:
  System(const io::network::Endpoint &endpoint, const size_t worker_count = 4);
  System(const System &) = delete;
  System(System &&) = delete;
  System &operator=(const System &) = delete;
  System &operator=(System &&) = delete;
  ~System();

  const io::network::Endpoint &endpoint() const { return server_.endpoint(); }

 private:
  using ServerT = communication::Server<Session, System>;
  friend class Session;
  friend class Server;

  /** Start a threadpool that relays the messages from the sockets to the
   * LocalEventStreams */
  void StartServer(int workers_count);

  void AddTask(std::shared_ptr<Socket> socket, const std::string &service,
               uint64_t message_id, std::unique_ptr<Message> message);
  void Add(Server &server);
  void Remove(const Server &server);

  std::mutex mutex_;
  // Service name to its server mapping.
  std::unordered_map<std::string, Server *> services_;
  ServerT server_;
};

class Server {
 public:
  Server(System &system, const std::string &name, int workers_count = 4);
  ~Server();

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
        typeid(typename TRequestResponse::Request), [callback = callback](
                                                        const Message
                                                            &base_message) {
          const auto &message =
              dynamic_cast<const typename TRequestResponse::Request &>(
                  base_message);
          return callback(message);
        });
    CHECK(got.second) << "Callback for that message type already registered";
  }

  const std::string &service_name() const { return service_name_; }

 private:
  friend class System;
  System &system_;
  Queue<std::tuple<std::shared_ptr<Socket>, uint64_t, std::unique_ptr<Message>>>
      queue_;
  std::string service_name_;
  ConcurrentMap<std::type_index,
                std::function<std::unique_ptr<Message>(const Message &)>>
      callbacks_;
  std::atomic<bool> alive_{true};
  std::vector<std::thread> threads_;
};

}  // namespace communication::rpc
