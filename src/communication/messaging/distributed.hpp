#pragma once

#include <cassert>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <tuple>
#include <typeindex>
#include <utility>

#include <gflags/gflags.h>

#include "communication/messaging/local.hpp"
#include "data_structures/queue.hpp"
#include "protocol.hpp"

#include "cereal/archives/binary.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/types/vector.hpp"

#include "communication/server.hpp"
#include "threading/sync/spinlock.hpp"

namespace communication::messaging {

class System;

// Writes message to remote event stream.
class Writer {
 public:
  Writer(System &system, const std::string &address, uint16_t port,
         const std::string &name);
  Writer(const Writer &) = delete;
  void operator=(const Writer &) = delete;
  Writer(Writer &&) = delete;
  void operator=(Writer &&) = delete;

  template <typename TMessage, typename... Args>
  void Send(Args &&... args) {
    Send(std::unique_ptr<Message>(
        std::make_unique<TMessage>(std::forward<Args>(args)...)));
  }

  void Send(std::unique_ptr<Message> message);

 private:
  System &system_;
  std::string address_;
  uint16_t port_;
  std::string name_;
};

class System {
 public:
  friend class Writer;

  System(const std::string &address, uint16_t port);
  System(const System &) = delete;
  System(System &&) = delete;
  System &operator=(const System &) = delete;
  System &operator=(System &&) = delete;
  ~System();

  std::shared_ptr<EventStream> Open(const std::string &name);
  void Shutdown();

  const std::string &address() const { return address_; }
  uint16_t port() const { return port_; }

 private:
  using Endpoint = io::network::NetworkEndpoint;
  using Socket = Socket;
  using ServerT = communication::Server<Session, SessionData>;

  struct NetworkMessage {
    NetworkMessage() {}

    NetworkMessage(const std::string &address, uint16_t port,
                   const std::string &channel,
                   std::unique_ptr<Message> &&message)
        : address(address),
          port(port),
          channel(channel),
          message(std::move(message)) {}

    NetworkMessage(NetworkMessage &&nm) = default;
    NetworkMessage &operator=(NetworkMessage &&nm) = default;

    std::string address;
    uint16_t port = 0;
    std::string channel;
    std::unique_ptr<Message> message;
  };

  /** Start a threadpool that dispatches the messages from the (outgoing) queue
   * to the sockets */
  void StartClient(int worker_count);

  /** Start a threadpool that relays the messages from the sockets to the
   * LocalEventStreams */
  void StartServer(int workers_count);

  // Client variables.
  std::vector<std::thread> pool_;
  Queue<NetworkMessage> queue_;

  // Server variables.
  std::thread thread_;
  SessionData protocol_data_;
  std::unique_ptr<ServerT> server_{nullptr};
  std::string address_;
  uint16_t port_;

  LocalSystem &system_ = protocol_data_.system;
};
}  // namespace communication::messaging
