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

#include "communication/reactor/reactor_local.hpp"
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

DECLARE_string(reactor_address);
DECLARE_int32(reactor_port);

namespace communication::reactor {

class DistributedSystem;

/**
 * Networking service.
 */
class Network {
 private:
  using Endpoint = io::network::NetworkEndpoint;
  using Socket = Socket;
  using ServerT = communication::Server<Session, SessionData>;
  friend class DistributedSystem;

  struct NetworkMessage {
    NetworkMessage() {}

    NetworkMessage(const std::string &address, uint16_t port,
                   const std::string &reactor, const std::string &channel,
                   std::unique_ptr<Message> &&message)
        : address(address),
          port(port),
          reactor(reactor),
          channel(channel),
          message(std::move(message)) {}

    NetworkMessage(NetworkMessage &&nm) = default;
    NetworkMessage &operator=(NetworkMessage &&nm) = default;

    std::string address;
    uint16_t port = 0;
    std::string reactor;
    std::string channel;
    std::unique_ptr<Message> message;
  };

 public:
  Network() = default;

  /** Start a threadpool that dispatches the messages from the (outgoing) queue
   * to the sockets */
  void StartClient(int worker_count) {
    LOG(INFO) << "Starting " << worker_count << " client workers";

    // condition variables here...
    for (int i = 0; i < worker_count; ++i) {
      pool_.push_back(std::thread([this]() {
        while (true) {
          auto message = queue_.AwaitPop();
          if (message == std::experimental::nullopt) break;
          SendMessage(message->address, message->port, message->reactor,
                      message->channel, std::move(message->message));
        }
      }));
    }
  }

  void StopClient() {
    while (true) {
      std::lock_guard<SpinLock> lock(mutex_);
      if (queue_.empty()) {
        break;
      }
    }
    queue_.Shutdown();
    for (size_t i = 0; i < pool_.size(); ++i) {
      pool_[i].join();
    }
    pool_.clear();
  }

  class RemoteChannelWriter : public ChannelWriter {
   public:
    RemoteChannelWriter(const std::string &address, uint16_t port,
                        const std::string &reactor, const std::string &channel,
                        DistributedSystem &system);

    // TODO: This is wrong. We should probbly have base class Address that would
    // contain everything needed to reference a channel. (address, port,
    // reactor_name, channel_name) in remote reactors and (reactor_name,
    // channel_name) in local reactors.
    virtual std::string Address() { return address_; }
    virtual uint16_t Port() { return port_; }
    std::string ReactorName() const override { return reactor_; }
    std::string Name() const override { return channel_; }

    template <typename TMessage, typename... Args>
    void Send(Args &&... args) {
      Send(std::unique_ptr<Message>(
          std::make_unique<TMessage>(std::forward<Args>(args)...)));
    }

    void Send(std::unique_ptr<Message> message) override {
      std::lock_guard<SpinLock> lock(network_->mutex_);
      network_->queue_.Emplace(address_, port_, reactor_, channel_,
                               std::move(message));
    }

   private:
    Network *network_;
    std::string address_;
    uint16_t port_;
    std::string reactor_;
    std::string channel_;
  };

  // server functions

  std::string address() const { return FLAGS_reactor_address; }

  uint16_t port() const { return FLAGS_reactor_port; }

  /** Start a threadpool that relays the messages from the sockets to the
   * LocalEventStreams */
  void StartServer(int workers_count) {
    if (server_ != nullptr) {
      LOG(FATAL) << "Tried to start a running server!";
    }

    // Initialize endpoint.
    Endpoint endpoint;
    try {
      endpoint = Endpoint(FLAGS_reactor_address.c_str(), FLAGS_reactor_port);
    } catch (io::network::NetworkEndpointException &e) {
      LOG(FATAL) << e.what();
    }
    // Initialize server
    server_ = std::make_unique<ServerT>(endpoint, protocol_data_);

    // Start server
    thread_ = std::thread(
        [workers_count, this]() { this->server_->Start(workers_count); });
  }

  void StopServer() {
    if (server_ != nullptr) {
      server_->Shutdown();
      thread_.join();
      server_ = nullptr;
    }
  }

 private:
  // client variables
  SpinLock mutex_;
  std::vector<std::thread> pool_;
  Queue<NetworkMessage> queue_;

  // server variables
  std::thread thread_;
  SessionData protocol_data_;
  std::unique_ptr<ServerT> server_{nullptr};
};

using RemoteChannelWriter = Network::RemoteChannelWriter;

/**
 * Placeholder for all functionality related to non-local communication.
 * E.g. resolve remote channels by memgraph node id, etc.
 */
class DistributedSystem {
 public:
  DistributedSystem() {
    network_.StartClient(4);
    network_.StartServer(4);
  }

  // Thread safe.
  std::unique_ptr<Reactor> Spawn(const std::string &name,
                                 std::function<void(Reactor &)> setup) {
    return system_.Spawn(name, setup);
  }

  // Non-thread safe.
  // TODO: figure out what should be interection of this function and
  // destructor.
  void StopServices() {
    network_.StopClient();
    network_.StopServer();
  }

  Network &network() { return network_; }
  const Network &network() const { return network_; }

  // Should be private
  Network network_;

 private:
  System &system_ = network_.protocol_data_.system;

  DistributedSystem(const DistributedSystem &) = delete;
  DistributedSystem(DistributedSystem &&) = delete;
  DistributedSystem &operator=(const DistributedSystem &) = delete;
  DistributedSystem &operator=(DistributedSystem &&) = delete;
};
}  // namespace communication::reactor
