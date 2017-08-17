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

#include "protocol.hpp"
#include "reactors_local.hpp"

#include "cereal/archives/binary.hpp"
#include "cereal/types/base_class.hpp"
#include "cereal/types/memory.hpp"
#include "cereal/types/polymorphic.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"  // utility has to be included because of std::pair
#include "cereal/types/vector.hpp"

#include "communication/server.hpp"
#include "threading/sync/spinlock.hpp"

DECLARE_string(address);
DECLARE_int32(port);

/**
 * Networking service.
 */
class Network {
 private:
  using Endpoint = Protocol::Endpoint;
  using Socket = Protocol::Socket;
  using NetworkServer = communication::Server<Protocol::Session,
                                              Protocol::Socket, Protocol::Data>;

  struct NetworkMessage {
    NetworkMessage()
      : address(""), port(0), reactor(""), channel(""), message(nullptr) {}

    NetworkMessage(const std::string& _address, uint16_t _port,
                   const std::string& _reactor, const std::string& _channel,
                   std::unique_ptr<Message> _message)
        : address(_address),
          port(_port),
          reactor(_reactor),
          channel(_channel),
          message(std::move(_message)) {}

    NetworkMessage(NetworkMessage &&nm)
      : address(std::move(nm.address)),
        port(std::move(nm.port)),
        reactor(std::move(nm.reactor)),
        channel(std::move(nm.channel)),
        message(std::move(nm.message)) {}

    std::string address;
    uint16_t port;
    std::string reactor;
    std::string channel;
    std::unique_ptr<Message> message;
  };

 public:
  Network(System *system);

  // client functions

  std::shared_ptr<Channel> Resolve(std::string address, uint16_t port,
                                   std::string reactor_name,
                                   std::string channel_name) {
    if (Protocol::SendMessage(address, port, reactor_name, channel_name,
                              nullptr)) {
      return std::make_shared<RemoteChannel>(this, address, port, reactor_name,
                                             channel_name);
    }
    return nullptr;
  }

  std::shared_ptr<EventStream> AsyncResolve(const std::string& address, uint16_t port,
                                            int32_t retries,
                                            std::chrono::seconds cooldown) {
    // TODO: Asynchronously resolve channel, and return an event stream
    // that emits the channel after it gets resolved.
    return nullptr;
  }

  /** Start a threadpool that dispatches the messages from the (outgoing) queue to the sockets */
  void StartClient(int worker_count) {
    LOG(INFO) << "Starting " << worker_count << " client workers";
    for (int i = 0; i < worker_count; ++i) {
      pool_.push_back(std::thread([worker_count, this]() {
        while (this->client_run_) {
          this->mutex_.lock();
          if (!this->queue_.empty()) {
            NetworkMessage nm(std::move(this->queue_.front()));
            this->queue_.pop();
            this->mutex_.unlock();
            // TODO: store success
            bool success =
                Protocol::SendMessage(nm.address, nm.port, nm.reactor,
                                      nm.channel, std::move(nm.message));
            std::cout << "Network client message send status: " << success << std::endl;
          } else {
            this->mutex_.unlock();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
      }));
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  }

  void StopClient() {
    while (true) {
      std::lock_guard<SpinLock> lock(mutex_);
      if (queue_.empty()) {
        break;
      }
    }
    client_run_ = false;
    for (size_t i = 0; i < pool_.size(); ++i) {
      pool_[i].join();
    }
  }

  class RemoteChannel : public Channel {
   public:
    RemoteChannel(Network *network, std::string address, uint16_t port,
                  std::string reactor, std::string channel)
        : network_(network),
          address_(address),
          port_(port),
          reactor_(reactor),
          channel_(channel) {}

    virtual std::string Address() { return address_; }

    virtual uint16_t Port() { return port_; }

    virtual std::string ReactorName() { return reactor_; }

    virtual std::string Name() { return channel_; }

    virtual void Send(std::unique_ptr<Message> message) {
      std::lock_guard<SpinLock> lock(network_->mutex_);
      network_->queue_.push(NetworkMessage(address_, port_, reactor_, channel_,
                                           std::move(message)));
    }

   private:
    Network *network_;
    std::string address_;
    uint16_t port_;
    std::string reactor_;
    std::string channel_;
  };

  // server functions

  std::string Address() { return FLAGS_address; }

  uint16_t Port() { return FLAGS_port; }

  /** Start a threadpool that relays the messages from the sockets to the LocalEventStreams */
  void StartServer(int workers_count) {
    if (server_ != nullptr) {
      LOG(FATAL) << "Tried to start a running server!";
    }

    // Initialize endpoint.
    Endpoint endpoint;
    try {
      endpoint = Endpoint(FLAGS_address.c_str(), FLAGS_port);
    } catch (io::network::NetworkEndpointException &e) {
      LOG(FATAL) << e.what();
    }

    // Initialize socket.
    Socket socket;
    if (!socket.Bind(endpoint)) {
      LOG(FATAL) << "Cannot bind to socket on " << FLAGS_address << " at "
                 << FLAGS_port;
    }
    if (!socket.SetNonBlocking()) {
      LOG(FATAL) << "Cannot set socket to non blocking!";
    }
    if (!socket.Listen(1024)) {
      LOG(FATAL) << "Cannot listen on socket!";
    }

    // Initialize server
    server_ =
        std::make_unique<NetworkServer>(std::move(socket), protocol_data_);

    // Start server
    thread_ = std::thread(
        [workers_count, this]() { this->server_->Start(workers_count); });
  }

  void StopServer() {
    if (server_ != nullptr) {
      server_->Shutdown();
      thread_.join();
    }
  }

 private:
  System *system_;

  // client variables
  SpinLock mutex_;
  std::vector<std::thread> pool_;
  std::queue<NetworkMessage> queue_;
  std::atomic<bool> client_run_{true};

  // server variables
  std::thread thread_;
  Protocol::Data protocol_data_;
  std::unique_ptr<NetworkServer> server_{nullptr};
};

class Distributed;

/**
 * Message that includes the sender channel used to respond.
 */
class SenderMessage : public Message {
 public:
  SenderMessage();
  SenderMessage(std::string reactor, std::string channel);

  std::string Address() const;
  uint16_t Port() const;
  std::string ReactorName() const;
  std::string ChannelName() const;

  std::shared_ptr<Channel> GetChannelToSender(System *system,
                                              Distributed *distributed = nullptr) const;

  template <class Archive>
  void serialize(Archive &ar) {
    ar(cereal::virtual_base_class<Message>(this), address_, port_,
       reactor_, channel_);
  }

 private:
  std::string address_;
  uint16_t port_;
  std::string reactor_;
  std::string channel_;
};
CEREAL_REGISTER_TYPE(SenderMessage);


/**
 * Message that will arrive on a stream returned by Distributed::FindChannel
 * once and if the channel is successfully resolved.
 */
class ChannelResolvedMessage : public Message {
 public:
  ChannelResolvedMessage() {}
  ChannelResolvedMessage(std::shared_ptr<Channel> channel)
    : Message(), channel_(channel) {}

  std::shared_ptr<Channel> channel() const { return channel_; }

 private:
  std::shared_ptr<Channel> channel_;
};

/**
 * Placeholder for all functionality related to non-local communication
 * E.g. resolve remote channels by memgraph node id, etc.
 */
class Distributed {
 public:
  Distributed(System &system) : system_(system), network_(&system) {}

  Distributed(const Distributed &) = delete;
  Distributed(Distributed &&) = delete;
  Distributed &operator=(const Distributed &) = delete;
  Distributed &operator=(Distributed &&) = delete;

  void StartServices() {
    network_.StartClient(4);
    network_.StartServer(4);
  }

  void StopServices() {
    network_.StopClient();
    network_.StopServer();
  }

  // TODO: Implement remote Spawn.

  /**
   * Resolves remote channel.
   *
   * TODO: Provide asynchronous implementation of this function.
   *
   * @return EventStream on which message will arrive once channel is resolved.
   * @warning It can only be called from local Reactor.
   */
  EventStream* FindChannel(const std::string &address,
                           uint16_t port,
                           const std::string &reactor_name,
                           const std::string &channel_name) {
    std::shared_ptr<Channel> channel = nullptr;
    while (!(channel = network_.Resolve(address, port, reactor_name, channel_name)))
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto stream_channel = current_reactor_->Open();
    stream_channel.second->Send<ChannelResolvedMessage>(channel);
    return stream_channel.first;
  }

  System &system() { return system_; }
  Network &network() { return network_; }

 protected:
  System &system_;
  Network network_;
};


class DistributedReactor : public Reactor {
 public:
  DistributedReactor(System *system, std::string name, Distributed &distributed)
    : Reactor(system, name), distributed_(distributed) {}

 protected:
  Distributed &distributed_;
};
