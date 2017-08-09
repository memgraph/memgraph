#pragma once

#include <cassert>
#include <condition_variable>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <typeindex>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>

#include "protocol.hpp"

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

class Message;
class EventStream;
class Reactor;
class System;
class Connector;

extern thread_local Reactor* current_reactor_;

/**
 * Write-end of a Connector (between two reactors).
 */
class Channel {
 public:
  /**
   * Construct and send the message to the channel.
   */
  template<typename MsgType, typename... Args>
  void Send(Args&&... args) {
    SendHelper(typeid(MsgType), std::unique_ptr<Message>(new MsgType(std::forward<Args>(args)...)));
  }

  template<typename MsgType>
  void Send(std::unique_ptr<MsgType>&& msg_ptr) {
    SendHelper(typeid(MsgType), std::move(msg_ptr));
  }

  virtual std::string Address() = 0;

  virtual uint16_t Port() = 0;

  virtual std::string ReactorName() = 0;

  virtual std::string Name() = 0;

  void operator=(const Channel &) = delete;

  template <class Archive>
  void serialize(Archive &archive) {
    archive(Address(), Port(), ReactorName(), Name());
  }

  virtual void SendHelper(const std::type_index&, std::unique_ptr<Message>) = 0;
};

/**
 * Read-end of a Connector (between two reactors).
 */
class EventStream {
 public:
  /**
   * Blocks until a message arrives.
   */
  virtual std::pair<std::type_index, std::unique_ptr<Message>> AwaitTypedEvent() = 0;
  std::unique_ptr<Message> AwaitEvent() { return AwaitTypedEvent().second; }

  /**
   * Polls if there is a message available, returning null if there is none.
   */
  virtual std::pair<std::type_index, std::unique_ptr<Message>> PopTypedEvent() = 0;
  std::unique_ptr<Message> PopEvent() { return PopTypedEvent().second; }

  /**
   * Subscription Service.
   *
   * Unsubscribe from a callback. Lightweight object (can copy by value).
   */
  class Subscription {
   public:
    /**
     * Unsubscribe. Call only once.
     */
    void unsubscribe() const;

   private:
    friend class Reactor;
    friend class Connector;

    Subscription(Connector& event_queue, std::type_index tidx, uint64_t cb_uid)
      : event_queue_(event_queue), tidx_(tidx), cb_uid_(cb_uid) { }

    Connector& event_queue_;
    std::type_index tidx_;
    uint64_t cb_uid_;
  };

  /**
   * Register a callback that will be called whenever an event arrives.
   */
  template<typename MsgType>
  void OnEvent(std::function<void(const MsgType&, const Subscription&)>&& cb) {
    OnEventHelper(typeid(MsgType), [cb = move(cb)](const Message& general_msg,
                                                   const Subscription& subscription) {
        const MsgType& correct_msg = dynamic_cast<const MsgType&>(general_msg);
        cb(correct_msg, subscription);
      });
  }

  class OnEventOnceChainer;
  /**
   * Starts a chain to register a callback that fires off only once.
   *
   * This method supports chaining (see the the class OnEventOnceChainer or the tests for examples).
   * Warning: when chaining callbacks, make sure that EventStream does not deallocate before the last
   * chained callback fired.
   */
  OnEventOnceChainer OnEventOnce() {
    return OnEventOnceChainer(*this);
  }

  /**
   * Close this event stream, disallowing further events from getting received.
   *
   * Any subsequent call after Close() to any function will be result in undefined
   * behavior (invalid pointer dereference). Can only be called from the thread
   * associated with the Reactor.
   */
  virtual void Close() = 0;

  /**
   * Convenience class to chain one-off callbacks.
   *
   * Usage: Create this class with OnEventOnce() and then chain callbacks using ChainOnce.
   * A callback will fire only once, unsubscribe and immediately subscribe the next callback to the stream.
   *
   * Example: stream->OnEventOnce().ChainOnce(firstCb).ChainOnce(secondCb);
   *
   * Implementation: This class is a temporary object that remembers the callbacks that are to be installed
   * and finally installs them in the destructor. Not sure is this kosher, is there another way?
   */
  class OnEventOnceChainer {
   public:
    OnEventOnceChainer(EventStream& event_stream) : event_stream_(event_stream) {}
    ~OnEventOnceChainer() {
      InstallCallbacks();
    }

    template<typename MsgType>
    OnEventOnceChainer& ChainOnce(std::function<void(const MsgType&)>&& cb) {
      std::function<void(const Message&, const Subscription&)> wrap =
        [cb = std::move(cb)](const Message& general_msg, const Subscription& subscription) {
          const MsgType& correct_msg = dynamic_cast<const MsgType&>(general_msg);
          subscription.unsubscribe();
          cb(correct_msg); // Warning: this can close the Channel, be careful what you put after it!
      };
      cbs_.emplace_back(typeid(MsgType), std::move(wrap));
      return *this;
    }

  private:
    void InstallCallbacks() {
      int num_callbacks = cbs_.size();
      assert(num_callbacks > 0); // We should install at least one callback, otherwise the usage is wrong?
      std::function<void(const Message&, const Subscription&)> next_cb = nullptr;
      std::type_index next_type = typeid(nullptr);

      for (int i = num_callbacks - 1; i >= 0; --i) {
        std::function<void(const Message&, const Subscription&)> tmp_cb = nullptr;
        tmp_cb = [cb = std::move(cbs_[i].second),
                  next_type,
                  next_cb = std::move(next_cb),
                  es_ptr = &this->event_stream_](const Message& msg, const Subscription& subscription) {
          cb(msg, subscription);
          if (next_cb != nullptr) {
            es_ptr->OnEventHelper(next_type, std::move(next_cb));
          }
        };
        next_cb = std::move(tmp_cb);
        next_type = cbs_[i].first;
      }

      event_stream_.OnEventHelper(next_type, std::move(next_cb));
    }

    EventStream& event_stream_;
    std::vector<std::pair<std::type_index, std::function<void(const Message&, const Subscription&)>>> cbs_;
  };
  typedef std::function<void(const Message&, const Subscription&)> Callback;

private:

  virtual void OnEventHelper(std::type_index tidx, Callback callback) = 0;
};

/**
 * Implementation of a connector.
 *
 * This class is an internal data structure that represents the state of the connector.
 * This class is not meant to be used by the clients of the messaging framework.
 * The Connector class wraps the event queue data structure, the mutex that protects
 * concurrent access to the event queue, the local channel and the event stream.
 * The class is owned by the Reactor. It gets closed when the owner reactor
 * (the one that owns the read-end of a connector) removes/closes it.
 */
class Connector {
 struct Params;

 public:
  friend class Reactor; // to create a Params initialization object
  friend class EventStream::Subscription;

  Connector(Params params)
      : system_(params.system),
        connector_name_(params.connector_name),
        reactor_name_(params.reactor_name),
        mutex_(params.mutex),
        cvar_(params.cvar),
        stream_(mutex_, connector_name_, this) {}

  /**
   * LocalChannel represents the channels to reactors living in the same reactor system (write-end of the connectors).
   *
   * Sending messages to the local channel requires acquiring the mutex.
   * LocalChannel holds a (weak) pointer to the enclosing Connector object.
   * Messages sent to a closed channel are ignored.
   * There can be multiple LocalChannels refering to the same stream if needed.
   */
  class LocalChannel : public Channel {
   public:
    friend class Connector;

    LocalChannel(std::shared_ptr<std::mutex> mutex, std::string reactor_name,
                 std::string connector_name, std::weak_ptr<Connector> queue, System *system)
        : mutex_(mutex),
          reactor_name_(reactor_name),
          connector_name_(connector_name),
          weak_queue_(queue),
          system_(system) {}

    virtual void SendHelper(const std::type_index& tidx, std::unique_ptr<Message> m) {
      std::shared_ptr<Connector> queue_ = weak_queue_.lock(); // Atomic, per the standard.
      if (queue_) {
        // We guarantee here that the Connector is not destroyed.
        std::unique_lock<std::mutex> lock(*mutex_);
        queue_->LockedPush(tidx, std::move(m));
      }
    }

    virtual std::string Address();

    virtual uint16_t Port();

    virtual std::string ReactorName();

    virtual std::string Name();

   private:
    std::shared_ptr<std::mutex> mutex_;
    std::string reactor_name_;
    std::string connector_name_;
    std::weak_ptr<Connector> weak_queue_;
    System *system_;
  };

  /**
   * Implementation of the event stream.
   *
   * After the enclosing Connector object is destroyed (by a call to CloseChannel or Close).
   */
  class LocalEventStream : public EventStream {
   public:
    friend class Connector;

    LocalEventStream(std::shared_ptr<std::mutex> mutex, std::string connector_name,
      Connector *queue) : mutex_(mutex), connector_name_(connector_name), queue_(queue) {}
    std::pair<std::type_index, std::unique_ptr<Message>> AwaitTypedEvent() {
      std::unique_lock<std::mutex> lock(*mutex_);
      return queue_->LockedAwaitPop(lock);
    }
    std::pair<std::type_index, std::unique_ptr<Message>> PopTypedEvent() {
      std::unique_lock<std::mutex> lock(*mutex_);
      return queue_->LockedPop();
    }
    void OnEventHelper(std::type_index tidx, Callback callback) {
      std::unique_lock<std::mutex> lock(*mutex_);
      queue_->LockedOnEventHelper(tidx, callback);
    }
    void Close();

   private:
    std::shared_ptr<std::mutex> mutex_;
    std::string connector_name_;
    Connector *queue_;
  };

  Connector(const Connector &other) = delete;
  Connector(Connector &&other) = default;
  Connector &operator=(const Connector &other) = delete;
  Connector &operator=(Connector &&other) = default;

private:
  /**
   * Initialization parameters to Connector.
   * Warning: do not forget to initialize self_ptr_ individually. Private because it shouldn't be created outside of a Reactor.
   */
  struct Params {
    System* system;
    std::string reactor_name;
    std::string connector_name;
    std::shared_ptr<std::mutex> mutex;
    std::shared_ptr<std::condition_variable> cvar;
  };


  void LockedPush(const std::type_index& tidx, std::unique_ptr<Message> m) {
    queue_.emplace(tidx, std::move(m));
    // This is OK because there is only one Reactor (thread) that can wait on this Connector.
    cvar_->notify_one();
  }

  std::shared_ptr<LocalChannel> LockedOpenChannel() {
    assert(!self_ptr_.expired()); // TODO(zuza): fix this using this answer https://stackoverflow.com/questions/45507041/how-to-check-if-weak-ptr-is-empty-non-assigned
    return std::make_shared<LocalChannel>(mutex_, reactor_name_, connector_name_, self_ptr_, system_);
  }

  std::pair<std::type_index, std::unique_ptr<Message>> LockedAwaitPop(std::unique_lock<std::mutex> &lock) {
    while (true) {
      std::pair<std::type_index, std::unique_ptr<Message>> m = LockedRawPop();
      if (!m.second) {
        cvar_->wait(lock);
      } else {
        return m;
      }
    }
  }

  std::pair<std::type_index, std::unique_ptr<Message>> LockedPop() {
    return LockedRawPop();
  }

  void LockedOnEventHelper(std::type_index tidx, EventStream::Callback callback) {
    uint64_t cb_uid = next_cb_uid++;
    callbacks_[tidx][cb_uid] = callback;
  }

  std::pair<std::type_index, std::unique_ptr<Message>> LockedRawPop() {
    if (queue_.empty()) return std::pair<std::type_index, std::unique_ptr<Message>>{typeid(nullptr), nullptr};
    std::pair<std::type_index, std::unique_ptr<Message> > t = std::move(queue_.front());
    queue_.pop();
    return t;
  }

  void RemoveCb(const EventStream::Subscription& subscription) {
    std::unique_lock<std::mutex> lock(*mutex_);
    size_t num_erased = callbacks_[subscription.tidx_].erase(subscription.cb_uid_);
    assert(num_erased == 1);
  }

  System *system_;
  std::string connector_name_;
  std::string reactor_name_;
  std::queue<std::pair<std::type_index, std::unique_ptr<Message>>> queue_;
  // Should only be locked once since it's used by a cond. var. Also caught in dctor, so must be recursive.
  std::shared_ptr<std::mutex> mutex_;
  std::shared_ptr<std::condition_variable> cvar_;
  /**
   * A weak_ptr to itself.
   *
   * There are initialization problems with this, check Params.
   */
  std::weak_ptr<Connector> self_ptr_;
  LocalEventStream stream_;
  std::unordered_map<std::type_index, std::unordered_map<uint64_t, EventStream::Callback> > callbacks_;
  uint64_t next_cb_uid = 0;
};

/**
 * A single unit of concurrent execution in the system.
 *
 * E.g. one worker, one client. Owned by System. Has a thread associated with it.
 */
class Reactor {
 public:
  friend class System;

  Reactor(System *system, std::string name)
      : system_(system), name_(name), main_(Open("main")) {}

  virtual ~Reactor() {}

  virtual void Run() = 0;

  std::pair<EventStream*, std::shared_ptr<Channel>> Open(const std::string &s);
  std::pair<EventStream*, std::shared_ptr<Channel>> Open();
  const std::shared_ptr<Channel> FindChannel(const std::string &channel_name);

  /**
   * Close a connector by name.
   *
   * Should only be called from the Reactor thread.
   */
  void CloseConnector(const std::string &s);

  /**
   * close all connectors (typically during shutdown).
   *
   * Should only be called from the Reactor thread.
   */
  void CloseAllConnectors();

  Reactor(const Reactor &other) = delete;
  Reactor(Reactor &&other) = default;
  Reactor &operator=(const Reactor &other) = delete;
  Reactor &operator=(Reactor &&other) = default;

 protected:
  System *system_;
  std::string name_;
  /*
   * Locks all Reactor data, including all Connector's in connectors_.
   *
   * This should be a shared_ptr because LocalChannel can outlive Reactor.
   */
  std::shared_ptr<std::mutex> mutex_ =
      std::make_shared<std::mutex>();
  std::shared_ptr<std::condition_variable> cvar_ =
      std::make_shared<std::condition_variable>();

  /**
   * List of connectors of a reactor indexed by name.
   *
   * While the connectors are owned by the reactor, a shared_ptr to solve the circular reference problem
   * between Channels and EventStreams.
   */
  std::unordered_map<std::string, std::shared_ptr<Connector>> connectors_;
  int64_t connector_name_counter_{0};
  std::pair<EventStream*, std::shared_ptr<Channel>> main_;

 private:
  typedef std::pair<std::unique_ptr<Message>,
                    std::vector<std::pair<EventStream::Callback, EventStream::Subscription> > > MsgAndCbInfo;

  /**
   * Dispatches all waiting messages to callbacks. Shuts down when there are no callbacks left.
   */
  void RunEventLoop();

  // TODO: remove proof of locking evidence ?!
  MsgAndCbInfo LockedGetPendingMessages();
};

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

    NetworkMessage(std::string _address, uint16_t _port, std::string _reactor,
                   std::string _channel, std::unique_ptr<Message> _message)
        : address(_address),
          port(_port),
          reactor(_reactor),
          channel(_channel),
          message(std::move(_message)) {}

    NetworkMessage(NetworkMessage &&nm)
        : address(nm.address),
          port(nm.port),
          reactor(nm.reactor),
          channel(nm.channel),
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

  std::shared_ptr<EventStream> AsyncResolve(std::string address, uint16_t port,
                                            int32_t retries,
                                            std::chrono::seconds cooldown) {
    // TODO: Asynchronously resolve channel, and return an event stream
    // that emits the channel after it gets resolved.
    return nullptr;
  }

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
    for (int i = 0; i < pool_.size(); ++i) {
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

    virtual void SendHelper(const std::type_index& tidx, std::unique_ptr<Message> message) {
      network_->mutex_.lock();
      network_->queue_.push(NetworkMessage(address_, port_, reactor_, channel_,
                                           std::move(message)));
      network_->mutex_.unlock();
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

/**
 * Base class for messages.
 */
class Message {
 public:
  virtual ~Message() {}

  template <class Archive>
  void serialize(Archive &) {}
};

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

  std::shared_ptr<Channel> GetChannelToSender(System *system) const;

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
 * Global placeholder for all reactors in the system. Alive through the entire process lifetime.
 *
 * E.g. holds set of reactors, channels for all reactors.
 */
class System {
 public:
  friend class Reactor;

  System() : network_(this) {}

  void operator=(const System &) = delete;

  void StartServices() {
    network_.StartClient(4);
    network_.StartServer(4);
  }

  template <class ReactorType, class... Args>
  const std::shared_ptr<Channel> Spawn(const std::string &name,
                                       Args &&... args) {
    std::unique_lock<std::recursive_mutex> lock(mutex_);
    auto *raw_reactor =
        new ReactorType(this, name, std::forward<Args>(args)...);
    std::unique_ptr<Reactor> reactor(raw_reactor);
    // Capturing a pointer isn't ideal, I would prefer to capture a Reactor&, but not sure how to do it.
    std::thread reactor_thread(
        [this, raw_reactor]() { this->StartReactor(*raw_reactor); });
    assert(reactors_.count(name) == 0);
    reactors_.emplace(name, std::pair<std::unique_ptr<Reactor>, std::thread>
                      (std::move(reactor), std::move(reactor_thread)));
    return nullptr;
  }

  const std::shared_ptr<Channel> FindChannel(const std::string &reactor_name,
                                             const std::string &channel_name) {
    std::unique_lock<std::recursive_mutex> lock(mutex_);
    auto it_reactor = reactors_.find(reactor_name);
    if (it_reactor == reactors_.end()) return nullptr;
    return it_reactor->second.first->FindChannel(channel_name);
  }

  void AwaitShutdown() {
    for (auto &key_value : reactors_) {
      auto &thread = key_value.second.second;
      thread.join();
    }
    network_.StopClient();
    network_.StopServer();
  }

  Network &network() { return network_; }

 private:
  void StartReactor(Reactor& reactor) {
    current_reactor_ = &reactor;
    reactor.Run();
    reactor.RunEventLoop();  // Activate callbacks.
  }

  std::recursive_mutex mutex_;
  // TODO: Replace with a map to a reactor Connector map to have more granular
  // locking.
  std::unordered_map<std::string,
                     std::pair<std::unique_ptr<Reactor>, std::thread>>
      reactors_;
  Network network_;
};
