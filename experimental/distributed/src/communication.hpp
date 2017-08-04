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
#include <unordered_map>

#include "cereal/types/base_class.hpp"

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
  virtual void Send(std::unique_ptr<Message>) = 0;

  virtual std::string Hostname() = 0;

  virtual int32_t Port() = 0;

  virtual std::string ReactorName() = 0;

  virtual std::string Name() = 0;

  void operator=(const Channel &) = delete;

  template <class Archive>
  void serialize(Archive &archive) {
    archive(Hostname(), Port(), ReactorName(), Name());
  }
};

/**
 * Read-end of a Connector (between two reactors).
 */
class EventStream {
 public:  
  /**
   * Blocks until a message arrives.
   */
  virtual std::unique_ptr<Message> AwaitEvent() = 0;

  /**
   * Polls if there is a message available, returning null if there is none.
   */
  virtual std::unique_ptr<Message> PopEvent() = 0;

  /**
   * Subscription Service. Lightweight object (can copy by value).
   */
  class Subscription {
   public:
    /**
     * Unsubscribe. Call only once.
     */
    void unsubscribe();

   private:
    friend class Reactor;

    Subscription(Connector& event_queue, uint64_t cb_uid) : event_queue_(event_queue) {
      cb_uid_ = cb_uid;
    }
    Connector& event_queue_;
    uint64_t cb_uid_;
  };
  
  typedef std::function<void(const Message&, Subscription&)> Callback;

  /**
   * Register a callback that will be called whenever an event arrives.
   */
  virtual void OnEvent(Callback callback) = 0;
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
 class Params;

 public:
  friend class Reactor; // to create a Params initialization object
  friend class EventStream::Subscription;

  Connector(Params params)
      : system_(params.system),
        name_(params.name),
        reactor_name_(params.reactor_name),
        mutex_(params.mutex),
        cvar_(params.cvar),
        stream_(mutex_, name_, this) {}

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
                 std::string name, std::weak_ptr<Connector> queue, System *system)
        : mutex_(mutex),
          reactor_name_(reactor_name),
          name_(name),
          weak_queue_(queue),
          system_(system) {}

    virtual void Send(std::unique_ptr<Message> m) {
      std::shared_ptr<Connector> queue_ = weak_queue_.lock(); // Atomic, per the standard.
      if (queue_) {
        // We guarantee here that the Connector is not destroyed.
        std::unique_lock<std::mutex> lock(*mutex_);
        queue_->LockedPush(lock, std::move(m));
      }
    }

    virtual std::string Hostname();

    virtual int32_t Port();

    virtual std::string ReactorName();

    virtual std::string Name();

   private:
    std::shared_ptr<std::mutex> mutex_;
    std::string reactor_name_;
    std::string name_;
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

    LocalEventStream(std::shared_ptr<std::mutex> mutex, std::string name,
      Connector *queue) : mutex_(mutex), name_(name), queue_(queue) {}
    std::unique_ptr<Message> AwaitEvent() {
      std::unique_lock<std::mutex> lock(*mutex_);
      if (queue_ != nullptr) {
        return queue_->LockedAwaitPop(lock);
      }
      throw std::runtime_error(
          "Cannot call method after connector was closed.");
    }
    std::unique_ptr<Message> PopEvent() {
      std::unique_lock<std::mutex> lock(*mutex_);
      if (queue_ != nullptr) {
        return queue_->LockedPop(lock);
      }
      throw std::runtime_error(
          "Cannot call method after connector was closed.");
    }
    void OnEvent(EventStream::Callback callback) {
      std::unique_lock<std::mutex> lock(*mutex_);
      if (queue_ != nullptr) {
        queue_->LockedOnEvent(callback);
        return;
      }
      throw std::runtime_error(
          "Cannot call method after connector was closed.");
    }

   private:
    std::shared_ptr<std::mutex> mutex_;
    std::string name_;
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
    /**
     * Connector name.
     */
    std::string name;
    std::shared_ptr<std::mutex> mutex;
    std::shared_ptr<std::condition_variable> cvar;
  };


  void LockedPush(std::unique_lock<std::mutex> &, std::unique_ptr<Message> m) {
    queue_.push(std::move(m));
    // This is OK because there is only one Reactor (thread) that can wait on this Connector.
    cvar_->notify_one();
  }

  std::shared_ptr<LocalChannel> LockedOpenChannel() {
    assert(!self_ptr_.expired()); // TODO(zuza): fix this using this answer https://stackoverflow.com/questions/45507041/how-to-check-if-weak-ptr-is-empty-non-assigned
    return std::make_shared<LocalChannel>(mutex_, reactor_name_, name_, self_ptr_, system_);
  }

  std::unique_ptr<Message> LockedAwaitPop(std::unique_lock<std::mutex> &lock) {
    std::unique_ptr<Message> m;
    while (!(m = LockedRawPop())) {
      cvar_->wait(lock);
    }
    return m;
  }

  std::unique_ptr<Message> LockedPop(std::unique_lock<std::mutex> &lock) {
    return LockedRawPop();
  }

  void LockedOnEvent(EventStream::Callback callback) {
    uint64_t cb_uid = next_cb_uid++;
    callbacks_[cb_uid] = callback;
  }
  
  std::unique_ptr<Message> LockedRawPop() {
    if (queue_.empty()) return nullptr;
    std::unique_ptr<Message> t = std::move(queue_.front());
    queue_.pop();
    return t;
  }

  void RemoveCbByUid(uint64_t uid) {
    std::unique_lock<std::mutex> lock(*mutex_);
    size_t num_erased = callbacks_.erase(uid);
    assert(num_erased == 1);
  }

  System *system_;
  std::string name_;
  std::string reactor_name_;
  std::queue<std::unique_ptr<Message>> queue_;
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
  std::unordered_map<uint64_t, EventStream::Callback> callbacks_;
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
  int64_t channel_name_counter_{0};
  std::pair<EventStream*, std::shared_ptr<Channel>> main_;

 private:
  typedef std::pair<std::unique_ptr<Message>,
                    std::vector<std::pair<EventStream::Callback, EventStream::Subscription> > > MsgAndCbInfo;

  /**
   * Dispatches all waiting messages to callbacks. Shuts down when there are no callbacks left.
   */
  void RunEventLoop();

  // TODO: remove proof of locking evidence ?!
  MsgAndCbInfo LockedGetPendingMessages(std::unique_lock<std::mutex> &lock);
};

/**
 * Configuration service.
 */
class Config {
 public:
  Config(System *system) : system_(system) {}

  std::string GetString(std::string key) {
    // TODO: Use configuration lib.
    assert(key == "hostname");
    return "localhost";
  }

  int32_t GetInt(std::string key) {
    // TODO: Use configuration lib.
    assert(key == "port");
    return 8080;
  }
 private:
  System *system_;
};

/**
 * Networking service.
 */
class Network {
 public:
  Network(System *system);

  std::string Hostname() { return hostname_; }

  int32_t Port() { return port_; }

  std::shared_ptr<Channel> Resolve(std::string hostname, int32_t port) {
    // TODO: Synchronously resolve and return channel.
    return nullptr;
  }

  std::shared_ptr<EventStream> AsyncResolve(std::string hostname, int32_t port,
                                            int32_t retries,
                                            std::chrono::seconds cooldown) {
    // TODO: Asynchronously resolve channel, and return an event stream
    // that emits the channel after it gets resolved.
    return nullptr;
  }

  class RemoteChannel : public Channel {
   public:
    RemoteChannel() {}

    virtual std::string Hostname() {
      throw std::runtime_error("Unimplemented.");
    }

    virtual int32_t Port() { throw std::runtime_error("Unimplemented."); }

    virtual std::string ReactorName() {
      throw std::runtime_error("Unimplemented.");
    }

    virtual std::string Name() { throw std::runtime_error("Unimplemented."); }

    virtual void Send(std::unique_ptr<Message> message) {
      // TODO: Implement.
    }
  };

 private:
  System *system_;
  std::string hostname_;
  int32_t port_;
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
  SenderMessage(std::shared_ptr<Channel> sender) : sender_(sender) {}

  std::shared_ptr<Channel> sender() { return sender_; }

  template <class Archive>
  void serialize(Archive &ar) {
    ar(sender_);
  }

 private:
  std::shared_ptr<Channel> sender_;
};

/**
 * Serialization service.
 */
class Serialization {
 public:
  using SerializedT = std::pair<char *, int64_t>;

  Serialization(System *system) : system_(system) {}

  SerializedT serialize(const Message &) {
    SerializedT serialized;
    throw std::runtime_error("Not yet implemented (Serialization::serialized)");
    return serialized;
  }

  Message deserialize(const SerializedT &) {
    Message message;
    throw std::runtime_error(
        "Not yet implemented (Serialization::deserialize)");
    return message;
  }

 private:
  System *system_;
};

/**
 * Global placeholder for all reactors in the system. Alive through the entire process lifetime.
 *
 * E.g. holds set of reactors, channels for all reactors.
 */
class System {
 public:
  friend class Reactor;

  System() : config_(this), network_(this), serialization_(this) {}

  void operator=(const System &) = delete;

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
  }

  Config &config() { return config_; }

  Network &network() { return network_; }

  Serialization &serialization() { return serialization_; }

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
  Config config_;
  Network network_;
  Serialization serialization_;
};
