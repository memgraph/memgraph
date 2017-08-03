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
class EventQueue;

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

    Subscription(EventQueue& event_queue, uint64_t cb_uid) : event_queue_(event_queue) {
      cb_uid_ = cb_uid;
    }
    EventQueue& event_queue_;
    uint64_t cb_uid_;
  };
  
  typedef std::function<void(const Message&, Subscription&)> Callback;

  /**
   * Register a callback that will be called whenever an event arrives.
   */
  virtual void OnEvent(Callback callback) = 0;

  /**
   * Close this event stream, disallowing further events from getting received.
   */
  virtual void Close() = 0;
};

/**
 * Implementation of a connector.
 *
 * This class is an internal data structure that represents the state of the connector.
 * This class is not meant to be used by the clients of the messaging framework.
 * The EventQueue class wraps the event queue data structure, the mutex that protects
 * concurrent access to the event queue, the local channel and the event stream.
 * The class is owned by the Reactor, but its LocalChannel can outlive it.
 * See the LocalChannel and LocalEventStream nested classes for further information.
 */
class EventQueue {
 public:
  friend class Reactor;
  friend class EventStream::Subscription;

  struct Params {
    System* system;
    std::string reactor_name;
    std::string name;
    std::shared_ptr<std::recursive_mutex> mutex;
    std::shared_ptr<std::condition_variable_any> cvar;
  };

  EventQueue(Params params)
      : system_(params.system),
        reactor_name_(params.reactor_name),
        name_(params.name),
        mutex_(params.mutex),
        cvar_(params.cvar) {}

  /**
   * The destructor locks the mutex of the EventQueue and sets queue pointer to null.
   */
  ~EventQueue() {
    // Ugly: this is the ONLY thing that is allowed to lock this recursive mutex twice.
    // This is because we can't make a locked and a unlocked version of the destructor.
    std::unique_lock<std::recursive_mutex> lock(*mutex_);
    stream_->queue_ = nullptr;
    channel_->queue_ = nullptr;
  }

  void LockedPush(std::unique_lock<std::recursive_mutex> &, std::unique_ptr<Message> m) {
    queue_.push(std::move(m));
    cvar_->notify_one();
  }

  std::unique_ptr<Message> LockedAwaitPop(std::unique_lock<std::recursive_mutex> &lock) {
    std::unique_ptr<Message> m;
    while (!(m = LockedRawPop())) {
      cvar_->wait(lock);
    }
    return m;
  }

  std::unique_ptr<Message> LockedPop(std::unique_lock<std::recursive_mutex> &lock) {
    return LockedRawPop();
  }

  void LockedOnEvent(EventStream::Callback callback) {
    uint64_t cb_uid = next_cb_uid++;
    callbacks_[cb_uid] = callback;
  }

  /**
   * LocalChannel represents the channels to reactors living in the same reactor system.
   *
   * Sending messages to the local channel requires acquiring the mutex.
   * LocalChannel holds a pointer to the enclosing EventQueue object.
   * The enclosing EventQueue object is destroyed when the reactor calls Close.
   * When this happens, the pointer to the enclosing EventQueue object is set to null.
   * After this, all the message sends on this channel are dropped.
   */
  class LocalChannel : public Channel {
   public:
    friend class EventQueue;

    LocalChannel(std::shared_ptr<std::recursive_mutex> mutex, std::string reactor_name,
                 std::string name, EventQueue *queue, System *system)
        : mutex_(mutex),
          reactor_name_(reactor_name),
          name_(name),
          queue_(queue),
          system_(system) {}

    virtual void Send(std::unique_ptr<Message> m) {
      std::unique_lock<std::recursive_mutex> lock(*mutex_);
      if (queue_ != nullptr) {
        queue_->LockedPush(lock, std::move(m));
      }
    }

    virtual std::string Hostname();

    virtual int32_t Port();

    virtual std::string ReactorName();

    virtual std::string Name();

   private:
    std::shared_ptr<std::recursive_mutex> mutex_;
    std::string reactor_name_;
    std::string name_;
    EventQueue *queue_;
    System *system_;
  };

  /**
   * Implementation of the event stream.
   *
   * After the enclosing EventQueue object is destroyed (by a call to Close),
   * it is no longer legal to call any of the event stream methods.
   */
  class LocalEventStream : public EventStream {
   public:
    friend class EventQueue;

    LocalEventStream(std::shared_ptr<std::recursive_mutex> mutex, std::string name,
      EventQueue *queue) : mutex_(mutex), name_(name), queue_(queue) {}
    std::unique_ptr<Message> AwaitEvent() {
      std::unique_lock<std::recursive_mutex> lock(*mutex_);
      if (queue_ != nullptr) {
        return queue_->LockedAwaitPop(lock);
      }
      throw std::runtime_error(
          "Cannot call method after connector was closed.");
    }
    std::unique_ptr<Message> PopEvent() {
      std::unique_lock<std::recursive_mutex> lock(*mutex_);
      if (queue_ != nullptr) {
        return queue_->LockedPop(lock);
      }
      throw std::runtime_error(
          "Cannot call method after connector was closed.");
    }
    void OnEvent(EventStream::Callback callback) {
      std::unique_lock<std::recursive_mutex> lock(*mutex_);
      if (queue_ != nullptr) {
        queue_->LockedOnEvent(callback);
        return;
      }
      throw std::runtime_error(
          "Cannot call method after connector was closed.");
    }

    void Close();

   private:
    std::shared_ptr<std::recursive_mutex> mutex_;
    std::string name_;
    EventQueue *queue_;
  };

 private:
  std::unique_ptr<Message> LockedRawPop() {
    if (queue_.empty()) return nullptr;
    std::unique_ptr<Message> t = std::move(queue_.front());
    queue_.pop();
    return std::move(t);
  }

  /**
   * Should the owner close this EventQueue?
   *
   * Currently only checks if there are no more messages and all callbacks have unsubscribed?
   * This assumes the event loop has been started.
   */
  bool LockedCanBeClosed() {
    return callbacks_.empty() && queue_.empty();
  }

  void RemoveCbByUid(uint64_t uid) {
    std::unique_lock<std::recursive_mutex> lock(*mutex_);
    size_t num_erased = callbacks_.erase(uid);
    assert(num_erased == 1);

    // TODO(zuza): if no more callbacks, shut down the class (and the eventloop is started). First, figure out ownership of EventQueue?
  }

  System *system_;
  std::string name_;
  std::string reactor_name_;

  std::queue<std::unique_ptr<Message>> queue_;
  // Should only be locked once since it's used by a cond. var. Also caught in dctor, so must be recursive.
  std::shared_ptr<std::recursive_mutex> mutex_; 
  std::shared_ptr<std::condition_variable_any> cvar_;
  std::shared_ptr<LocalEventStream> stream_ =
    std::make_shared<LocalEventStream>(mutex_, name_, this);
  std::shared_ptr<LocalChannel> channel_ =
    std::make_shared<LocalChannel>(mutex_, reactor_name_, name_, this, system_);
  std::unordered_map<uint64_t, EventStream::Callback> callbacks_;
  uint64_t next_cb_uid = 0;
  
};

/**
 * Pair composed of read-end and write-end of a connection.
 */
using ConnectorT = std::pair<std::shared_ptr<EventStream>, std::shared_ptr<Channel>>;
using ChannelRefT = std::shared_ptr<Channel>;

/**
 * A single unit of concurrent execution in the system.
 * 
 * E.g. one worker, one client. Owned by System.
 */
class Reactor {
 public:
  friend class System;

  Reactor(System *system, std::string name)
      : system_(system), name_(name), main_(Open("main")) {}

  virtual ~Reactor() {}

  virtual void Run() = 0;

  ConnectorT Open(const std::string &s);
  ConnectorT Open();
  const std::shared_ptr<Channel> FindChannel(const std::string &channel_name);
  void Close(const std::string &s);

 protected:
  System *system_;
  std::string name_;
  /*
   * Locks all Reactor data, including all EventQueue's in connectors_.
   *
   * This should be a shared_ptr because LocalChannel can outlive Reactor.
   */
  std::shared_ptr<std::recursive_mutex> mutex_ =
      std::make_shared<std::recursive_mutex>();
  std::shared_ptr<std::condition_variable_any> cvar_ =
      std::make_shared<std::condition_variable_any>();
  std::unordered_map<std::string, EventQueue> connectors_;
  int64_t channel_name_counter_{0};
  ConnectorT main_;

 private:
  typedef std::pair<std::unique_ptr<Message>,
                    std::vector<std::pair<EventStream::Callback, EventStream::Subscription> > > MsgAndCbInfo;

  /**
   * Dispatches all waiting messages to callbacks. Shuts down when there are no callbacks left.
   */
  void RunEventLoop();

  void LockedCloseInternal(EventQueue& event_queue);

  // TODO: remove proof of locking evidence ?!
  MsgAndCbInfo LockedGetPendingMessages(std::unique_lock<std::recursive_mutex> &lock);
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
  SenderMessage(ChannelRefT sender) : sender_(sender) {}

  ChannelRefT sender() { return sender_; }

  template <class Archive>
  void serialize(Archive &ar) {
    ar(sender_);
  }

 private:
  ChannelRefT sender_;
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
  // TODO: Replace with a map to a reactor EventQueue map to have more granular
  // locking.
  std::unordered_map<std::string,
                     std::pair<std::unique_ptr<Reactor>, std::thread>>
      reactors_;
  Config config_;
  Network network_;
  Serialization serialization_;
};
