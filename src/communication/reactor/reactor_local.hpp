#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <utility>

#include "cereal/types/memory.hpp"
#include "glog/logging.h"

namespace communication::reactor {

class EventStream;
class Reactor;
class System;
class Channel;

/**
 * Base class for messages.
 */
class Message {
 public:
  virtual ~Message() {}

  template <class Archive>
  void serialize(Archive &) {}

  /**
   * Run-time type identification that is used for callbacks.
   *
   * Warning: this works because of the virtual destructor, don't remove it from
   * this class
   */
  std::type_index GetTypeIndex() { return typeid(*this); }
};

/**
 * Write-end of a Channel (between two reactors).
 */
class ChannelWriter {
 public:
  ChannelWriter() = default;
  ChannelWriter(const ChannelWriter &) = delete;
  void operator=(const ChannelWriter &) = delete;
  ChannelWriter(ChannelWriter &&) = delete;
  void operator=(ChannelWriter &&) = delete;

  /**
   * Construct and send the message to the channel.
   */
  template <typename TMessage, typename... Args>
  void Send(Args &&... args) {
    Send(std::unique_ptr<Message>(
        std::make_unique<TMessage>(std::forward<Args>(args)...)));
  }

  virtual void Send(std::unique_ptr<Message> message) = 0;

  virtual std::string ReactorName() const = 0;
  virtual std::string Name() const = 0;
};

class ChannelFinder {
 public:
  virtual ~ChannelFinder() {}

  // Find local channel.
  virtual std::shared_ptr<ChannelWriter> FindChannel(
      const std::string &reactor_name, const std::string &channel_name) = 0;

  // Find remote channel.
  virtual std::shared_ptr<ChannelWriter> FindChannel(
      const std::string &address, uint16_t port,
      const std::string &reactor_name, const std::string &channel_name) = 0;
};

/**
 * Read-end of a Channel (between two reactors).
 */
class EventStream {
 public:
  class OnEventOnceChainer;
  class Subscription;

  /**
   * Register a callback that will be called whenever an event arrives.
   */
  template <typename TMessage>
  void OnEvent(
      std::function<void(const TMessage &, const Subscription &)> &&callback) {
    OnEventHelper(typeid(TMessage), [callback = std::move(callback)](
                                        const Message &base_message,
                                        const Subscription &subscription) {
      const auto &message = dynamic_cast<const TMessage &>(base_message);
      callback(message, subscription);
    });
  }

  /**
   * Register a callback that will be called only once.
   * Once event is received, channel of this EventStream is closed.
   */
  template <typename TMessage>
  void OnEventOnceThenClose(std::function<void(const TMessage &)> &&callback) {
    OnEventHelper(typeid(TMessage), [callback = std::move(callback)](
                                        const Message &base_message,
                                        const Subscription &subscription) {
      const TMessage &message = dynamic_cast<const TMessage &>(base_message);
      subscription.CloseChannel();
      callback(message);
    });
  }

  /**
   * Starts a chain to register a callback that fires off only once.
   *
   * This method supports chaining (see the the class OnEventOnceChainer or the
   * tests for examples).
   * Warning: when chaining callbacks, make sure that EventStream does not
   * deallocate before the last
   * chained callback fired.
   */
  OnEventOnceChainer OnEventOnce() { return OnEventOnceChainer(*this); }

  /**
   * Get the name of the channel.
   */
  virtual const std::string &ChannelName() = 0;

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
    void Unsubscribe() const;

    /**
     * Close the stream. Convenience method.
     */
    void CloseChannel() const;

    /**
     * Get the name of the channel the message is delivered to.
     */
    const std::string &channel_name() const;

   private:
    friend class Reactor;
    friend class Channel;

    Subscription(Channel &event_queue, std::type_index type_index,
                 uint64_t callback_id)
        : event_queue_(event_queue),
          type_index_(type_index),
          callback_id_(callback_id) {}

    Channel &event_queue_;
    std::type_index type_index_;
    uint64_t callback_id_;
  };

  /**
   * Close this event stream, disallowing further events from getting received.
   *
   * Any subsequent call after Close() to any function will be result in
   * undefined
   * behavior (invalid pointer dereference). Can only be called from the thread
   * associated with the Reactor.
   */
  virtual void Close() = 0;

  /**
   * Convenience class to chain one-off callbacks.
   *
   * Usage: Create this class with OnEventOnce() and then chain callbacks using
   * ChainOnce.
   * A callback will fire only once, unsubscribe and immediately subscribe the
   * next callback to the stream.
   *
   * Example: stream->OnEventOnce().ChainOnce(firstCb).ChainOnce(secondCb);
   *
   * Implementation: This class is a temporary object that remembers the
   * callbacks that are to be installed
   * and finally installs them in the destructor. Not sure is this kosher, is
   * there another way?
   */
  class OnEventOnceChainer {
   public:
    explicit OnEventOnceChainer(EventStream &event_stream)
        : event_stream_(event_stream) {}
    ~OnEventOnceChainer() { InstallCallbacks(); }

    template <typename TMessage>
    OnEventOnceChainer &ChainOnce(
        std::function<void(const TMessage &, const Subscription &)>
            &&callback) {
      std::function<void(const Message &, const Subscription &)>
          wrap = [callback = std::move(callback)](
              const Message &base_message, const Subscription &subscription) {
        const TMessage &message = dynamic_cast<const TMessage &>(base_message);
        subscription.Unsubscribe();
        // Warning: this can close the Channel, be careful what you put after
        // it!
        callback(message, subscription);
      };
      callbacks_.emplace_back(typeid(TMessage), std::move(wrap));
      return *this;
    }

   private:
    void InstallCallbacks() {
      int num_callbacks = callbacks_.size();
      CHECK(num_callbacks > 0) << "No callback will be installed";
      std::function<void(const Message &, const Subscription &)> next_callback;
      std::type_index next_type = typeid(nullptr);

      for (int i = num_callbacks - 1; i >= 0; --i) {
        std::function<void(const Message &, const Subscription &)>
            tmp_callback = [
              callback = std::move(callbacks_[i].second), next_type,
              next_callback = std::move(next_callback),
              event_stream = &this->event_stream_
            ](const Message &message, const Subscription &subscription) {
          callback(message, subscription);
          if (next_callback) {
            event_stream->OnEventHelper(next_type, std::move(next_callback));
          }
        };
        next_callback = std::move(tmp_callback);
        next_type = callbacks_[i].first;
      }

      event_stream_.OnEventHelper(next_type, std::move(next_callback));
    }

    EventStream &event_stream_;
    std::vector<
        std::pair<std::type_index,
                  std::function<void(const Message &, const Subscription &)>>>
        callbacks_;
  };

  typedef std::function<void(const Message &, const Subscription &)> Callback;

 private:
  virtual void OnEventHelper(std::type_index type_index, Callback callback) = 0;
};

/**
 * Implementation of a channel.
 *
 * This class is an internal data structure that represents the state of the
 * channel. This class is not meant to be used by the clients of the messaging
 * framework. The Channel class wraps the event queue data structure, the mutex
 * that protects concurrent access to the event queue, the local channel and the
 * event stream. The class is owned by the Reactor. It gets closed when the
 * owner reactor (the one that owns the read-end of a channel) removes/closes
 * it.
 */
class Channel {
  struct Params;

 public:
  friend class Reactor;  // to create a Params initialization object
  friend class EventStream::Subscription;

  explicit Channel(const Params &params)
      : channel_name_(params.channel_name),
        reactor_name_(params.reactor_name),
        mutex_(params.mutex),
        cvar_(params.cvar),
        stream_(mutex_, this),
        reactor_(params.reactor) {}

  /**
   * LocalChannelWriter represents the channels to reactors living in the same
   * reactor system (write-end of the channels).
   *
   * Sending messages to the local channel requires acquiring the mutex.
   * LocalChannelWriter holds a (weak) pointer to the enclosing Channel object.
   * Messages sent to a closed channel are ignored.
   * There can be multiple LocalChannelWriters refering to the same stream if
   * needed.
   *
   * It must outlive System.
   */
  class LocalChannelWriter : public ChannelWriter {
   public:
    friend class Channel;

    LocalChannelWriter(const std::string &reactor_name,
                       const std::string &channel_name,
                       const std::weak_ptr<Channel> &queue,
                       ChannelFinder &system)
        : reactor_name_(reactor_name),
          channel_name_(channel_name),
          queue_(queue),
          system_(system) {}

    void Send(std::unique_ptr<Message> m) override;
    std::string ReactorName() const override;
    std::string Name() const override;

   private:
    std::string reactor_name_;
    std::string channel_name_;
    std::weak_ptr<Channel> queue_;
    ChannelFinder &system_;
  };

  /**
   * Implementation of the event stream.
   *
   * After the enclosing Channel object is destroyed (by a call to CloseChannel
   * or Close).
   */
  class LocalEventStream : public EventStream {
   public:
    friend class Channel;

    LocalEventStream(const std::shared_ptr<std::mutex> &mutex, Channel *queue)
        : mutex_(mutex), queue_(queue) {}

    void OnEventHelper(std::type_index type_index, Callback callback) {
      std::unique_lock<std::mutex> lock(*mutex_);
      queue_->LockedOnEventHelper(type_index, callback);
    }

    const std::string &ChannelName() { return queue_->channel_name_; }

    void Close() { queue_->Close(); }

   private:
    std::shared_ptr<std::mutex> mutex_;
    std::string channel_name_;
    Channel *queue_;
  };

  /**
   * Close the channel. Must be called from the reactor that owns the channel.
   */
  void Close();

  Channel(const Channel &other) = delete;
  Channel(Channel &&other) = default;
  Channel &operator=(const Channel &other) = delete;
  Channel &operator=(Channel &&other) = default;

 private:
  /**
   * Initialization parameters to Channel.
   * Warning: do not forget to initialize self_ptr_ individually. Private
   * because it shouldn't be created outside of a Reactor.
   */
  struct Params {
    std::string reactor_name;
    std::string channel_name;
    std::shared_ptr<std::mutex> mutex;
    std::shared_ptr<std::condition_variable> cvar;
    Reactor &reactor;
  };

  void Push(std::unique_ptr<Message> m) {
    std::unique_lock<std::mutex> guard(*mutex_);
    queue_.emplace(std::move(m));
    // This is OK because there is only one Reactor (thread) that can wait on
    // this Channel.
    cvar_->notify_one();
  }

  std::shared_ptr<LocalChannelWriter> LockedOpenChannel();
  std::unique_ptr<Message> LockedPop() { return LockedRawPop(); }

  void LockedOnEventHelper(std::type_index type_index,
                           EventStream::Callback callback) {
    uint64_t callback_id = next_callback_id++;
    callbacks_[type_index][callback_id] = callback;
  }

  std::unique_ptr<Message> LockedRawPop() {
    if (queue_.empty()) return nullptr;
    std::unique_ptr<Message> t = std::move(queue_.front());
    queue_.pop();
    return t;
  }

  void RemoveCallback(const EventStream::Subscription &subscription) {
    std::unique_lock<std::mutex> lock(*mutex_);
    auto num_erased =
        callbacks_[subscription.type_index_].erase(subscription.callback_id_);
    CHECK(num_erased == 1) << "Expected to remove 1 element";
  }

  std::string channel_name_;
  std::string reactor_name_;
  std::queue<std::unique_ptr<Message>> queue_;
  // Should only be locked once since it's used by a cond. var. Also caught in
  // dctor, so must be recursive.
  std::shared_ptr<std::mutex> mutex_;
  std::shared_ptr<std::condition_variable> cvar_;

  /**
   * A weak_ptr to itself.
   *
   * There are initialization problems with this, check Params.
   */
  std::weak_ptr<Channel> self_ptr_;
  LocalEventStream stream_;
  Reactor &reactor_;
  std::unordered_map<std::type_index,
                     std::unordered_map<uint64_t, EventStream::Callback>>
      callbacks_;
  uint64_t next_callback_id = 0;
};

/**
 * A single unit of concurrent execution in the system.
 *
 * E.g. one worker, one client. Owned by System. Has a thread associated with
 * it.
 */
class Reactor {
  friend class System;

 public:
  Reactor(ChannelFinder &system, const std::string &name,
          const std::function<void(Reactor &)> &setup, System &system2);
  ~Reactor();

  std::pair<EventStream *, std::shared_ptr<ChannelWriter>> Open(
      const std::string &s);
  std::pair<EventStream *, std::shared_ptr<ChannelWriter>> Open();
  std::shared_ptr<ChannelWriter> FindChannel(const std::string &channel_name);

  /**
   * Close a channel by name.
   *
   * Should only be called from the Reactor thread.
   */
  void CloseChannel(const std::string &s);

  /**
   * Get Reactor name
   */
  const std::string &name() const { return name_; }

  Reactor(const Reactor &other) = delete;
  Reactor(Reactor &&other) = default;
  Reactor &operator=(const Reactor &other) = delete;
  Reactor &operator=(Reactor &&other) = default;

  ChannelFinder &system_;
  System &system2_;
  std::string name_;
  std::function<void(Reactor &)> setup_;

  /*
   * Locks all Reactor data, including all Channel's in channels_.
   *
   * This should be a shared_ptr because LocalChannelWriter can outlive Reactor.
   */
  std::shared_ptr<std::mutex> mutex_ = std::make_shared<std::mutex>();
  std::shared_ptr<std::condition_variable> cvar_ =
      std::make_shared<std::condition_variable>();

  /**
   * List of channels of a reactor indexed by name.
   */
  std::unordered_map<std::string, std::shared_ptr<Channel>> channels_;
  int64_t channel_name_counter_ = 0;
  // I don't understand why ChannelWriter is shared. ChannelWriter is just
  // endpoint that could be copied to every user.
  std::pair<EventStream *, std::shared_ptr<ChannelWriter>> main_;

 private:
  struct PendingMessageInfo {
    std::unique_ptr<Message> message;
    std::vector<std::pair<EventStream::Callback, EventStream::Subscription>>
        callbacks;
  };

  std::thread thread_;

  /**
   * Dispatches all waiting messages to callbacks. Shuts down when there are no
   * callbacks left.
   */
  void RunEventLoop();

  PendingMessageInfo GetPendingMessages();
};

/**
 * Placeholder for all reactors.
 * Make sure object of this class outlives all Reactors created by it.
 */
class System : public ChannelFinder {
 public:
  friend class Reactor;
  System() = default;

  std::unique_ptr<Reactor> Spawn(const std::string &name,
                                 std::function<void(Reactor &)> setup,
                                 ChannelFinder *finder = nullptr) {
    if (!finder) {
      finder = this;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    CHECK(reactors_.find(name) == reactors_.end())
        << "Reactor with name: '" << name << "' already exists.";
    auto reactor = std::make_unique<Reactor>(*finder, name, setup, *this);
    reactors_.emplace(name, reactor.get());
    return reactor;
  }

  std::shared_ptr<ChannelWriter> FindChannel(
      const std::string &reactor_name,
      const std::string &channel_name) override {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it_reactor = reactors_.find(reactor_name);
    if (it_reactor == reactors_.end())
      return std::shared_ptr<ChannelWriter>(new Channel::LocalChannelWriter(
          reactor_name, channel_name, {}, *this));
    return it_reactor->second->FindChannel(channel_name);
  }

  std::shared_ptr<ChannelWriter> FindChannel(const std::string &, uint16_t,
                                             const std::string &,
                                             const std::string &) override {
    // TODO: This is awful design, but at this point I just want to make
    // reactors work. We should templatize Reactor by system instead of dealing
    // with interfaces then System would spawn Reactor<System> and
    // DistributedSystem would spawn Reactor<DistributedSystem>.
    LOG(FATAL) << "Tried to resolve remote channel in local System";
  }

  void RemoveReactor(const std::string &name_) {
    std::unique_lock<std::mutex> guard(mutex_);
    auto it = reactors_.find(name_);
    CHECK(it != reactors_.end()) << "Trying to delete notexisting reactor";
    reactors_.erase(it);
  }

 private:
  System(const System &) = delete;
  System(System &&) = delete;
  System &operator=(const System &) = delete;
  System &operator=(System &&) = delete;

  std::mutex mutex_;
  std::unordered_map<std::string, Reactor *> reactors_;
};

using Subscription = Channel::LocalEventStream::Subscription;
}  // namespace communication::reactor
