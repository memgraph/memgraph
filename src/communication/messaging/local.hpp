#pragma once

#include <mutex>
#include <string>
#include <type_traits>
#include <typeindex>
#include <unordered_map>

#include "boost/serialization/access.hpp"

#include "data_structures/queue.hpp"

namespace communication::messaging {

/**
 * Base class for messages.
 */
class Message {
 public:
  virtual ~Message() {}

  /**
   * Run-time type identification that is used for callbacks.
   *
   * Warning: this works because of the virtual destructor, don't remove it from
   * this class
   */
  std::type_index type_index() const { return typeid(*this); }

 private:
  friend boost::serialization::access;

  template <class TArchive>
  void serialize(TArchive &, unsigned int) {}
};

class EventStream;
class LocalWriter;

class LocalSystem {
 public:
  friend class EventStream;
  friend class LocalWriter;

  LocalSystem() = default;
  LocalSystem(const LocalSystem &) = delete;
  LocalSystem(LocalSystem &&) = delete;
  LocalSystem &operator=(const LocalSystem &) = delete;
  LocalSystem &operator=(LocalSystem &&) = delete;

  std::shared_ptr<EventStream> Open(const std::string &name);

 private:
  std::shared_ptr<EventStream> Resolve(const std::string &name);

  void Remove(const std::string &name);

  std::mutex mutex_;
  std::unordered_map<std::string, std::weak_ptr<EventStream>> channels_;
};

class LocalWriter {
 public:
  LocalWriter(LocalSystem &system, const std::string &name)
      : system_(system), name_(name) {}
  LocalWriter(const LocalWriter &) = delete;
  void operator=(const LocalWriter &) = delete;
  LocalWriter(LocalWriter &&) = delete;
  void operator=(LocalWriter &&) = delete;

  template <typename TMessage, typename... Args>
  void Send(Args &&... args) {
    Send(std::unique_ptr<Message>(
        std::make_unique<TMessage>(std::forward<Args>(args)...)));
  }

  void Send(std::unique_ptr<Message> message);

 private:
  LocalSystem &system_;
  std::string name_;
};

class EventStream {
 public:
  friend class LocalWriter;
  friend class LocalSystem;

  EventStream(const EventStream &) = delete;
  void operator=(const EventStream &) = delete;
  EventStream(EventStream &&) = delete;
  void operator=(EventStream &&) = delete;
  ~EventStream();

  std::unique_ptr<Message> Poll();
  std::unique_ptr<Message> Await(
      std::chrono::system_clock::duration timeout =
          std::chrono::system_clock::duration::max());
  void Shutdown();

  const std::string &name() const { return name_; }

 private:
  EventStream(LocalSystem &system, const std::string &name)
      : system_(system), name_(name) {}

  void Push(std::unique_ptr<Message> message);

  LocalSystem &system_;
  std::string name_;
  Queue<std::unique_ptr<Message>> queue_;
};

}  // namespace communication::messaging
