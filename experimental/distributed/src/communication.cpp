#include "communication.hpp"

void EventStream::Subscription::unsubscribe() {
  event_queue_.RemoveCbByUid(cb_uid_);
}

thread_local Reactor* current_reactor_ = nullptr;

std::string EventQueue::LocalChannel::Hostname() {
  return system_->network().Hostname();
}

int32_t EventQueue::LocalChannel::Port() {
  return system_->network().Port();
}

std::string EventQueue::LocalChannel::ReactorName() {
  return reactor_name_;
}

std::string EventQueue::LocalChannel::Name() {
  return name_;
}

void EventQueue::LocalEventStream::Close() {
  current_reactor_->Close(name_);
}

ConnectorT Reactor::Open(const std::string &channel_name) {
  std::unique_lock<std::recursive_mutex> lock(*mutex_);
  // TODO: Improve the check that the channel name does not exist in the
  // system.
  assert(connectors_.count(channel_name) == 0);
  auto it = connectors_.emplace(channel_name,
    EventQueue::Params{system_, name_, channel_name, mutex_, cvar_}).first;
  return ConnectorT(it->second.stream_, it->second.channel_);
}

ConnectorT Reactor::Open() {
  std::unique_lock<std::recursive_mutex> lock(*mutex_);
  do {
    std::string channel_name = "stream-" + std::to_string(channel_name_counter_++);
    if (connectors_.count(channel_name) == 0) {
      // EventQueue &queue = connectors_[channel_name];
      auto it = connectors_.emplace(channel_name,
        EventQueue::Params{system_, name_, channel_name, mutex_, cvar_}).first;
      return ConnectorT(it->second.stream_, it->second.channel_);
    }
  } while (true);
}

const std::shared_ptr<Channel> Reactor::FindChannel(
    const std::string &channel_name) {
  std::unique_lock<std::recursive_mutex> lock(*mutex_);
  auto it_connector = connectors_.find(channel_name);
  if (it_connector == connectors_.end()) return nullptr;
  return it_connector->second.channel_;
}

void Reactor::Close(const std::string &s) {
  std::unique_lock<std::recursive_mutex> lock(*mutex_);
  auto it = connectors_.find(s);
  assert(it != connectors_.end());
  LockedCloseInternal(it->second);
  connectors_.erase(it); // this calls the EventQueue destructor that catches the mutex, ugh.
}

void Reactor::LockedCloseInternal(EventQueue& event_queue) {
  // TODO(zuza): figure this out! @@@@
  std::cout << "Close Channel! Reactor name = " << name_ << " Channel name = " << event_queue.name_ << std::endl;
}

void Reactor::RunEventLoop() {
  std::cout << "event loop is run!" << std::endl;
  while (true) {
    // Clean up EventQueues without callbacks.
    {
      std::unique_lock<std::recursive_mutex> lock(*mutex_);
      for (auto connectors_it = connectors_.begin(); connectors_it != connectors_.end(); ) {
        EventQueue& event_queue = connectors_it->second;
        if (event_queue.LockedCanBeClosed()) {
          LockedCloseInternal(event_queue);
          connectors_it = connectors_.erase(connectors_it); // This removes the element from the collection.
        } else {
          ++connectors_it;
        }
      }
    }

    // Process and wait for events to dispatch.
    MsgAndCbInfo msgAndCb;
    {
      std::unique_lock<std::recursive_mutex> lock(*mutex_);

      // Exit the loop if there are no more EventQueues.
      if (connectors_.empty()) {
        return;
      }

      while (true) {
        msgAndCb = LockedGetPendingMessages(lock);
        if (msgAndCb.first != nullptr) break;
        cvar_->wait(lock);
      }
    }

    for (auto& cbAndSub : msgAndCb.second) {
      auto& cb = cbAndSub.first;
      const Message& msg = *msgAndCb.first;
      cb(msg, cbAndSub.second);
    }
  }
}

/**
 * Checks if there is any nonempty EventStream.
 */
auto Reactor::LockedGetPendingMessages(std::unique_lock<std::recursive_mutex> &lock) -> MsgAndCbInfo {
  // return type after because the scope Reactor:: is not searched before the name
  for (auto& connectors_key_value : connectors_) {
    EventQueue& event_queue = connectors_key_value.second;
    auto msg_ptr = event_queue.LockedPop(lock);
    if (msg_ptr == nullptr) continue;
    
    std::vector<std::pair<EventStream::Callback, EventStream::Subscription> > cb_info;
    for (auto& callbacks_key_value : event_queue.callbacks_) {
      uint64_t uid = callbacks_key_value.first;
      EventStream::Callback cb = callbacks_key_value.second;
      cb_info.emplace_back(cb, EventStream::Subscription(event_queue, uid));
    }
    return make_pair(std::move(msg_ptr), cb_info);
  }

  return MsgAndCbInfo(nullptr, {});
}

Network::Network(System *system) : system_(system),
  hostname_(system->config().GetString("hostname")),
  port_(system->config().GetInt("port")) {}
