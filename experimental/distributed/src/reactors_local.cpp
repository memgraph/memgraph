#include "reactors_local.hpp"

void EventStream::Subscription::unsubscribe() const {
  event_queue_.RemoveCb(*this);
}

thread_local Reactor* current_reactor_ = nullptr;

std::string Channel::LocalChannelWriter::ReactorName() {
  return reactor_name_;
}

std::string Channel::LocalChannelWriter::Name() {
  return channel_name_;
}

void Channel::LocalEventStream::Close() {
  current_reactor_->CloseChannel(channel_name_);
}

std::pair<EventStream*, std::shared_ptr<ChannelWriter>> Reactor::Open(const std::string &channel_name) {
  std::unique_lock<std::mutex> lock(*mutex_);
  // TODO: Improve the check that the channel name does not exist in the
  // system.
  if (channels_.count(channel_name) != 0) {
    throw std::runtime_error("Channel with name " + channel_name
        + "already exists");
  }
  auto it = channels_.emplace(channel_name,
    std::make_shared<Channel>(Channel::Params{name_, channel_name, mutex_, cvar_})).first;
  it->second->self_ptr_ = it->second;
  return make_pair(&it->second->stream_, it->second->LockedOpenChannel());
}

std::pair<EventStream*, std::shared_ptr<ChannelWriter>> Reactor::Open() {
  std::unique_lock<std::mutex> lock(*mutex_);
  do {
    std::string channel_name = "stream-" + std::to_string(channel_name_counter_++);
    if (channels_.count(channel_name) == 0) {
      // Channel &queue = channels_[channel_name];
      auto it = channels_.emplace(channel_name,
        std::make_shared<Channel>(Channel::Params{name_, channel_name, mutex_, cvar_})).first;
      it->second->self_ptr_ = it->second;
      return make_pair(&it->second->stream_, it->second->LockedOpenChannel());
    }
  } while (true);
}

const std::shared_ptr<ChannelWriter> Reactor::FindChannel(
    const std::string &channel_name) {
  std::unique_lock<std::mutex> lock(*mutex_);
  auto it_channel = channels_.find(channel_name);
  if (it_channel == channels_.end()) return nullptr;
  return it_channel->second->LockedOpenChannel();
}

void Reactor::CloseChannel(const std::string &s) {
  std::unique_lock<std::mutex> lock(*mutex_);
  auto it = channels_.find(s);
  assert(it != channels_.end());
  channels_.erase(it);
}

void Reactor::CloseAllChannels() {
  std::unique_lock<std::mutex> lock(*mutex_);
  channels_.clear();
}

void Reactor::RunEventLoop() {
  bool exit_event_loop = false;

  while (true) {
    // Find (or wait) for the next Message.
    MsgAndCbInfo msg_and_cb;
    {
      std::unique_lock<std::mutex> lock(*mutex_);

      while (true) {

        // Exit the loop if there are no more Channels.
        if (channels_.empty()) {
          exit_event_loop = true;
          break;
        }

        // Not fair because was taken earlier, talk to lion.
        msg_and_cb = LockedGetPendingMessages();
        if (msg_and_cb.first != nullptr) break;

        cvar_->wait(lock);
      }

      if (exit_event_loop) break;
    }

    for (auto &cbAndSub : msg_and_cb.second) {
      auto &cb = cbAndSub.first;
      const Message &msg = *msg_and_cb.first;
      cb(msg, cbAndSub.second);
    }
  }
}

/**
 * Checks if there is any nonempty EventStream.
 */
auto Reactor::LockedGetPendingMessages() -> MsgAndCbInfo {
  // return type after because the scope Reactor:: is not searched before the name
  for (auto &channels_key_value : channels_) {
    Channel &event_queue = *channels_key_value.second;
    auto msg_ptr = event_queue.LockedPop();
    if (msg_ptr == nullptr) continue;
    std::type_index tidx = msg_ptr->GetTypeIndex();

    std::vector<std::pair<EventStream::Callback, EventStream::Subscription> > cb_info;
    auto msg_type_cb_iter = event_queue.callbacks_.find(tidx);
    if (msg_type_cb_iter != event_queue.callbacks_.end()) { // There is a callback for this type.
      for (auto &tidx_cb_key_value : msg_type_cb_iter->second) {
        uint64_t uid = tidx_cb_key_value.first;
        EventStream::Callback cb = tidx_cb_key_value.second;
        cb_info.emplace_back(cb, EventStream::Subscription(event_queue, tidx, uid));
      }
    }

    return MsgAndCbInfo(std::move(msg_ptr), std::move(cb_info));
  }

  return MsgAndCbInfo(nullptr, {});
}
