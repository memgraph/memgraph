#include "communication/reactor/reactor_local.hpp"

#include "utils/exceptions.hpp"

namespace communication::reactor {

void EventStream::Subscription::Unsubscribe() const {
  event_queue_.RemoveCallback(*this);
}

void EventStream::Subscription::CloseChannel() const { event_queue_.Close(); }

const std::string &EventStream::Subscription::channel_name() const {
  return event_queue_.channel_name_;
}

std::string Channel::LocalChannelWriter::ReactorName() const {
  return reactor_name_;
}

std::string Channel::LocalChannelWriter::Name() const { return channel_name_; }

void Channel::Close() {
  // TODO(zuza): there will be major problems if a reactor tries to close a
  // stream that isn't theirs luckily this should never happen if the framework
  // is used as expected.
  reactor_.CloseChannel(channel_name_);
}

std::pair<EventStream *, std::shared_ptr<ChannelWriter>> Reactor::Open(
    const std::string &channel_name) {
  std::unique_lock<std::mutex> lock(*mutex_);
  if (channels_.count(channel_name) != 0) {
    throw utils::BasicException("Channel with name " + channel_name +
                                "already exists");
  }
  auto it = channels_
                .emplace(channel_name,
                         std::make_shared<Channel>(Channel::Params{
                             name_, channel_name, mutex_, cvar_, *this}))
                .first;
  it->second->self_ptr_ = it->second;
  return make_pair(&it->second->stream_, it->second->LockedOpenChannel());
}

std::pair<EventStream *, std::shared_ptr<ChannelWriter>> Reactor::Open() {
  std::unique_lock<std::mutex> lock(*mutex_);
  do {
    std::string channel_name =
        "stream-" + std::to_string(channel_name_counter_++);
    if (channels_.count(channel_name) == 0) {
      auto it = channels_
                    .emplace(channel_name,
                             std::make_shared<Channel>(Channel::Params{
                                 name_, channel_name, mutex_, cvar_, *this}))
                    .first;
      it->second->self_ptr_ = it->second;
      return make_pair(&it->second->stream_, it->second->LockedOpenChannel());
    }
  } while (true);
}

std::shared_ptr<ChannelWriter> Reactor::FindChannel(
    const std::string &channel_name) {
  std::unique_lock<std::mutex> lock(*mutex_);
  auto it_channel = channels_.find(channel_name);
  if (it_channel == channels_.end()) return nullptr;
  return it_channel->second->LockedOpenChannel();
}

void Reactor::CloseChannel(const std::string &s) {
  std::unique_lock<std::mutex> lock(*mutex_);
  auto it = channels_.find(s);
  CHECK(it != channels_.end()) << "Trying to close nonexisting channel";
  channels_.erase(it);
  cvar_->notify_all();
}

void Reactor::RunEventLoop() {
  bool exit_event_loop = false;

  while (true) {
    // Find (or wait) for the next Message.
    PendingMessageInfo info;
    {
      std::unique_lock<std::mutex> guard(*mutex_);

      while (true) {
        // Not fair because was taken earlier, talk to lion.
        info = GetPendingMessages();
        if (info.message != nullptr) break;

        // Exit the loop if there are no more Channels.
        if (channels_.empty()) {
          exit_event_loop = true;
          break;
        }

        cvar_->wait(guard);
      }

      if (exit_event_loop) break;
    }

    for (auto &callback_info : info.callbacks) {
      callback_info.first(*info.message, callback_info.second);
    }
  }
}

/**
 * Checks if there is any nonempty EventStream.
 */
Reactor::PendingMessageInfo Reactor::GetPendingMessages() {
  for (auto &channels_key_value : channels_) {
    Channel &event_queue = *channels_key_value.second;
    auto message = event_queue.LockedPop();
    if (message == nullptr) continue;
    std::type_index type_index = message->GetTypeIndex();

    using Subscription = EventStream::Subscription;
    std::vector<std::pair<EventStream::Callback, Subscription>> callback_info;
    auto msg_type_cb_iter = event_queue.callbacks_.find(type_index);
    if (msg_type_cb_iter != event_queue.callbacks_.end()) {
      // There is a callback for this type.
      for (auto &type_index_cb_key_value : msg_type_cb_iter->second) {
        auto uid = type_index_cb_key_value.first;
        auto callback = type_index_cb_key_value.second;
        callback_info.emplace_back(callback,
                                   Subscription(event_queue, type_index, uid));
      }
    }

    return PendingMessageInfo{std::move(message), std::move(callback_info)};
  }

  return PendingMessageInfo{};
}
}
