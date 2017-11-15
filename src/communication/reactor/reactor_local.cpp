#include "communication/reactor/reactor_local.hpp"

#include <chrono>

#include "utils/exceptions.hpp"

namespace communication::reactor {

using namespace std::literals::chrono_literals;

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

void Channel::LocalChannelWriter::Send(std::unique_ptr<Message> m) {
  // Atomic, per the standard.  We guarantee here that if channel exists it
  // will not be destroyed by the end of this function.
  for (int i = 0; i < 2; ++i) {
    std::shared_ptr<Channel> queue = queue_.lock();
    // Check if cached queue exists and send message.
    if (queue) {
      queue->Push(std::move(m));
      break;
    }
    // If it doesn't exist. Check if there is a new channel with same name.
    auto new_channel = system_.FindChannel(reactor_name_, channel_name_);
    auto t =
        std::dynamic_pointer_cast<Channel::LocalChannelWriter>(new_channel);
    CHECK(t) << "t is of unexpected type";
    queue_ = t->queue_;
  }
}

std::string Channel::LocalChannelWriter::Name() const { return channel_name_; }

std::shared_ptr<Channel::LocalChannelWriter> Channel::LockedOpenChannel() {
  // TODO(zuza): fix this CHECK using this answer
  // https://stackoverflow.com/questions/45507041/how-to-check-if-weak-ptr-is-empty-non-assigned
  // TODO: figure out zuza's TODO. Does that mean this CHECK is kind of flaky
  // or that it doesn't fail sometimes, when it should.
  CHECK(!self_ptr_.expired());
  return std::make_shared<LocalChannelWriter>(reactor_name_, channel_name_,
                                              self_ptr_, reactor_.system_);
}

void Channel::Close() { reactor_.CloseChannel(channel_name_); }

Reactor::Reactor(ChannelFinder &system, const std::string &name,
                 const std::function<void(Reactor &)> &setup, System &system2)
    : system_(system),
      system2_(system2),
      name_(name),
      setup_(setup),
      main_(Open("main")),
      thread_([this] {
        setup_(*this);
        RunEventLoop();
        system2_.RemoveReactor(name_);
      }) {}

Reactor::~Reactor() {
  {
    std::unique_lock<std::mutex> guard(*mutex_);
    channels_.clear();
  }
  cvar_->notify_all();
  thread_.join();
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
  while (true) {
    // Find (or wait) for the next Message.
    PendingMessageInfo info;
    {
      std::unique_lock<std::mutex> guard(*mutex_);
      // Exit the loop if there are no more Channels.
      cvar_->wait_for(guard, 200ms, [&] {
        if (channels_.empty()) return true;
        info = GetPendingMessages();
        return static_cast<bool>(info.message);
      });
      if (channels_.empty()) break;
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
