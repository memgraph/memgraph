#include "communication/messaging/local.hpp"

#include "fmt/format.h"
#include "glog/logging.h"

namespace communication::messaging {

std::shared_ptr<EventStream> LocalSystem::Open(const std::string &name) {
  std::unique_lock<std::mutex> guard(mutex_);
  // TODO: It would be better to use std::make_shared here, but we can't since
  // constructor is private.
  std::shared_ptr<EventStream> stream(new EventStream(*this, name));
  auto got = channels_.emplace(name, stream);
  CHECK(got.second) << fmt::format("Stream with name {} already exists", name);
  return stream;
}

std::shared_ptr<EventStream> LocalSystem::Resolve(const std::string &name) {
  std::unique_lock<std::mutex> guard(mutex_);
  auto it = channels_.find(name);
  if (it == channels_.end()) return nullptr;
  return it->second.lock();
}

void LocalSystem::Remove(const std::string &name) {
  std::unique_lock<std::mutex> guard(mutex_);
  auto it = channels_.find(name);
  CHECK(it != channels_.end()) << "Trying to delete nonexisting stream";
  channels_.erase(it);
}

void LocalWriter::Send(std::unique_ptr<Message> message) {
  // TODO: We could add caching to LocalWriter so that we don't need to acquire
  // lock on system every time we want to send a message. This can be
  // accomplished by storing weak_ptr to EventStream.
  auto stream = system_.Resolve(name_);
  if (!stream) return;
  stream->Push(std::move(message));
}

EventStream::~EventStream() { system_.Remove(name_); }

std::unique_ptr<Message> EventStream::Poll() {
  auto opt_message = queue_.MaybePop();
  if (opt_message == std::experimental::nullopt) return nullptr;
  return std::move(*opt_message);
}

void EventStream::Push(std::unique_ptr<Message> message) {
  queue_.Push(std::move(message));
}

std::unique_ptr<Message> EventStream::Await(
    std::chrono::system_clock::duration timeout) {
  auto opt_message = queue_.AwaitPop(timeout);
  if (opt_message == std::experimental::nullopt) return nullptr;
  return std::move(*opt_message);
};

void EventStream::Shutdown() { queue_.Shutdown(); }
}
