#pragma once

#include "io/network/event_listener.hpp"

namespace io::network {

template <class Derived, class Stream, size_t max_events = 64,
          int wait_timeout = -1>
class StreamListener : public EventListener<Derived, max_events, wait_timeout> {
 public:
  using EventListener<Derived, max_events, wait_timeout>::EventListener;

  void Add(Stream &stream) {
    // add the stream to the event listener
    this->listener_.Add(stream.socket_, &stream.event_);
  }

  void OnCloseEvent(Epoll::Event &event) {
    this->derived().OnClose(to_stream(event));
  }

  void OnErrorEvent(Epoll::Event &event) {
    this->derived().OnError(to_stream(event));
  }

  void OnDataEvent(Epoll::Event &event) {
    this->derived().OnData(to_stream(event));
  }

  template <class... Args>
  void OnExceptionEvent(Epoll::Event &event, Args &&... args) {
    this->derived().OnException(to_stream(event), args...);
  }

 private:
  Stream &to_stream(Epoll::Event &event) {
    return *reinterpret_cast<Stream *>(event.data.ptr);
  }
};
}
