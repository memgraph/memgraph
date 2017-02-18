#pragma once

#include "io/network/event_listener.hpp"

namespace io {

template <class Derived, class Stream, size_t max_events = 64,
          int wait_timeout = -1>
class StreamListener : public EventListener<Derived, max_events, wait_timeout> {
 public:
  using EventListener<Derived, max_events, wait_timeout>::EventListener;

  void add(Stream &stream) {
    // add the stream to the event listener
    this->listener.add(stream.socket, &stream.event);
  }

  void on_close_event(Epoll::Event &event) {
    this->derived().on_close(to_stream(event));
  }

  void on_error_event(Epoll::Event &event) {
    this->derived().on_error(to_stream(event));
  }

  void on_data_event(Epoll::Event &event) {
    this->derived().on_data(to_stream(event));
  }

  template <class... Args>
  void on_exception_event(Epoll::Event &event, Args &&... args) {
    this->derived().on_exception(to_stream(event), args...);
  }

 private:
  Stream &to_stream(Epoll::Event &event) {
    return *reinterpret_cast<Stream *>(event.data.ptr);
  }
};
}
