// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <errno.h>
#include <fmt/format.h>
#include <malloc.h>
#include <sys/epoll.h>

#include "io/network/socket.hpp"
#include "utils/exceptions.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"

namespace memgraph::io::network {

/**
 * Wrapper class for epoll.
 * Creates an object that listens on file descriptor status changes.
 * see: man 4 epoll
 */
class Epoll {
 public:
  using Event = struct epoll_event;

  Epoll(bool set_cloexec = false) : epoll_fd_(epoll_create1(set_cloexec ? EPOLL_CLOEXEC : 0)) {
    // epoll_create1 returns an error if there is a logical error in our code
    // (for example invalid flags) or if there is irrecoverable error. In both
    // cases it is best to terminate.
    MG_ASSERT(epoll_fd_ != -1, "Error on epoll create: ({}) {}", errno, strerror(errno));
  }

  /**
   * This function adds/modifies a file descriptor to be listened for events.
   *
   * @param fd file descriptor to add to epoll
   * @param events epoll events mask
   * @param ptr pointer to the associated event handler
   * @param modify modify an existing file descriptor
   */
  void Add(int fd, uint32_t events, void *ptr, bool modify = false) {
    Event event;
    event.events = events;
    event.data.ptr = ptr;
    int status = epoll_ctl(epoll_fd_, (modify ? EPOLL_CTL_MOD : EPOLL_CTL_ADD), fd, &event);
    // epoll_ctl can return an error on our logical error or on irrecoverable
    // error. There is a third possibility that some system limit is reached. In
    // that case we could return an error and close connection. Chances of
    // reaching system limit in normally working memgraph is extremely unlikely,
    // so it is correct to terminate even in that case.
    MG_ASSERT(!status, "Error on epoll {}: ({}) {}", (modify ? "modify" : "add"), errno, strerror(errno));
  }

  /**
   * This function modifies a file descriptor that is listened for events.
   *
   * @param fd file descriptor to modify in epoll
   * @param events epoll events mask
   * @param ptr pointer to the associated event handler
   */
  void Modify(int fd, uint32_t events, void *ptr) { Add(fd, events, ptr, true); }

  /**
   * This function deletes a file descriptor that is listened for events.
   *
   * @param fd file descriptor to delete from epoll
   */
  void Delete(int fd) {
    int status = epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, NULL);
    // epoll_ctl can return an error on our logical error or on irrecoverable
    // error. There is a third possibility that some system limit is reached. In
    // that case we could return an error and close connection. Chances of
    // reaching system limit in normally working memgraph is extremely unlikely,
    // so it is correct to terminate even in that case.
    MG_ASSERT(!status, "Error on epoll delete: ({}) {}", errno, strerror(errno));
  }

  /**
   * This function waits for events from epoll.
   * It can be called from multiple threads, but should be used with care in
   * that case, see:
   * https://stackoverflow.com/questions/7058737/is-epoll-thread-safe
   *
   * @param fd file descriptor to delete from epoll
   */
  int Wait(Event *events, int max_events, int timeout) {
    auto num_events = epoll_wait(epoll_fd_, events, max_events, timeout);
    // If this check fails there was logical error in our code.
    MG_ASSERT(num_events != -1 || errno == EINTR, "Error on epoll wait: ({}) {}", errno, strerror(errno));
    // num_events can be -1 if errno was EINTR (epoll_wait interrupted by signal
    // handler). We treat that as no events, so we return 0.
    return num_events == -1 ? 0 : num_events;
  }

 private:
  const int epoll_fd_;
};
}  // namespace memgraph::io::network
