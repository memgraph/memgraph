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

#include <csignal>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

namespace memgraph::utils {

// TODO: align bits so signals can be combined
//       Signal::Terminate | Signal::Interrupt
enum class Signal : int {
  Terminate = SIGTERM,
  SegmentationFault = SIGSEGV,
  Interrupt = SIGINT,
  Quit = SIGQUIT,
  Abort = SIGABRT,
  Pipe = SIGPIPE,
  BusError = SIGBUS,
  User1 = SIGUSR1,
  User2 = SIGUSR2,
};

/**
 * This function ignores a signal for the whole process. That means that a
 * signal that is ignored from any thread will be ignored in all threads.
 */
bool SignalIgnore(const Signal signal);

class SignalHandler {
 private:
  static std::map<int, std::function<void()>> handlers_;

  static void Handle(int signal);

 public:
  /// Install a signal handler.
  static bool RegisterHandler(Signal signal, std::function<void()> func);

  /// Like RegisterHandler, but takes a `signal_mask` argument for blocking
  /// signals during execution of the handler. `signal_mask` should be created
  /// using `sigemptyset` and `sigaddset` functions from `<signal.h>`.
  static bool RegisterHandler(Signal signal, std::function<void()> func, sigset_t signal_mask);
};
}  // namespace memgraph::utils
