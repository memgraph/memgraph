// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/signals.hpp"

namespace memgraph::utils {

bool SignalIgnore(const Signal signal) {
  int signal_number = static_cast<int>(signal);
  struct sigaction action;
  // `sa_sigaction` must be cleared before `sa_handler` is set because on some
  // platforms the two are a union.
  action.sa_sigaction = nullptr;
  action.sa_handler = SIG_IGN;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  if (sigaction(signal_number, &action, nullptr) == -1) return false;
  return true;
}

void SignalHandler::Handle(int signal) { handlers_[signal](); }

bool SignalHandler::RegisterHandler(Signal signal, std::function<void()> func) {
  sigset_t signal_mask;
  sigemptyset(&signal_mask);
  return RegisterHandler(signal, func, signal_mask);
}

bool SignalHandler::RegisterHandler(Signal signal, std::function<void()> func, sigset_t signal_mask) {
  int signal_number = static_cast<int>(signal);
  handlers_[signal_number] = func;
  struct sigaction action;
  // `sa_sigaction` must be cleared before `sa_handler` is set because on some
  // platforms the two are a union.
  action.sa_sigaction = nullptr;
  action.sa_handler = SignalHandler::Handle;
  action.sa_mask = signal_mask;
  action.sa_flags = SA_RESTART;
  if (sigaction(signal_number, &action, nullptr) == -1) return false;
  return true;
}

std::map<int, std::function<void()>> SignalHandler::handlers_ = {};
}  // namespace memgraph::utils
