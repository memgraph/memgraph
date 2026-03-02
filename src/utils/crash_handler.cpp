// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/crash_handler.hpp"

#include <execinfo.h>
#include <signal.h>
#include <unistd.h>

#include <array>
#include <cstddef>

namespace memgraph::utils {
namespace {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
volatile sig_atomic_t handling_crash = 0;

constexpr std::size_t kAlternateSignalStackSize = 64 * 1024;
alignas(std::max_align_t) thread_local std::array<std::byte, kAlternateSignalStackSize> alternate_stack_storage{};

template <std::size_t N>
void WriteRawLiteral(const char (&message)[N]) {
  static_assert(N > 0);
  (void)!write(STDERR_FILENO, message, N - 1);
}

void WriteSignalNumber(int signal) {
  char number_buffer[16]{};
  int pos = 0;
  unsigned int value = static_cast<unsigned int>(signal);
  do {
    number_buffer[pos++] = static_cast<char>('0' + (value % 10U));
    value /= 10U;
  } while (value != 0 && pos < static_cast<int>(sizeof(number_buffer)));

  for (int i = pos - 1; i >= 0; --i) {
    (void)!write(STDERR_FILENO, &number_buffer[i], 1);
  }
}

[[noreturn]] void CrashSignalHandler(int signal, siginfo_t *, void *) {
  if (handling_crash) _exit(128 + signal);
  handling_crash = 1;

  WriteRawLiteral("\nMemgraph fatal signal ");
  WriteSignalNumber(signal);
  WriteRawLiteral(" received. Stack trace:\n");

  void *frames[128];
  const int frame_count = backtrace(frames, static_cast<int>(std::size(frames)));
  backtrace_symbols_fd(frames, frame_count, STDERR_FILENO);

  WriteRawLiteral("End of stack trace.\n");

  struct sigaction default_action{};
  default_action.sa_handler = SIG_DFL;
  sigemptyset(&default_action.sa_mask);
  default_action.sa_flags = 0;
  (void)!sigaction(signal, &default_action, nullptr);
  sigset_t signal_set{};
  sigemptyset(&signal_set);
  sigaddset(&signal_set, signal);
  (void)!sigprocmask(SIG_UNBLOCK, &signal_set, nullptr);
  (void)!kill(getpid(), signal);
  _exit(128 + signal);
}

void InstallSignal(int signal) {
  struct sigaction action{};
  action.sa_sigaction = CrashSignalHandler;
  sigemptyset(&action.sa_mask);
  action.sa_flags = SA_SIGINFO | SA_RESETHAND | SA_ONSTACK;
  (void)!sigaction(signal, &action, nullptr);
}

void InstallAlternateSignalStack() {
  stack_t current_stack{};
  if (sigaltstack(nullptr, &current_stack) == 0 && !(current_stack.ss_flags & SS_DISABLE)) return;

  stack_t new_stack{};
  new_stack.ss_sp = alternate_stack_storage.data();
  new_stack.ss_size = alternate_stack_storage.size();
  new_stack.ss_flags = 0;

  (void)!sigaltstack(&new_stack, nullptr);
}

}  // namespace

void InstallCrashHandler() {
  InstallAlternateSignalStack();

  InstallSignal(SIGSEGV);
  InstallSignal(SIGABRT);
  InstallSignal(SIGBUS);
  InstallSignal(SIGILL);
  InstallSignal(SIGFPE);
}

}  // namespace memgraph::utils
