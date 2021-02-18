#include "utils/signals.hpp"

namespace utils {

bool SignalIgnore(const Signal signal) {
  int signal_number = static_cast<int>(signal);
  struct sigaction action;
  // `sa_sigaction` must be cleared before `sa_handler` is set because on some
  // platforms the two are a union.
  action.sa_sigaction = nullptr;
  action.sa_handler = SIG_IGN;
  sigemptyset(&action.sa_mask);
  action.sa_flags = 0;
  if (sigaction(signal_number, &action, NULL) == -1) return false;
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
  if (sigaction(signal_number, &action, NULL) == -1) return false;
  return true;
}

std::map<int, std::function<void()>> SignalHandler::handlers_ = {};
}  // namespace utils
