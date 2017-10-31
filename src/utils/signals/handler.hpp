#include <csignal>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

using Function = std::function<void()>;

// TODO: align bits so signals can be combined
//       Signal::Terminate | Signal::Interupt
enum class Signal : int {
  Terminate = SIGTERM,
  SegmentationFault = SIGSEGV,
  Interupt = SIGINT,
  Quit = SIGQUIT,
  Abort = SIGABRT,
  BusError = SIGBUS,
};

class SignalHandler {
 private:
  static std::map<int, std::function<void()>> handlers_;

  static void Handle(int signal) { handlers_[signal](); }

 public:
  /// Install a signal handler.
  static bool RegisterHandler(Signal signal, Function func) {
    sigset_t signal_mask;
    sigemptyset(&signal_mask);
    return RegisterHandler(signal, func, signal_mask);
  }

  /// Like RegisterHandler, but takes a `signal_mask` argument for blocking
  /// signals during execution of the handler. `signal_mask` should be created
  /// using `sigemptyset` and `sigaddset` functions from `<signal.h>`.
  static bool RegisterHandler(Signal signal, Function func,
                              sigset_t signal_mask) {
    int signal_number = static_cast<int>(signal);
    handlers_[signal_number] = func;
    struct sigaction action;
    action.sa_handler = SignalHandler::Handle;
    action.sa_mask = signal_mask;
    action.sa_flags = SA_RESTART;
    if (sigaction(signal_number, &action, NULL) == -1) return false;
    return true;
  }
};

std::map<int, std::function<void()>> SignalHandler::handlers_ = {};
