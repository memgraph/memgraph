#include <csignal>
#include <functional>
#include <iostream>
#include <map>
#include <string>
#include <utility>
#include <vector>

using Function = std::function<void()>;

enum class Signal : int {
  Terminate = SIGTERM,
  SegmentationFault = SIGSEGV,
  Interupt = SIGINT,
  Quit = SIGQUIT,
  Abort = SIGABRT
};

class SignalHandler {
 private:
  static std::map<int, std::function<void()>> handlers_;

  static void handle(int signal) { handlers_[signal](); }

 public:
  static void register_handler(Signal signal, Function func) {
    int signal_number = static_cast<int>(signal);
    handlers_[signal_number] = func;
    std::signal(signal_number, SignalHandler::handle);
  }

  // TODO possible changes if signelton needed later
  /*
    static SignalHandler& instance() {
      static SignalHandler instance;
      return instance;
    }
  */
};

std::map<int, std::function<void()>> SignalHandler::handlers_ = {};
