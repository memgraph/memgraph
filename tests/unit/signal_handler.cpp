#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include <iostream>
#include <string>
#include <utility>

#include "utils/signals/handler.hpp"
#include "utils/stacktrace.hpp"

TEST_CASE("SignalHandler Segmentation Fault Test") {
  SignalHandler::register_handler(Signal::SegmentationFault, []() {
    std::cout << "Segmentation Fault" << std::endl;
    Stacktrace stacktrace;

    int size = 10;
    std::string message;
    for (int i = 0; i < size; i++) {
      message.append(fmt::format("\n at {} ({})", stacktrace[i].function,
                                 stacktrace[i].location));
    }
    std::cout << message << std::endl;

  });

  std::raise(SIGSEGV);
}
