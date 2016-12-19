#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include <iostream>
#include <string>
#include <utility>

#include "utils/signals/handler.hpp"
#include "utils/stacktrace/stacktrace.hpp"

TEST_CASE("SignalHandler Segmentation Fault Test")
{
    SignalHandler::register_handler(Signal::SegmentationFault, []() {
        std::cout << "Segmentation Fault" << std::endl;
        Stacktrace stacktrace;
        std::cout << stacktrace.dump() << std::endl;
    });

    std::raise(SIGSEGV);
}
