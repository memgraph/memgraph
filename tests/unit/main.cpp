#include <gtest/gtest.h>
#include <utils/logging.hpp>

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  logging::RedirectToStderr();
  spdlog::set_level(spdlog::level::trace);
  return RUN_ALL_TESTS();
}
