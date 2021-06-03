#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "integrations/kafka/consumer.hpp"

using namespace integrations::kafka;

TEST(Consumer, Trial) {
  ConsumerInfo info{
      [](const std::vector<Message> &messages) {}, "TestConsumer", "localhost:9092", {"t"}, "trial_group", {100}, {}};
  Consumer consumer{std::move(info)};
  spdlog::set_level(spdlog::level::info);
  consumer.Test(1, [](const std::vector<Message> &messages) {
    for (const auto &message : messages) {
      const auto payload = message.Payload();
      std::string_view payload_as_str(payload.begin(), payload.end());
      spdlog::info("Message received: {}", payload_as_str);
    }
  });
}
