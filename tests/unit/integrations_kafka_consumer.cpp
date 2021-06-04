#include <chrono>
#include <string_view>
#include <thread>

#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "integrations/kafka/consumer.hpp"
#include "kafka_mock.hpp"
#include "utils/timer.hpp"

using namespace integrations::kafka;

TEST(Consumer, Trial) {
  using namespace std::literals;
  spdlog::set_level(spdlog::level::info);
  const std::vector<std::string> kTopicNames{"Toopic"};
  KafkaClusterMock cluster{kTopicNames};
  cluster.SeedTopic(kTopicNames.front(), "need a message to initialize the topic on the broker");

  const std::array expected_messages{"first"sv, "second"sv, "third"sv};
  size_t last_received_message = 0;
  ConsumerInfo info{[&expected_messages, &last_received_message](const std::vector<Message> &messages) {
                      for (const auto &message : messages) {
                        const auto payload = message.Payload();
                        std::string_view payload_as_str(payload.begin(), payload.end());
                        spdlog::info("Message received: {}", payload_as_str);
                        if (last_received_message < expected_messages.size() &&
                            expected_messages[last_received_message] == payload_as_str) {
                          last_received_message++;
                        }
                      }
                    },
                    "TestConsumer",
                    cluster.Bootstraps(),
                    kTopicNames,
                    "TrialGroup",
                    {10},
                    {}};
  Consumer consumer{std::move(info)};
  // TODO(antaljanosbenjamin) figure out why this sleep is necessary
  std::this_thread::sleep_for(std::chrono::seconds(5));
  consumer.Start(std::nullopt);
  ASSERT_TRUE(consumer.IsRunning());

  utils::Timer timer;
  while (last_received_message < expected_messages.size() && timer.Elapsed<std::chrono::seconds>().count() < 5) {
    for (const auto message : expected_messages) {
      cluster.SeedTopic(kTopicNames[0], message);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_EQ(last_received_message, expected_messages.size());
}
