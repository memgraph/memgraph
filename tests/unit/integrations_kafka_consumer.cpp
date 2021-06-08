#include <chrono>
#include <optional>
#include <string>
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

namespace {
std::span<const char> IntToSpan(int &value) { return std::span{reinterpret_cast<const char *>(&value), sizeof(int)}; }
int SpanToInt(std::span<const char> span) {
  int result{0};
  // ignore if span is shorter than sizeof(int)
  std::memcpy(&result, span.data(), sizeof(int));
  return result;
}
}  // namespace

struct ConsumerTest : public ::testing::Test {
  ConsumerTest() {}

  ConsumerInfo CreatePartialConsumerInfo() {
    const auto test_name = std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()};
    return ConsumerInfo{
        .consumer_function = {},
        .consumer_name = "Consumer" + test_name,
        .bootstrap_servers = cluster.Bootstraps(),
        .topics = {kTopicName},
        .consumer_group = "ConsumerGroup " + test_name,
        .batch_interval = std::nullopt,
        .batch_size = std::nullopt,
    };
  };

  std::unique_ptr<Consumer> CreateConsumer(ConsumerInfo &&info) {
    auto custom_consumer_function = std::move(info.consumer_function);
    auto last_message = std::make_shared<std::atomic<int>>(0);
    info.consumer_function = [weak_last_message = std::weak_ptr{last_message},
                              custom_consumer_function =
                                  std::move(custom_consumer_function)](const std::vector<Message> &messages) {
      auto last_message = weak_last_message.lock();
      if (last_message != nullptr) {
        *last_message = SpanToInt(messages.back().Payload());
      } else {
        custom_consumer_function(messages);
      }
    };

    auto consumer = std::make_unique<Consumer>(std::move(info));
    int sent_messages{1};
    cluster.SeedTopic(kTopicName, IntToSpan(sent_messages));

    consumer->Start(std::nullopt);
    if (!consumer->IsRunning()) {
      return nullptr;
    }

    while (last_message->load() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      sent_messages++;
      cluster.SeedTopic(kTopicName, IntToSpan(sent_messages));
    }
    while (last_message->load() != sent_messages) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    };
    consumer->Stop();
    std::this_thread::sleep_for(std::chrono::seconds(4));
    return consumer;
  }

  void SeedTopicWithInt(const std::string &topic_name, int value) {
    std::array<char, sizeof(int)> int_as_char{};
    std::memcpy(int_as_char.data(), &value, int_as_char.size());

    cluster.SeedTopic(topic_name, int_as_char);
  }

  static const std::string kTopicName;
  KafkaClusterMock cluster{{kTopicName}};
};

const std::string ConsumerTest::kTopicName{"FirstTopic"};

TEST_F(ConsumerTest, BatchInterval) {
  // There might be ~300ms delay in message delivery with librdkafka mock, thus the batch interval cannot be too small.
  constexpr auto kBatchInterval = std::chrono::milliseconds{500};
  auto info = CreatePartialConsumerInfo();
  std::vector<std::pair<size_t, std::chrono::steady_clock::time_point>> received_timestamps{};
  received_timestamps.reserve(16);
  info.batch_interval = kBatchInterval;
  constexpr std::string_view kMessage = "BatchIntervalTestMessage";
  info.consumer_function = [&](const std::vector<Message> &messages) mutable {
    received_timestamps.push_back({messages.size(), std::chrono::steady_clock::now()});
  };

  auto consumer = CreateConsumer(std::move(info));
  consumer->Start(std::nullopt);
  ASSERT_TRUE(consumer->IsRunning());

  constexpr auto kMessageCount = 7;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, kMessage);
    std::this_thread::sleep_for(kBatchInterval * 0.5);
  }

  auto check_received_timestamp = [&received_timestamps, kBatchInterval](size_t index) {
    SCOPED_TRACE("Checking index " + std::to_string(index));
    EXPECT_GE(index, 0);
    const auto message_count = received_timestamps[index].first;
    EXPECT_LE(1, message_count);

    auto actual_diff = std::chrono::duration_cast<std::chrono::milliseconds>(received_timestamps[index].second -
                                                                             received_timestamps[index - 1].second);
    const auto min_diff = kBatchInterval * 0.9;
    const auto max_diff = kBatchInterval * 1.1;
    EXPECT_LE(min_diff.count(), actual_diff.count());
    EXPECT_GE(max_diff.count(), actual_diff.count());
  };

  ASSERT_FALSE(received_timestamps.empty());

  EXPECT_TRUE(1 <= received_timestamps[0].first && received_timestamps[0].first <= 2);

  EXPECT_LE(3, received_timestamps.size());
  for (auto i = 1; i < received_timestamps.size(); ++i) {
    check_received_timestamp(i);
  }
}
