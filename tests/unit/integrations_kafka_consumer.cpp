#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include <fmt/core.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "integrations/kafka/consumer.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "kafka_mock.hpp"
#include "utils/timer.hpp"

using namespace integrations::kafka;

namespace {
int SpanToInt(std::span<const char> span) {
  int result{0};
  if (span.size() != sizeof(int)) {
    std::runtime_error("Invalid span size");
  }
  std::memcpy(&result, span.data(), sizeof(int));
  return result;
}
}  // namespace

struct ConsumerTest : public ::testing::Test {
  ConsumerTest() {}

  ConsumerInfo CreateDefaultConsumerInfo() const {
    const auto test_name = std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()};
    return ConsumerInfo{
        .consumer_function = [](const std::vector<Message> &) {},
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
    auto last_received_message = std::make_shared<std::atomic<int>>(0);
    info.consumer_function = [weak_last_received_message = std::weak_ptr{last_received_message},
                              custom_consumer_function =
                                  std::move(custom_consumer_function)](const std::vector<Message> &messages) {
      auto last_received_message = weak_last_received_message.lock();
      if (last_received_message != nullptr) {
        *last_received_message = SpanToInt(messages.back().Payload());
      } else {
        custom_consumer_function(messages);
      }
    };

    auto consumer = std::make_unique<Consumer>(std::move(info));
    int sent_messages{1};
    SeedTopicWithInt(kTopicName, sent_messages);

    consumer->Start(std::nullopt);
    if (!consumer->IsRunning()) {
      return nullptr;
    }

    // Send messages to the topic until the consumer starts to receive them. In the first few seconds the consumer
    // doesn't get messages because there is no leader in the consumer group. If consumer group leader election timeout
    // could be lowered (didn't find anything in librdkafka docs), then this mechanism will become unnecessary.
    while (last_received_message->load() == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      SeedTopicWithInt(kTopicName, ++sent_messages);
    }

    while (last_received_message->load() != sent_messages) {
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
  constexpr std::string_view kMessage = "BatchIntervalTestMessage";
  auto info = CreateDefaultConsumerInfo();
  std::vector<std::pair<size_t, std::chrono::steady_clock::time_point>> received_timestamps{};
  info.batch_interval = kBatchInterval;
  auto expected_messages_received = true;
  info.consumer_function = [&](const std::vector<Message> &messages) mutable {
    received_timestamps.push_back({messages.size(), std::chrono::steady_clock::now()});
    for (const auto &message : messages) {
      expected_messages_received &= (kMessage == std::string_view(message.Payload().data(), message.Payload().size()));
    }
  };

  auto consumer = CreateConsumer(std::move(info));
  consumer->Start(std::nullopt);
  ASSERT_TRUE(consumer->IsRunning());

  constexpr auto kMessageCount = 7;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, kMessage);
    std::this_thread::sleep_for(kBatchInterval * 0.5);
  }

  consumer->Stop();
  EXPECT_TRUE(expected_messages_received) << "Some unexpected message have been received";

  auto check_received_timestamp = [&received_timestamps, kBatchInterval](size_t index) {
    SCOPED_TRACE("Checking index " + std::to_string(index));
    EXPECT_GE(index, 0) << "Cannot check first timestamp!";
    const auto message_count = received_timestamps[index].first;
    EXPECT_LE(1, message_count);

    auto actual_diff = std::chrono::duration_cast<std::chrono::milliseconds>(received_timestamps[index].second -
                                                                             received_timestamps[index - 1].second);
    constexpr auto kMinDiff = kBatchInterval * 0.9;
    constexpr auto kMaxDiff = kBatchInterval * 1.1;
    EXPECT_LE(kMinDiff.count(), actual_diff.count());
    EXPECT_GE(kMaxDiff.count(), actual_diff.count());
  };

  ASSERT_FALSE(received_timestamps.empty());

  EXPECT_TRUE(1 <= received_timestamps[0].first && received_timestamps[0].first <= 2);

  EXPECT_LE(3, received_timestamps.size());
  for (auto i = 1; i < received_timestamps.size(); ++i) {
    check_received_timestamp(i);
  }
}

TEST_F(ConsumerTest, StartStop) {
  Consumer consumer{CreateDefaultConsumerInfo()};

  auto start = [&consumer](const bool use_conditional) {
    if (use_conditional) {
      consumer.StartIfStopped();
    } else {
      consumer.Start(std::nullopt);
    }
  };

  auto stop = [&consumer](const bool use_conditional) {
    if (use_conditional) {
      consumer.StopIfRunning();
    } else {
      consumer.Stop();
    }
  };

  auto check_config = [&start, &stop, &consumer](const bool use_conditional_start,
                                                 const bool use_conditional_stop) mutable {
    SCOPED_TRACE(
        fmt::format("Conditional start {} and conditional stop {}", use_conditional_start, use_conditional_stop));
    EXPECT_FALSE(consumer.IsRunning());
    EXPECT_THROW(consumer.Stop(), ConsumerStoppedException);
    consumer.StopIfRunning();
    EXPECT_FALSE(consumer.IsRunning());

    start(use_conditional_start);
    EXPECT_TRUE(consumer.IsRunning());
    EXPECT_THROW(consumer.Start(std::nullopt), ConsumerRunningException);
    consumer.StartIfStopped();
    EXPECT_TRUE(consumer.IsRunning());

    stop(use_conditional_stop);
    EXPECT_FALSE(consumer.IsRunning());
  };

  constexpr auto kSimpleStart = false;
  constexpr auto kSimpleStop = false;
  constexpr auto kConditionalStart = true;
  constexpr auto kConditionalStop = true;

  check_config(kSimpleStart, kSimpleStop);
  check_config(kSimpleStart, kConditionalStop);
  check_config(kConditionalStart, kSimpleStop);
  check_config(kConditionalStart, kConditionalStop);
}

TEST_F(ConsumerTest, BatchSize) {
  // Increase default batch interval to give more time for messages to receive
  constexpr auto kBatchInterval = std::chrono::milliseconds{1000};
  constexpr auto kBatchSize = 3;
  auto info = CreateDefaultConsumerInfo();
  std::vector<std::pair<size_t, std::chrono::steady_clock::time_point>> received_timestamps{};
  info.batch_interval = kBatchInterval;
  info.batch_size = kBatchSize;
  constexpr std::string_view kMessage = "BatchSizeTestMessage";
  auto expected_messages_received = true;
  info.consumer_function = [&](const std::vector<Message> &messages) mutable {
    received_timestamps.push_back({messages.size(), std::chrono::steady_clock::now()});
    for (const auto &message : messages) {
      expected_messages_received &= (kMessage == std::string_view(message.Payload().data(), message.Payload().size()));
    }
  };

  auto consumer = CreateConsumer(std::move(info));
  consumer->Start(std::nullopt);
  ASSERT_TRUE(consumer->IsRunning());

  constexpr auto kLastBatchMessageCount = 1;
  constexpr auto kMessageCount = 3 * kBatchSize + kLastBatchMessageCount;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, kMessage);
  }
  std::this_thread::sleep_for(kBatchInterval * 2);
  consumer->Stop();
  EXPECT_TRUE(expected_messages_received) << "Some unexpected message have been received";

  auto check_received_timestamp = [&received_timestamps, kBatchInterval](size_t index, size_t expected_message_count) {
    SCOPED_TRACE("Checking index " + std::to_string(index));
    EXPECT_GE(index, 0) << "Cannot check first timestamp!";
    const auto message_count = received_timestamps[index].first;
    EXPECT_EQ(expected_message_count, message_count);

    auto actual_diff = std::chrono::duration_cast<std::chrono::milliseconds>(received_timestamps[index].second -
                                                                             received_timestamps[index - 1].second);
    if (expected_message_count == kBatchSize) {
      EXPECT_LE(actual_diff, kBatchInterval * 0.5);
    } else {
      constexpr auto kMinDiff = kBatchInterval * 0.9;
      constexpr auto kMaxDiff = kBatchInterval * 1.1;
      EXPECT_LE(kMinDiff.count(), actual_diff.count());
      EXPECT_GE(kMaxDiff.count(), actual_diff.count());
    }
  };

  ASSERT_FALSE(received_timestamps.empty());

  EXPECT_EQ(kBatchSize, received_timestamps[0].first);

  constexpr auto kExpectedBatchCount = kMessageCount / kBatchSize + 1;
  EXPECT_EQ(kExpectedBatchCount, received_timestamps.size());
  for (auto i = 1; i < received_timestamps.size() - 1; ++i) {
    check_received_timestamp(i, kBatchSize);
  }
  check_received_timestamp(received_timestamps.size() - 1, kLastBatchMessageCount);
}

TEST_F(ConsumerTest, InvalidBootstrapServers) {
  auto info = CreateDefaultConsumerInfo();
  info.bootstrap_servers = "non.existing.host:9092";
  EXPECT_THROW(Consumer(std::move(info)), ConsumerFailedToInitializeException);
}

TEST_F(ConsumerTest, InvalidTopic) {
  auto info = CreateDefaultConsumerInfo();
  info.topics = {"Non existing topic"};
  EXPECT_THROW(Consumer(std::move(info)), TopicNotFoundException);
}

TEST_F(ConsumerTest, StartsFromPreviousOffset) {
  constexpr auto kBatchSize = 1;
  auto info = CreateDefaultConsumerInfo();
  info.batch_size = kBatchSize;
  std::atomic<int> received_message_count{0};
  const std::string kMessagePrefix{"Message"};
  auto expected_messages_received = true;
  info.consumer_function = [&](const std::vector<Message> &messages) mutable {
    auto message_count = received_message_count.load();
    for (const auto &message : messages) {
      std::string message_payload = kMessagePrefix + std::to_string(message_count++);
      expected_messages_received &=
          (message_payload == std::string_view(message.Payload().data(), message.Payload().size()));
    }
    received_message_count = message_count;
  };

  // This test depends on CreateConsumer starts and stops the consumer, so the offset is stored
  auto consumer = CreateConsumer(std::move(info));
  ASSERT_FALSE(consumer->IsRunning());

  constexpr auto kMessageCount = 4;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, std::string_view{kMessagePrefix + std::to_string(sent_messages)});
  }

  auto do_batches = [&](int64_t batch_count) {
    SCOPED_TRACE(fmt::format("Already received messages: {}", received_message_count.load()));
    consumer->Start(batch_count);
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(consumer->IsRunning());
    constexpr auto kMaxWaitTime = std::chrono::seconds(5);

    while (consumer->IsRunning() && (std::chrono::steady_clock::now() - start) < kMaxWaitTime) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    // it is stopped because of limited batches
    ASSERT_FALSE(consumer->IsRunning());
  };

  ASSERT_NO_FATAL_FAILURE(do_batches(kMessageCount / 2));
  ASSERT_NO_FATAL_FAILURE(do_batches(kMessageCount / 2));

  EXPECT_TRUE(expected_messages_received) << "Some unexpected message have been received";
  EXPECT_EQ(received_message_count, kMessageCount);
}

TEST_F(ConsumerTest, TestMethodWorks) {
  constexpr auto kBatchSize = 1;
  auto info = CreateDefaultConsumerInfo();
  info.batch_size = kBatchSize;
  const std::string kMessagePrefix{"Message"};
  info.consumer_function = [](const std::vector<Message> &messages) mutable {};

  // This test depends on CreateConsumer starts and stops the consumer, so the offset is stored
  auto consumer = CreateConsumer(std::move(info));

  constexpr auto kMessageCount = 4;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, std::string_view{kMessagePrefix + std::to_string(sent_messages)});
  }

  // The test shouldn't commit the offsets, so it is possible to consume the same messages multiple times.
  auto check_test_method = [&]() {
    std::atomic<int> received_message_count{0};
    auto expected_messages_received = true;

    ASSERT_FALSE(consumer->IsRunning());

    consumer->Test(kMessageCount, [&](const std::vector<Message> &messages) mutable {
      auto message_count = received_message_count.load();
      for (const auto &message : messages) {
        std::string message_payload = kMessagePrefix + std::to_string(message_count++);
        expected_messages_received &=
            (message_payload == std::string_view(message.Payload().data(), message.Payload().size()));
      }
      received_message_count = message_count;
    });
    ASSERT_FALSE(consumer->IsRunning());

    EXPECT_TRUE(expected_messages_received) << "Some unexpected message have been received";
    EXPECT_EQ(received_message_count, kMessageCount);
  };

  {
    SCOPED_TRACE("First run");
    EXPECT_NO_FATAL_FAILURE(check_test_method());
  }
  {
    SCOPED_TRACE("Second run");
    EXPECT_NO_FATAL_FAILURE(check_test_method());
  }
}
