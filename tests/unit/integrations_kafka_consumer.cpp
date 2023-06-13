// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <fmt/core.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>

#include "integrations/kafka/consumer.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "kafka_mock.hpp"
#include "utils/string.hpp"

using namespace memgraph::integrations::kafka;

namespace {
const auto kDummyConsumerFunction = [](const auto & /*messages*/) {};
inline constexpr std::optional<std::chrono::milliseconds> kDontCareTimeout = std::nullopt;

int SpanToInt(std::span<const char> span) {
  int result{0};
  if (span.size() != sizeof(int)) {
    std::runtime_error("Invalid span size");
  }
  std::memcpy(&result, span.data(), sizeof(int));
  return result;
}

inline constexpr std::chrono::milliseconds kDefaultBatchInterval{100};
inline constexpr int64_t kDefaultBatchSize{1000};

}  // namespace

struct ConsumerTest : public ::testing::Test {
  ConsumerTest() {}

  ConsumerInfo CreateDefaultConsumerInfo() const {
    const auto test_name = std::string{::testing::UnitTest::GetInstance()->current_test_info()->name()};
    return ConsumerInfo{
        .consumer_name = "Consumer" + test_name,
        .topics = {kTopicName},
        .consumer_group = "ConsumerGroup " + test_name,
        .bootstrap_servers = cluster.Bootstraps(),
        .batch_interval = kDefaultBatchInterval,
        .batch_size = kDefaultBatchSize,
        .public_configs = {},
        .private_configs = {},
    };
  };

  std::unique_ptr<Consumer> CreateConsumer(ConsumerInfo &&info, ConsumerFunction consumer_function) {
    EXPECT_EQ(1, info.topics.size());
    EXPECT_EQ(info.topics.at(0), kTopicName);
    auto last_received_message = std::make_shared<std::atomic<int>>(0);
    const auto consumer_function_wrapper = [weak_last_received_message = std::weak_ptr{last_received_message},
                                            consumer_function =
                                                std::move(consumer_function)](const std::vector<Message> &messages) {
      auto last_received_message = weak_last_received_message.lock();

      EXPECT_FALSE(messages.empty());
      for (const auto &message : messages) {
        EXPECT_EQ(message.TopicName(), kTopicName);
      }
      if (last_received_message != nullptr) {
        *last_received_message = SpanToInt(messages.back().Payload());
      } else {
        consumer_function(messages);
      }
    };

    auto consumer = std::make_unique<Consumer>(std::move(info), std::move(consumer_function_wrapper));
    int sent_messages{1};
    SeedTopicWithInt(kTopicName, sent_messages);

    consumer->Start();
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
  static constexpr auto kBatchInterval = std::chrono::milliseconds{500};
  static constexpr std::string_view kMessage = "BatchIntervalTestMessage";
  auto info = CreateDefaultConsumerInfo();
  std::vector<std::pair<size_t, std::chrono::steady_clock::time_point>> received_timestamps{};
  info.batch_interval = kBatchInterval;
  auto expected_messages_received = true;
  auto consumer_function = [&](const std::vector<Message> &messages) mutable {
    received_timestamps.push_back({messages.size(), std::chrono::steady_clock::now()});
    for (const auto &message : messages) {
      expected_messages_received &= (kMessage == std::string_view(message.Payload().data(), message.Payload().size()));
    }
  };

  auto consumer = CreateConsumer(std::move(info), std::move(consumer_function));
  consumer->Start();
  ASSERT_TRUE(consumer->IsRunning());

  static constexpr auto kMessageCount = 7;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, kMessage);
    std::this_thread::sleep_for(kBatchInterval * 0.5);
  }

  consumer->Stop();
  EXPECT_TRUE(expected_messages_received) << "Some unexpected message has been received";

  auto check_received_timestamp = [&received_timestamps](size_t index) {
    SCOPED_TRACE("Checking index " + std::to_string(index));
    EXPECT_GE(index, 0) << "Cannot check first timestamp!";
    const auto message_count = received_timestamps[index].first;
    EXPECT_LE(1, message_count);

    auto actual_diff = std::chrono::duration_cast<std::chrono::milliseconds>(received_timestamps[index].second -
                                                                             received_timestamps[index - 1].second);
    static constexpr auto kMinDiff = kBatchInterval * 0.9;
    static constexpr auto kMaxDiff = kBatchInterval * 1.1;
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
  Consumer consumer{CreateDefaultConsumerInfo(), kDummyConsumerFunction};

  auto stop = [&consumer](const bool use_conditional) {
    if (use_conditional) {
      consumer.StopIfRunning();
    } else {
      consumer.Stop();
    }
  };

  auto check_config = [&stop, &consumer](const bool use_conditional_stop) mutable {
    SCOPED_TRACE(fmt::format("Start and conditionally stop {}", use_conditional_stop));
    EXPECT_FALSE(consumer.IsRunning());
    EXPECT_THROW(consumer.Stop(), ConsumerStoppedException);
    consumer.StopIfRunning();
    EXPECT_FALSE(consumer.IsRunning());

    consumer.Start();
    EXPECT_TRUE(consumer.IsRunning());
    EXPECT_THROW(consumer.Start(), ConsumerRunningException);

    EXPECT_TRUE(consumer.IsRunning());

    stop(use_conditional_stop);
    EXPECT_FALSE(consumer.IsRunning());
  };

  static constexpr auto kSimpleStop = false;
  static constexpr auto kConditionalStop = true;

  check_config(kSimpleStop);
  check_config(kConditionalStop);
}

TEST_F(ConsumerTest, BatchSize) {
  // Increase default batch interval to give more time for messages to receive
  static constexpr auto kBatchInterval = std::chrono::milliseconds{1000};
  static constexpr auto kBatchSize = 3;
  auto info = CreateDefaultConsumerInfo();
  std::vector<std::pair<size_t, std::chrono::steady_clock::time_point>> received_timestamps{};
  info.batch_interval = kBatchInterval;
  info.batch_size = kBatchSize;
  static constexpr std::string_view kMessage = "BatchSizeTestMessage";
  auto expected_messages_received = true;
  auto consumer_function = [&](const std::vector<Message> &messages) mutable {
    received_timestamps.push_back({messages.size(), std::chrono::steady_clock::now()});
    for (const auto &message : messages) {
      expected_messages_received &= (kMessage == std::string_view(message.Payload().data(), message.Payload().size()));
    }
  };

  auto consumer = CreateConsumer(std::move(info), std::move(consumer_function));
  consumer->Start();
  ASSERT_TRUE(consumer->IsRunning());

  static constexpr auto kLastBatchMessageCount = 1;
  static constexpr auto kMessageCount = 3 * kBatchSize + kLastBatchMessageCount;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, kMessage);
  }
  std::this_thread::sleep_for(kBatchInterval * 2);
  consumer->Stop();
  EXPECT_TRUE(expected_messages_received) << "Some unexpected message has been received";

  auto check_received_timestamp = [&received_timestamps](size_t index, size_t expected_message_count) {
    SCOPED_TRACE("Checking index " + std::to_string(index));
    EXPECT_GE(index, 0) << "Cannot check first timestamp!";
    const auto message_count = received_timestamps[index].first;
    EXPECT_EQ(expected_message_count, message_count);

    auto actual_diff = std::chrono::duration_cast<std::chrono::milliseconds>(received_timestamps[index].second -
                                                                             received_timestamps[index - 1].second);
    if (expected_message_count == kBatchSize) {
      EXPECT_LE(actual_diff, kBatchInterval * 0.5);
    } else {
      static constexpr auto kMinDiff = kBatchInterval * 0.9;
      static constexpr auto kMaxDiff = kBatchInterval * 1.1;
      EXPECT_LE(kMinDiff.count(), actual_diff.count());
      EXPECT_GE(kMaxDiff.count(), actual_diff.count());
    }
  };

  ASSERT_FALSE(received_timestamps.empty());

  EXPECT_EQ(kBatchSize, received_timestamps[0].first);

  static constexpr auto kExpectedBatchCount = kMessageCount / kBatchSize + 1;
  EXPECT_EQ(kExpectedBatchCount, received_timestamps.size());
  for (auto i = 1; i < received_timestamps.size() - 1; ++i) {
    check_received_timestamp(i, kBatchSize);
  }
  check_received_timestamp(received_timestamps.size() - 1, kLastBatchMessageCount);
}

TEST_F(ConsumerTest, InvalidBootstrapServers) {
  auto info = CreateDefaultConsumerInfo();
  info.bootstrap_servers = "non.existing.host:9092";

  EXPECT_THROW(Consumer(std::move(info), kDummyConsumerFunction), ConsumerFailedToInitializeException);
}

TEST_F(ConsumerTest, InvalidTopic) {
  auto info = CreateDefaultConsumerInfo();
  info.topics = {"Nonexistenttopic"};
  EXPECT_THROW(Consumer(std::move(info), kDummyConsumerFunction), TopicNotFoundException);
}

TEST_F(ConsumerTest, InvalidBatchInterval) {
  auto info = CreateDefaultConsumerInfo();

  info.batch_interval = std::chrono::milliseconds{0};
  EXPECT_THROW(Consumer(info, kDummyConsumerFunction), ConsumerFailedToInitializeException);

  info.batch_interval = std::chrono::milliseconds{-1};
  EXPECT_THROW(Consumer(info, kDummyConsumerFunction), ConsumerFailedToInitializeException);

  info.batch_interval = std::chrono::milliseconds{1};
  EXPECT_NO_THROW(Consumer(info, kDummyConsumerFunction));
}

TEST_F(ConsumerTest, InvalidBatchSize) {
  auto info = CreateDefaultConsumerInfo();

  info.batch_size = 0;
  EXPECT_THROW(Consumer(info, kDummyConsumerFunction), ConsumerFailedToInitializeException);

  info.batch_size = -1;
  EXPECT_THROW(Consumer(info, kDummyConsumerFunction), ConsumerFailedToInitializeException);

  info.batch_size = 1;
  EXPECT_NO_THROW(Consumer(info, kDummyConsumerFunction));
}

TEST_F(ConsumerTest, DISABLED_StartsFromPreviousOffset) {
  static constexpr auto kBatchSize = 1;
  auto info = CreateDefaultConsumerInfo();
  info.batch_size = kBatchSize;
  std::atomic<int> received_message_count{0};
  const std::string kMessagePrefix{"Message"};
  auto expected_messages_received = true;
  auto consumer_function = [&](const std::vector<Message> &messages) mutable {
    auto message_count = received_message_count.load();
    EXPECT_EQ(messages.size(), 1);
    for (const auto &message : messages) {
      std::string message_payload = kMessagePrefix + std::to_string(message_count++);
      expected_messages_received &=
          (message_payload == std::string_view(message.Payload().data(), message.Payload().size()));
    }
    received_message_count = message_count;
  };

  {
    // This test depends on CreateConsumer starts and stops the consumer, so the offset is stored
    auto consumer = CreateConsumer(ConsumerInfo{info}, consumer_function);
    ASSERT_FALSE(consumer->IsRunning());
  }

  auto send_and_consume_messages = [&](int batch_count) {
    SCOPED_TRACE(fmt::format("Already received messages: {}", received_message_count.load()));

    for (auto sent_messages = 0; sent_messages < batch_count; ++sent_messages) {
      cluster.SeedTopic(kTopicName,
                        std::string_view{kMessagePrefix + std::to_string(received_message_count + sent_messages)});
    }
    auto expected_total_messages = received_message_count + batch_count;
    auto consumer = std::make_unique<Consumer>(ConsumerInfo{info}, consumer_function);
    ASSERT_FALSE(consumer->IsRunning());
    consumer->Start();
    const auto start = std::chrono::steady_clock::now();
    ASSERT_TRUE(consumer->IsRunning());

    static constexpr auto kMaxWaitTime = std::chrono::seconds(5);

    while (expected_total_messages != received_message_count.load() &&
           (std::chrono::steady_clock::now() - start) < kMaxWaitTime) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    // it is stopped because of limited batches
    EXPECT_EQ(expected_total_messages, received_message_count);
    EXPECT_NO_THROW(consumer->Stop());
    ASSERT_FALSE(consumer->IsRunning());
    EXPECT_TRUE(expected_messages_received) << "Some unexpected message has been received";
  };

  ASSERT_NO_FATAL_FAILURE(send_and_consume_messages(2));
  ASSERT_NO_FATAL_FAILURE(send_and_consume_messages(2));
}

TEST_F(ConsumerTest, CheckMethodWorks) {
  static constexpr auto kBatchSize = 1;
  auto info = CreateDefaultConsumerInfo();
  info.batch_size = kBatchSize;
  const std::string kMessagePrefix{"Message"};

  // This test depends on CreateConsumer starts and stops the consumer, so the offset is stored
  auto consumer = CreateConsumer(std::move(info), kDummyConsumerFunction);

  static constexpr auto kMessageCount = 4;
  for (auto sent_messages = 0; sent_messages < kMessageCount; ++sent_messages) {
    cluster.SeedTopic(kTopicName, std::string_view{kMessagePrefix + std::to_string(sent_messages)});
  }

  // The test shouldn't commit the offsets, so it is possible to consume the same messages multiple times.
  auto check_test_method = [&]() {
    std::atomic<int> received_message_count{0};
    auto expected_messages_received = true;

    ASSERT_FALSE(consumer->IsRunning());

    consumer->Check(kDontCareTimeout, kMessageCount, [&](const std::vector<Message> &messages) mutable {
      auto message_count = received_message_count.load();
      for (const auto &message : messages) {
        std::string message_payload = kMessagePrefix + std::to_string(message_count++);
        expected_messages_received &=
            (message_payload == std::string_view(message.Payload().data(), message.Payload().size()));
      }
      received_message_count = message_count;
    });
    ASSERT_FALSE(consumer->IsRunning());

    EXPECT_TRUE(expected_messages_received) << "Some unexpected message has been received";
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

TEST_F(ConsumerTest, CheckMethodTimeout) {
  Consumer consumer{CreateDefaultConsumerInfo(), kDummyConsumerFunction};

  std::chrono::milliseconds timeout{3000};

  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(consumer.Check(timeout, std::nullopt, kDummyConsumerFunction), ConsumerCheckFailedException);
  const auto end = std::chrono::steady_clock::now();

  const auto elapsed = (end - start);
  EXPECT_LE(timeout, elapsed);
  EXPECT_LE(elapsed, timeout * 1.2);
}

TEST_F(ConsumerTest, CheckWithInvalidTimeout) {
  Consumer consumer{CreateDefaultConsumerInfo(), kDummyConsumerFunction};

  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(consumer.Check(std::chrono::milliseconds{0}, std::nullopt, kDummyConsumerFunction),
               ConsumerCheckFailedException);
  const auto end = std::chrono::steady_clock::now();

  static constexpr std::chrono::seconds kMaxExpectedTimeout{2};
  EXPECT_LE((end - start), kMaxExpectedTimeout) << "The check most probably failed because of an actual timeout and "
                                                   "not because of the invalid value for timeout.";
}

TEST_F(ConsumerTest, CheckWithInvalidBatchSize) {
  Consumer consumer{CreateDefaultConsumerInfo(), kDummyConsumerFunction};

  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(consumer.Check(std::nullopt, 0, kDummyConsumerFunction), ConsumerCheckFailedException);
  const auto end = std::chrono::steady_clock::now();

  static constexpr std::chrono::seconds kMaxExpectedTimeout{2};
  EXPECT_LE((end - start), kMaxExpectedTimeout) << "The check most probably failed because of an actual timeout and "
                                                   "not because of the invalid value for batch size.";
}

TEST_F(ConsumerTest, ConsumerStatus) {
  const std::string kConsumerName = "ConsumerGroupNameAAAA";
  const std::vector<std::string> topics = {"Topic1QWER", "Topic2XCVBB"};
  const std::string kConsumerGroupName = "ConsumerGroupTestAsdf";
  static constexpr auto kBatchInterval = std::chrono::milliseconds{111};
  static constexpr auto kBatchSize = 222;

  for (const auto &topic : topics) {
    cluster.CreateTopic(topic);
  }

  auto check_info = [&](const ConsumerInfo &info) {
    EXPECT_EQ(kConsumerName, info.consumer_name);
    EXPECT_EQ(kConsumerGroupName, info.consumer_group);
    EXPECT_EQ(kBatchInterval, info.batch_interval);
    EXPECT_EQ(kBatchSize, info.batch_size);
    EXPECT_EQ(2, info.topics.size()) << memgraph::utils::Join(info.topics, ",");
    ASSERT_LE(2, info.topics.size());
    EXPECT_EQ(topics[0], info.topics[0]);
    EXPECT_EQ(topics[1], info.topics[1]);
  };

  Consumer consumer{
      ConsumerInfo{kConsumerName, topics, kConsumerGroupName, cluster.Bootstraps(), kBatchInterval, kBatchSize},
      kDummyConsumerFunction};

  check_info(consumer.Info());
  consumer.Start();
  check_info(consumer.Info());
  consumer.StopIfRunning();
  check_info(consumer.Info());
}

TEST_F(ConsumerTest, LimitBatches_CannotStartIfAlreadyRunning) {
  static constexpr auto kLimitBatches = 3;

  auto info = CreateDefaultConsumerInfo();

  auto consumer = CreateConsumer(std::move(info), kDummyConsumerFunction);

  consumer->Start();
  ASSERT_TRUE(consumer->IsRunning());

  EXPECT_THROW(consumer->StartWithLimit(kLimitBatches, std::nullopt /*timeout*/), ConsumerRunningException);

  EXPECT_TRUE(consumer->IsRunning());

  consumer->Stop();
  EXPECT_FALSE(consumer->IsRunning());
}

TEST_F(ConsumerTest, LimitBatches_SendingMoreThanLimit) {
  /*
  We send more messages than the BatchSize*LimitBatches:
  -Consumer should receive 2*3=6 messages.
  -Consumer should not be running afterwards.
  */
  static constexpr auto kBatchSize = 2;
  static constexpr auto kLimitBatches = 3;
  static constexpr auto kNumberOfMessagesToSend = 20;
  static constexpr auto kNumberOfMessagesExpected = kBatchSize * kLimitBatches;
  static constexpr auto kBatchInterval =
      std::chrono::seconds{2};  // We do not want the batch interval to be the limiting factor here.

  auto info = CreateDefaultConsumerInfo();
  info.batch_size = kBatchSize;
  info.batch_interval = kBatchInterval;

  static constexpr std::string_view kMessage = "LimitBatchesTestMessage";

  auto expected_messages_received = true;
  auto number_of_messages_received = 0;
  auto consumer_function = [&expected_messages_received,
                            &number_of_messages_received](const std::vector<Message> &messages) mutable {
    number_of_messages_received += messages.size();
    for (const auto &message : messages) {
      expected_messages_received &= (kMessage == std::string_view(message.Payload().data(), message.Payload().size()));
    }
  };

  auto consumer = CreateConsumer(std::move(info), consumer_function);

  for (auto sent_messages = 0; sent_messages <= kNumberOfMessagesToSend; ++sent_messages) {
    cluster.SeedTopic(kTopicName, kMessage);
  }

  consumer->StartWithLimit(kLimitBatches, kDontCareTimeout);

  EXPECT_FALSE(consumer->IsRunning());
  EXPECT_EQ(number_of_messages_received, kNumberOfMessagesExpected);
  EXPECT_TRUE(expected_messages_received) << "Some unexpected message has been received";
}

TEST_F(ConsumerTest, LimitBatches_Timeout_Reached) {
  // We do not send any messages, we expect an exception to be thrown.
  static constexpr auto kLimitBatches = 3;

  auto info = CreateDefaultConsumerInfo();

  auto consumer = CreateConsumer(std::move(info), kDummyConsumerFunction);

  std::chrono::milliseconds timeout{3000};

  const auto start = std::chrono::steady_clock::now();
  EXPECT_THROW(consumer->StartWithLimit(kLimitBatches, timeout), ConsumerStartFailedException);
  const auto end = std::chrono::steady_clock::now();
  const auto elapsed = (end - start);

  EXPECT_LE(timeout, elapsed);
  EXPECT_LE(elapsed, timeout * 1.2);
}
