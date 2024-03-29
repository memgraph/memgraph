// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <cstring>
#include <exception>
#include <iterator>
#include <memory>
#include <optional>
#include <string>

#include "gtest/gtest.h"
#include "integrations/kafka/consumer.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/stream/common.hpp"
#include "test_utils.hpp"
#include "utils/pmr/vector.hpp"

/// This class implements the interface of RdKafka::Message such that it can be mocked.
/// It's important to note that integrations::kafka::Message member functions
/// use c_ptr() to indirectly access the results inside the rd_kafka_message_s structure
/// effectively bypassing the mocked values returned by the overrides below. Therefore, to
/// protect against accidental use of the public members, the functions are marked as
/// [[noreturn]] and throw an std::logic_error exception.
class MockedRdKafkaMessage : public RdKafka::Message {
 public:
  explicit MockedRdKafkaMessage(std::string key, std::string payload, int64_t offset)
      : key_(std::move(key)), payload_(std::move(payload)) {
    message_.err = rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__BEGIN;
    message_.key = static_cast<void *>(key_.data());
    message_.key_len = key_.size();
    message_.offset = offset;
    message_.payload = static_cast<void *>(payload_.data());
    message_.len = payload_.size();
    rd_kafka_ = rd_kafka_new(rd_kafka_type_t::RD_KAFKA_CONSUMER, nullptr, nullptr, 0);
    message_.rkt = rd_kafka_topic_new(rd_kafka_, mocked_topic_name.data(), nullptr);
  }

  ~MockedRdKafkaMessage() override {
    rd_kafka_destroy(rd_kafka_);
    rd_kafka_topic_destroy(message_.rkt);
  }

  // The two can be accessed safely. Any use of the other public members should
  // be considered accidental (as per the current semantics of the class
  // Message) and therefore they are marked as [[noreturn]] and throw
  rd_kafka_message_s *c_ptr() override { return &message_; }

  // This is used by Message() constructor

  RdKafka::ErrorCode err() const override { return RdKafka::ErrorCode::ERR_NO_ERROR; }

  [[noreturn]] std::string errstr() const override { ThrowIllegalCallError(); }

  [[noreturn]] RdKafka::Topic *topic() const override { ThrowIllegalCallError(); }

  [[noreturn]] std::string topic_name() const override { ThrowIllegalCallError(); }

  [[noreturn]] int32_t partition() const override { ThrowIllegalCallError(); }

  [[noreturn]] void *payload() const override { ThrowIllegalCallError(); }

  [[noreturn]] size_t len() const override { ThrowIllegalCallError(); }

  [[noreturn]] const std::string *key() const override { ThrowIllegalCallError(); }

  [[noreturn]] const void *key_pointer() const override { ThrowIllegalCallError(); }

  [[noreturn]] size_t key_len() const override { ThrowIllegalCallError(); }

  [[noreturn]] int64_t offset() const override { ThrowIllegalCallError(); }

  [[noreturn]] RdKafka::MessageTimestamp timestamp() const override { ThrowIllegalCallError(); }

  [[noreturn]] void *msg_opaque() const override { ThrowIllegalCallError(); }

  [[noreturn]] int64_t latency() const override { ThrowIllegalCallError(); }

  [[noreturn]] Status status() const override { ThrowIllegalCallError(); }

  [[noreturn]] RdKafka::Headers *headers() override { ThrowIllegalCallError(); }

  [[noreturn]] RdKafka::Headers *headers(RdKafka::ErrorCode *err) override { ThrowIllegalCallError(); }

  [[noreturn]] int32_t broker_id() const override { ThrowIllegalCallError(); }

 private:
  [[noreturn]] void ThrowIllegalCallError() const {
    throw std::logic_error("This function should not have been called");
  }

  std::string key_;
  rd_kafka_t *rd_kafka_;
  std::string payload_;
  rd_kafka_message_s message_;

  static std::string mocked_topic_name;
};

std::string MockedRdKafkaMessage::mocked_topic_name = "Topic1";

class MgpApiTest : public ::testing::Test {
 public:
  using Message = memgraph::integrations::kafka::Message;
  using KafkaMessage = MockedRdKafkaMessage;

  MgpApiTest() { messages_.emplace(CreateMockedBatch()); }
  ~MgpApiTest() override { messages_.reset(); }

  mgp_messages &Messages() { return *messages_; }

 protected:
  struct ExpectedResult {
    const char *payload;
    const char key;
    const char *topic_name;
    const size_t payload_size;
    const int64_t offset;
  };

  static constexpr std::array<ExpectedResult, 2> expected = {ExpectedResult{"payload1", '1', "Topic1", 8, 0},
                                                             ExpectedResult{"payload2", '2', "Topic1", 8, 1}};

 private:
  memgraph::utils::pmr::vector<mgp_message> CreateMockedBatch() {
    std::transform(
        expected.begin(), expected.end(), std::back_inserter(msgs_storage_),
        [i = int64_t(0)](const auto expected) mutable {
          return Message(std::make_unique<KafkaMessage>(std::string(1, expected.key), expected.payload, i++));
        });
    auto v = memgraph::utils::pmr::vector<mgp_message>(memgraph::utils::NewDeleteResource());
    v.reserve(expected.size());
    std::transform(msgs_storage_.begin(), msgs_storage_.end(), std::back_inserter(v),
                   [](auto &msgs) { return mgp_message{msgs}; });
    return v;
  }

  memgraph::utils::pmr::vector<Message> msgs_storage_{memgraph::utils::NewDeleteResource()};
  std::optional<mgp_messages> messages_;
};

TEST_F(MgpApiTest, TestAllMgpKafkaCApi) {
  mgp_messages &messages = Messages();
  EXPECT_EQ(EXPECT_MGP_NO_ERROR(size_t, mgp_messages_size, &messages), expected.size());

  for (int i = 0; i < expected.size(); ++i) {
    auto *message = EXPECT_MGP_NO_ERROR(mgp_message *, mgp_messages_at, &messages, i);
    // Test for key and key size. Key size is always 1 in this test.
    EXPECT_EQ(EXPECT_MGP_NO_ERROR(size_t, mgp_message_key_size, message), 1);
    EXPECT_EQ(*EXPECT_MGP_NO_ERROR(const char *, mgp_message_key, message), expected[i].key);

    // Test for source type
    EXPECT_EQ(EXPECT_MGP_NO_ERROR(mgp_source_type, mgp_message_source_type, message), mgp_source_type::KAFKA);
    // Test for payload size
    EXPECT_EQ(EXPECT_MGP_NO_ERROR(size_t, mgp_message_payload_size, message), expected[i].payload_size);
    // Test for payload
    EXPECT_FALSE(std::strcmp(EXPECT_MGP_NO_ERROR(const char *, mgp_message_payload, message), expected[i].payload));
    // Test for topic name
    EXPECT_FALSE(
        std::strcmp(EXPECT_MGP_NO_ERROR(const char *, mgp_message_topic_name, message), expected[i].topic_name));
    // Test for offset
    EXPECT_EQ(EXPECT_MGP_NO_ERROR(int64_t, mgp_message_offset, message), expected[i].offset);
  }

  // Unfortunately, we can't test timestamp here because we can't mock (as explained above)
  // and the test does not have access to the internal rd_kafka_message2msg() function.
  // auto expected_timestamp = rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
  // EXPECT_EQ(mgp_message_timestamp(first_msg), expected_timestamp);
  // EXPECT_EQ(mgp_message_timestamp(second_msg), expected_timestamp);
}
