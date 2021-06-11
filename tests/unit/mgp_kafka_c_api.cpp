#include <algorithm>
#include <cstring>
#include <exception>
#include <iterator>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "integrations/kafka/consumer.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "utils/pmr/vector.hpp"

/// This class implements the interface of RdKafka::Message such that it can be mocked.
/// It's important to note that integrations::kafka::Message member functions
/// use c_ptr() to indirectly access the results inside the rd_kafka_message_s structure
/// effectively bypassing the mocked values returned by the overrides below. Therefore, to
/// protect against accidental use of the public members, the functions are marked as
/// [[noreturn]] and throw an std::logic_error exception.
class MockedRdKafkaMessage : public RdKafka::Message {
 public:
  explicit MockedRdKafkaMessage(std::string key, std::string payload)
      : key_(std::move(key)), payload_(std::move(payload)) {
    message_.err = rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__BEGIN;
    message_.key = static_cast<void *>(&key_[0]);
    message_.key_len = key_.size();
    message_.offset = 0;
    message_.payload = static_cast<void *>(payload_.data());
    message_.len = payload_.size();
    rd_kafka_ = rd_kafka_new(rd_kafka_type_t::RD_KAFKA_CONSUMER, nullptr, nullptr, 0);
    message_.rkt = rd_kafka_topic_new(rd_kafka_, topic_.data(), nullptr);
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

  [[noreturn]] std::string errstr() const override { throw_error(); }

  [[noreturn]] RdKafka::Topic *topic() const override { throw_error(); }

  [[noreturn]] std::string topic_name() const override { throw_error(); }

  [[noreturn]] int32_t partition() const override { throw_error(); }

  [[noreturn]] void *payload() const override { throw_error(); }

  [[noreturn]] size_t len() const override { throw_error(); }

  [[noreturn]] const std::string *key() const override { throw_error(); }

  [[noreturn]] const void *key_pointer() const override { throw_error(); }

  [[noreturn]] size_t key_len() const override { throw_error(); }

  [[noreturn]] int64_t offset() const override { throw_error(); }

  [[noreturn]] RdKafka::MessageTimestamp timestamp() const override { throw_error(); }

  [[noreturn]] void *msg_opaque() const override { throw_error(); }

  [[noreturn]] int64_t latency() const override { throw_error(); }

  [[noreturn]] Status status() const override { throw_error(); }

  [[noreturn]] RdKafka::Headers *headers() override { throw_error(); }

  [[noreturn]] RdKafka::Headers *headers(RdKafka::ErrorCode *err) override { throw_error(); }

  [[noreturn]] int32_t broker_id() const override { throw_error(); }

 private:
  [[noreturn]] void throw_error() const { throw std::logic_error("This function should not have been called"); }

  std::string key_;
  rd_kafka_message_s message_;
  rd_kafka_t *rd_kafka_;
  std::string payload_;
  std::string topic_{"Topic1"};
};

class MgpApiTest : public ::testing::Test {
 public:
  using Message = integrations::kafka::Message;
  using KMessage = MockedRdKafkaMessage;
  MgpApiTest() : msgs_storage_(utils::NewDeleteResource()) {
    messages_ = std::make_unique<mgp_messages>(CreateMockedBatch());
  }
  mgp_messages *Messages() { return messages_.get(); }

  const auto &ExpectedPayloads() { return expected_payload_; }

  const auto &ExpectedKeys() { return expected_key_; }

  const auto &ExpectedTopicNames() { return expected_tn_; }

  const auto &ExpectedPayloadSizes() { return expected_payload_sz_; }

  static constexpr size_t sample_size_{2};

 private:
  utils::pmr::vector<mgp_message> CreateMockedBatch() {
    for (int i = 0; i < sample_size_; ++i)
      msgs_storage_.push_back(
          Message(std::make_unique<KMessage>(std::string(1, expected_key_[i]), expected_payload_[i])));
    auto v = utils::pmr::vector<mgp_message>(utils::NewDeleteResource());
    std::transform(msgs_storage_.begin(), msgs_storage_.end(), std::back_inserter(v),
                   [](auto &msgs) { return mgp_message{&msgs}; });
    return v;
  }

  utils::pmr::vector<Message> msgs_storage_;
  std::unique_ptr<mgp_messages> messages_;
  const std::array<const char *, sample_size_> expected_payload_{"payload1", "payload2"};
  const std::array<char, sample_size_> expected_key_{'1', '2'};
  const std::array<const char *, sample_size_> expected_tn_{"Topic1", "Topic1"};
  const std::array<size_t, sample_size_> expected_payload_sz_{8, 8};
};

TEST_F(MgpApiTest, TEST_ALL_MGP_KAFKA_C_API) {
  const mgp_messages *messages = Messages();
  EXPECT_EQ(mgp_messages_size(messages), MgpApiTest::sample_size_);
  // Test for keys
  const auto msgs = std::array<const mgp_message *, MgpApiTest::sample_size_>{mgp_messages_at(messages, 0),
                                                                              mgp_messages_at(messages, 1)};
  for (int i = 0; i < MgpApiTest::sample_size_; ++i) {
    // Test for keys
    EXPECT_EQ(*mgp_message_key(msgs[i]), ExpectedKeys()[i]);
    // Test for payload size
    EXPECT_EQ(mgp_message_get_payload_size(msgs[i]), ExpectedPayloadSizes()[i]);
    // Test for payload
    EXPECT_FALSE(std::strcmp(mgp_message_get_payload(msgs[i]), ExpectedPayloads()[i]));
    // Test for topic name
    EXPECT_FALSE(std::strcmp(mgp_message_topic_name(msgs[i]), ExpectedTopicNames()[i]));
  }
  //
  /* TODO @kostasrim
  //Test for timestamp
  auto expected_timestamp = rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE;
  EXPECT_EQ(mgp_message_timestamp(first_msg), expected_timestamp);
  EXPECT_EQ(mgp_message_timestamp(second_msg), expected_timestamp);
  */
}
