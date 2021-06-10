#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "integrations/kafka/consumer.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "utils/pmr/vector.hpp"

/// This class implements the interface of RdKafka::Message such that it can be used
/// to construct an object of the class integrations::kafka::Message. Since this test,
/// only uses the key() and key_len() member functions the rest are implemented as foo
/// values. It's important to note that integrations::kafka::Message member functions
/// use c_ptr() to indirectly access the results inside the rd_kafka_message_s structure
/// effectively bypassing the foo values returned by the overrides below. Therefore,
/// any use (othern than Key() method of class integrations::kafka::Message) will lead
/// to UB.
class MockedRdKafkaMessage : public RdKafka::Message {
 public:
  explicit MockedRdKafkaMessage(std::string key, std::string payload)
      : key_(std::move(key)), payload_(std::move(payload)) {
    message_.err = rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__BEGIN;
    message_.key = static_cast<void *>(&key_[0]);
    message_.key_len = key_.size();
    message_.offset = 0;
    message_.payload = static_cast<void *>(payload_.data());
    message_.len = key_.size();
  }

  std::string errstr() const override { return {}; }

  RdKafka::ErrorCode err() const override { return RdKafka::ErrorCode::ERR_NO_ERROR; }

  RdKafka::Topic *topic() const override { return nullptr; }

  std::string topic_name() const override { return "Topic1"; };

  int32_t partition() const override { return 0; }

  void *payload() const override { return nullptr; }

  size_t len() const override { return 0; }

  const std::string *key() const override { return &key_; }

  const void *key_pointer() const override { return key_.data(); }

  size_t key_len() const override { return key_.size(); }

  int64_t offset() const override { return 0; }

  rd_kafka_message_s *c_ptr() override { return &message_; }

  RdKafka::MessageTimestamp timestamp() const override { return RdKafka::MessageTimestamp{}; }

  void *msg_opaque() const override { return nullptr; }

  int64_t latency() const override { return 0; }

  Status status() const override { return RdKafka::Message::Status::MSG_STATUS_NOT_PERSISTED; }

  RdKafka::Headers *headers() override { return nullptr; }

  RdKafka::Headers *headers(RdKafka::ErrorCode *err) override { return nullptr; }

  int32_t broker_id() const override { return 0; }

 private:
  std::string key_;
  rd_kafka_message_s message_;
  std::string payload_;
};

class MgpApiTest : public ::testing::Test {
 public:
  using Message = integrations::kafka::Message;
  using KMessage = MockedRdKafkaMessage;
  MgpApiTest() : messages_(std::make_unique<mgp_messages>(CreateMockedBatch())) {}

  mgp_messages *Messages() { return messages_.get(); }

 private:
  utils::pmr::vector<mgp_message> CreateMockedBatch() {
    msgs_storage_.push_back(Message(std::make_unique<KMessage>("1", "payload1")));
    msgs_storage_.push_back(Message(std::make_unique<KMessage>("2", "payload2")));
    auto v = utils::pmr::vector<mgp_message>(utils::NewDeleteResource());
    v.push_back(mgp_message{&msgs_storage_.front()});
    v.push_back(mgp_message{&msgs_storage_.back()});
    return v;
  }
  std::unique_ptr<mgp_messages> messages_;
  utils::pmr::vector<Message> msgs_storage_{utils::NewDeleteResource()};
};

TEST_F(MgpApiTest, TEST_ALL_MGP_KAFKA_C_API) {
  const mgp_messages *messages = Messages();
  constexpr size_t expected_size = 2;
  EXPECT_EQ(mgp_messages_size(messages), expected_size);
  // Test for keys
  const auto *first_msg = mgp_messages_at(messages, 0);
  constexpr char expected_first_kv = '1';
  const char *result = mgp_message_key(first_msg);
  EXPECT_EQ(*result, expected_first_kv);

  const auto *second_msg = mgp_messages_at(messages, 1);
  constexpr char expected_second_kv = '2';
  EXPECT_EQ(*mgp_message_key(second_msg), expected_second_kv);
  // Test for payload size
  constexpr size_t expected_first_msg_payload_size = 9;
  EXPECT_EQ(mgp_message_get_payload_size(first_msg), expected_first_msg_payload_size);

  constexpr size_t expected_second_msg_payload_size = 9;
  EXPECT_EQ(mgp_message_get_payload_size(second_msg), expected_second_msg_payload_size);

  // Test for payload
  const char *expected_first_msg_payload = "payload1";
  EXPECT_EQ(mgp_message_get_payload(first_msg), expected_first_msg_payload);

  const char *expected_second_msg_payload = "payload2";
  EXPECT_EQ(mgp_message_get_payload(second_msg), expected_second_msg_payload);
  /*
  //Test for topic name
  const char* expected_topic_name = "Topic1";
  EXPECT_EQ(mgp_message_topic_name(first_msg), expected_topic_name);
  EXPECT_EQ(mgp_message_topic_name(second_msg), expected_topic_name);
  //
  //Test for timestamp
  auto expected_timestamp = rd_kafka_timestamp_type_t::RD_KAFKA_TIMESTAMP_NOT_AVAILABLE
  EXPECT_EQ(mgp_message_timestamp(first_msg), expected_timestamp);
  EXPECT_EQ(mgp_message_timestamp(second_msg), expected_timestamp);
  */
}
