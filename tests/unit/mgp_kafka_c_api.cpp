#include <memory>
#include <span>
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
  explicit MockedRdKafkaMessage(std::string key) : key_(std::move(key)) {
    message.err = rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR__BEGIN;
    message.key = reinterpret_cast<void *>(key_.data());
    message.len = key_.size();
  }
  std::string errstr() const override { return {}; }

  RdKafka::ErrorCode err() const override { return RdKafka::ErrorCode::ERR_NO_ERROR; }

  RdKafka::Topic *topic() const override { return nullptr; }

  std::string topic_name() const override { return "Topic1"; };

  int32_t partition() const override { return 0; }

  void *payload() const override { return nullptr; }

  size_t len() const override { return 32; }

  const std::string *key() const override { return &key_; }

  const void *key_pointer() const override { return nullptr; }

  size_t key_len() const override { return key_.size(); }

  int64_t offset() const override { return 0; }

  rd_kafka_message_s *c_ptr() override { return &message; }

  RdKafka::MessageTimestamp timestamp() const override { return RdKafka::MessageTimestamp{}; }

  void *msg_opaque() const override { return nullptr; }

  int64_t latency() const override { return 0; }

  Status status() const override { return RdKafka::Message::Status::MSG_STATUS_NOT_PERSISTED; }

  RdKafka::Headers *headers() override { return nullptr; }

  RdKafka::Headers *headers(RdKafka::ErrorCode *err) override { return nullptr; }

  int32_t broker_id() const override { return 0; }
  // 	MessageTimestampType MSG_TIMESTAMP_NOT_AVAILABLE,

 private:
  std::string key_;
  rd_kafka_message_s message;
};

class MgpApiTest : public ::testing::Test {
 public:
  using Message = integrations::kafka::Message;

  MgpApiTest() { messages_ = std::make_unique<mgp_messages>(CreateMockedBatch()); }
  mgp_messages *Messages() { return messages_.get(); }

 private:
  utils::pmr::vector<mgp_message> CreateMockedBatch() {
    msgs_storage_.push_back(Message(std::make_unique<MockedRdKafkaMessage>("1")));
    msgs_storage_.push_back(Message(std::make_unique<MockedRdKafkaMessage>("2")));
    auto v = utils::pmr::vector<mgp_message>(utils::NewDeleteResource());
    v.push_back(mgp_message{&msgs_storage_.front()});
    v.push_back(mgp_message{&msgs_storage_.back()});
    return v;
  }
  std::unique_ptr<mgp_messages> messages_;
  utils::pmr::vector<Message> msgs_storage_{utils::NewDeleteResource()};
};

TEST_F(MgpApiTest, TEST_ALL_MGP_KAFKA_C_API) {
  mgp_messages *messages = Messages();
  auto first = mgp_messages_at(messages, 0);
  char expected_first = *mgp_message_key(first);
  EXPECT_EQ(expected_first, '1');

  auto second = mgp_messages_at(messages, 1);
  char expected_second = *mgp_message_key(second);
  EXPECT_EQ(expected_second, '2');

  // TODO add tests for the rest of the api
}
