#pragma once

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafka_mock.h>
#include <librdkafka/rdkafkacpp.h>

// Based on https://github.com/edenhill/librdkafka/issues/2693
namespace details {
struct RdKafkaDeleter {
  void operator()(rd_kafka_t *rd);
};
struct RdKafkaMockClusterDeleter {
  void operator()(rd_kafka_mock_cluster_t *rd);
};
}  // namespace details

using RdKafkaUniquePtr = std::unique_ptr<rd_kafka_t, details::RdKafkaDeleter>;
using RdKafkaMockClusterUniquePtr = std::unique_ptr<rd_kafka_mock_cluster_t, details::RdKafkaMockClusterDeleter>;

class KafkaClusterMock {
 public:
  explicit KafkaClusterMock(const std::vector<std::string> &topics);

  std::string Bootstraps() const;
  void SeedTopic(const std::string &topic_name, std::span<const char> message);
  void SeedTopic(const std::string &topic_name, std::string_view message);

 private:
  RdKafkaUniquePtr rk_{nullptr};
  RdKafkaMockClusterUniquePtr cluster_{nullptr};
};