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
  void CreateTopic(const std::string &topic_name);
  void SeedTopic(const std::string &topic_name, std::span<const char> message);

 private:
  RdKafkaUniquePtr rk_{nullptr};
  RdKafkaMockClusterUniquePtr cluster_{nullptr};
};
