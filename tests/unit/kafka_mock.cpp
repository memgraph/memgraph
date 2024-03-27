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

#include "kafka_mock.hpp"

#include <chrono>
#include <thread>

namespace details {
void RdKafkaDeleter::operator()(rd_kafka_t *rd) {
  if (rd != nullptr) {
    rd_kafka_destroy(rd);
  }
}
void RdKafkaMockClusterDeleter::operator()(rd_kafka_mock_cluster_t *rd) {
  if (rd != nullptr) {
    rd_kafka_mock_cluster_destroy(rd);
  }
}
}  // namespace details

namespace {
void TestDeliveryReportCallback(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque) {
  if (rkmessage->_private != nullptr) {
    int *remains = static_cast<int *>(rkmessage->_private);
    (*remains)--;
  }
}
}  // namespace

KafkaClusterMock::KafkaClusterMock(const std::vector<std::string> &topics) {
  char errstr[256];
  auto *conf = rd_kafka_conf_new();
  if (conf == nullptr) {
    throw std::runtime_error("Couldn't create conf for Kafka mock");
  }

  if (rd_kafka_conf_set(conf, "client.id", "MOCK", errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    throw std::runtime_error(std::string("Failed to set client.id: ") + errstr);
  };

  rk_.reset(rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)));
  if (rk_ == nullptr) {
    throw std::runtime_error("Couldn't create producer for Kafka mock");
  }

  if (rk_ == nullptr) {
    throw std::runtime_error(std::string("Failed to create mock cluster rd_kafka_t: ") + errstr);
  }
  static constexpr auto broker_count = 1;
  cluster_.reset(rd_kafka_mock_cluster_new(rk_.get(), broker_count));
  if (cluster_ == nullptr) {
    throw std::runtime_error("Couldn't create cluster for Kafka mock");
  }

  for (const auto &topic : topics) {
    CreateTopic(topic);
  }
};

std::string KafkaClusterMock::Bootstraps() const { return rd_kafka_mock_cluster_bootstraps(cluster_.get()); };

void KafkaClusterMock::CreateTopic(const std::string &topic_name) {
  static constexpr auto partition_count = 1;
  static constexpr auto replication_factor = 1;
  rd_kafka_resp_err_t topic_err =
      rd_kafka_mock_topic_create(cluster_.get(), topic_name.c_str(), partition_count, replication_factor);
  if (RD_KAFKA_RESP_ERR_NO_ERROR != topic_err) {
    throw std::runtime_error("Failed to create the mock topic (" + topic_name + "): " + rd_kafka_err2str(topic_err));
  }
}

void KafkaClusterMock::SeedTopic(const std::string &topic_name, std::span<const char> message) {
  char errstr[256] = {'\0'};
  std::string bootstraps_servers = Bootstraps();

  rd_kafka_conf_t *conf = rd_kafka_conf_new();
  if (conf == nullptr) {
    throw std::runtime_error("Failed to create configuration for Kafka Mock producer to seed the topic " + topic_name);
  }
  rd_kafka_conf_set_dr_msg_cb(conf, TestDeliveryReportCallback);

  if (rd_kafka_conf_set(conf, "bootstrap.servers", bootstraps_servers.c_str(), errstr, sizeof(errstr)) !=
      RD_KAFKA_CONF_OK) {
    throw std::runtime_error("Failed to configure 'bootstrap.servers' to seed the topic " + topic_name +
                             "error: " + errstr);
  }

  rd_kafka_t *rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (nullptr == rk) {
    throw std::runtime_error("Failed to create RdKafka producer to seed the topic " + topic_name + "error: " + errstr);
  }

  rd_kafka_topic_conf_t *topic_conf = rd_kafka_topic_conf_new();

  rd_kafka_topic_t *rkt = rd_kafka_topic_new(rk, topic_name.c_str(), topic_conf);
  if (nullptr == rkt) {
    throw std::runtime_error("Failed to create RdKafka topic " + topic_name);
  }

  int remains = 1;
  if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
                       static_cast<void *>(const_cast<char *>(message.data())), message.size(), nullptr, 0,
                       &remains) == -1) {
    throw std::runtime_error("Failed to produce a message on " + topic_name + " to seed it");
  }

  while (remains > 0 && rd_kafka_outq_len(rk) > 0) {
    rd_kafka_poll(rk, 1000);
  }

  rd_kafka_topic_destroy(rkt);
  rd_kafka_destroy(rk);
  if (remains != 0) {
    throw std::runtime_error("Failed to delivered a message on " + topic_name + " to seed it");
  }
}
