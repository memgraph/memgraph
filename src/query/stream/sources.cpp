// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/stream/sources.hpp"

#include <json/json.hpp>

namespace query {
KafkaStream::KafkaStream(std::string stream_name, StreamInfo stream_info,
                         ConsumerFunction<integrations::kafka::Message> consumer_function) {
  integrations::kafka::ConsumerInfo consumer_info{
      .consumer_name = std::move(stream_name),
      .topics = std::move(stream_info.topics),
      .consumer_group = std::move(stream_info.consumer_group),
      .batch_interval = stream_info.common_info.batch_interval,
      .batch_size = stream_info.common_info.batch_size,
  };
  consumer_.emplace(std::move(stream_info.bootstrap_servers), std::move(consumer_info), std::move(consumer_function));
};

KafkaStream::StreamInfo KafkaStream::Info(std::string transformation_name) const {
  const auto &info = consumer_->Info();
  return {{.batch_interval = info.batch_interval,
           .batch_size = info.batch_size,
           .transformation_name = std::move(transformation_name)},
          .topics = info.topics,
          .consumer_group = info.consumer_group};
}

void KafkaStream::Start() { consumer_->Start(); }
void KafkaStream::Stop() { consumer_->Stop(); }
bool KafkaStream::IsRunning() const { return consumer_->IsRunning(); }

void KafkaStream::Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> batch_limit,
                        const ConsumerFunction<integrations::kafka::Message> &consumer_function) const {
  consumer_->Check(timeout, batch_limit, consumer_function);
}

namespace {
const std::string kTopicsKey{"topics"};
const std::string kConsumerGroupKey{"consumer_group"};
const std::string kBoostrapServers{"bootstrap_servers"};
}  // namespace

void to_json(nlohmann::json &data, KafkaStream::StreamInfo &&info) {
  data[kCommonInfoKey] = std::move(info.common_info);
  data[kTopicsKey] = std::move(info.topics);
  data[kConsumerGroupKey] = info.consumer_group;
  data[kBoostrapServers] = std::move(info.bootstrap_servers);
}

void from_json(const nlohmann::json &data, KafkaStream::StreamInfo &info) {
  data.at(kCommonInfoKey).get_to(info.common_info);
  data.at(kTopicsKey).get_to(info.topics);
  data.at(kConsumerGroupKey).get_to(info.consumer_group);
  data.at(kBoostrapServers).get_to(info.bootstrap_servers);
}

PulsarStream::PulsarStream(std::string stream_name, StreamInfo stream_info,
                           ConsumerFunction<integrations::pulsar::Message> consumer_function) {
  integrations::pulsar::ConsumerInfo consumer_info{
      .batch_size = stream_info.common_info.batch_size,
      .batch_interval = stream_info.common_info.batch_interval,
      .topic = std::move(stream_info.topic),
      .subscription_name = std::move(stream_name),
  };

  consumer_.emplace(stream_info.cluster_endpoint, std::move(consumer_info), std::move(consumer_function));
};

PulsarStream::StreamInfo PulsarStream::Info(std::string transformation_name) const {
  const auto &info = consumer_->Info();
  return {{.batch_interval = info.batch_interval,
           .batch_size = info.batch_size,
           .transformation_name = std::move(transformation_name)},
          .topic = info.topic};
}

void PulsarStream::Start() { consumer_->Start(); }
void PulsarStream::Stop() { consumer_->Stop(); }
bool PulsarStream::IsRunning() const { return consumer_->IsRunning(); }

void PulsarStream::Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> batch_limit,
                         const ConsumerFunction<Message> &consumer_function) const {
  consumer_->Check(timeout, batch_limit, consumer_function);
}

namespace {
const std::string kClusterEndpoint{"cluster_endpoint"};
}  // namespace

void to_json(nlohmann::json &data, PulsarStream::StreamInfo &&info) {
  data[kCommonInfoKey] = std::move(info.common_info);
  data[kTopicsKey] = std::move(info.topic);
  data[kClusterEndpoint] = std::move(info.cluster_endpoint);
}

void from_json(const nlohmann::json &data, PulsarStream::StreamInfo &info) {
  data.at(kCommonInfoKey).get_to(info.common_info);
  data.at(kTopicsKey).get_to(info.topic);
  data.at(kClusterEndpoint).get_to(info.cluster_endpoint);
}
}  // namespace query
