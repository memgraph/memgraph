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

#include "query/stream/sources.hpp"

#include <json/json.hpp>

#include "integrations/constants.hpp"

namespace memgraph::query::stream {
KafkaStream::KafkaStream(std::string stream_name, StreamInfo stream_info,
                         ConsumerFunction<integrations::kafka::Message> consumer_function) {
  integrations::kafka::ConsumerInfo consumer_info{
      .consumer_name = std::move(stream_name),
      .topics = std::move(stream_info.topics),
      .consumer_group = std::move(stream_info.consumer_group),
      .bootstrap_servers = std::move(stream_info.bootstrap_servers),
      .batch_interval = stream_info.common_info.batch_interval,
      .batch_size = stream_info.common_info.batch_size,
      .public_configs = std::move(stream_info.configs),
      .private_configs = std::move(stream_info.credentials),
  };
  consumer_.emplace(std::move(consumer_info), std::move(consumer_function));
};

KafkaStream::StreamInfo KafkaStream::Info(std::string transformation_name) const {
  const auto &info = consumer_->Info();
  return {{.batch_interval = info.batch_interval,
           .batch_size = info.batch_size,
           .transformation_name = std::move(transformation_name)},
          .topics = info.topics,
          .consumer_group = info.consumer_group,
          .bootstrap_servers = info.bootstrap_servers,
          .configs = info.public_configs,
          .credentials = info.private_configs};
}

void KafkaStream::Start() { consumer_->Start(); }
void KafkaStream::StartWithLimit(uint64_t batch_limit, std::optional<std::chrono::milliseconds> timeout) const {
  consumer_->StartWithLimit(batch_limit, timeout);
}
void KafkaStream::Stop() { consumer_->Stop(); }
bool KafkaStream::IsRunning() const { return consumer_->IsRunning(); }

void KafkaStream::Check(std::optional<std::chrono::milliseconds> timeout, std::optional<uint64_t> batch_limit,
                        const ConsumerFunction<integrations::kafka::Message> &consumer_function) const {
  consumer_->Check(timeout, batch_limit, consumer_function);
}

utils::BasicResult<std::string> KafkaStream::SetStreamOffset(const int64_t offset) {
  return consumer_->SetConsumerOffsets(offset);
}

namespace {
const std::string kTopicsKey{"topics"};
const std::string kConsumerGroupKey{"consumer_group"};
const std::string kBootstrapServers{"bootstrap_servers"};
const std::string kConfigs{"configs"};
const std::string kCredentials{"credentials"};

const std::unordered_map<std::string, std::string> kDefaultConfigsMap;
}  // namespace

void to_json(nlohmann::json &data, KafkaStream::StreamInfo &&info) {
  data[kCommonInfoKey] = std::move(info.common_info);
  data[kTopicsKey] = std::move(info.topics);
  data[kConsumerGroupKey] = info.consumer_group;
  data[kBootstrapServers] = std::move(info.bootstrap_servers);
  data[kConfigs] = std::move(info.configs);
  data[kCredentials] = std::move(info.credentials);
}

void from_json(const nlohmann::json &data, KafkaStream::StreamInfo &info) {
  data.at(kCommonInfoKey).get_to(info.common_info);
  data.at(kTopicsKey).get_to(info.topics);
  data.at(kConsumerGroupKey).get_to(info.consumer_group);
  data.at(kBootstrapServers).get_to(info.bootstrap_servers);
  // These values might not be present in the persisted JSON object
  info.configs = data.value(kConfigs, kDefaultConfigsMap);
  info.credentials = data.value(kCredentials, kDefaultConfigsMap);
}

PulsarStream::PulsarStream(std::string stream_name, StreamInfo stream_info,
                           ConsumerFunction<integrations::pulsar::Message> consumer_function) {
  integrations::pulsar::ConsumerInfo consumer_info{.batch_size = stream_info.common_info.batch_size,
                                                   .batch_interval = stream_info.common_info.batch_interval,
                                                   .topics = std::move(stream_info.topics),
                                                   .consumer_name = std::move(stream_name),
                                                   .service_url = std::move(stream_info.service_url)};

  consumer_.emplace(std::move(consumer_info), std::move(consumer_function));
};

PulsarStream::StreamInfo PulsarStream::Info(std::string transformation_name) const {
  const auto &info = consumer_->Info();
  return {{.batch_interval = info.batch_interval,
           .batch_size = info.batch_size,
           .transformation_name = std::move(transformation_name)},
          .topics = info.topics,
          .service_url = info.service_url};
}

void PulsarStream::Start() { consumer_->Start(); }
void PulsarStream::StartWithLimit(uint64_t batch_limit, std::optional<std::chrono::milliseconds> timeout) const {
  consumer_->StartWithLimit(batch_limit, timeout);
}
void PulsarStream::Stop() { consumer_->Stop(); }
bool PulsarStream::IsRunning() const { return consumer_->IsRunning(); }
void PulsarStream::Check(std::optional<std::chrono::milliseconds> timeout, std::optional<uint64_t> batch_limit,
                         const ConsumerFunction<Message> &consumer_function) const {
  consumer_->Check(timeout, batch_limit, consumer_function);
}

namespace {
const std::string kServiceUrl{"service_url"};
}  // namespace

void to_json(nlohmann::json &data, PulsarStream::StreamInfo &&info) {
  data[kCommonInfoKey] = std::move(info.common_info);
  data[kTopicsKey] = std::move(info.topics);
  data[kServiceUrl] = std::move(info.service_url);
}

void from_json(const nlohmann::json &data, PulsarStream::StreamInfo &info) {
  data.at(kCommonInfoKey).get_to(info.common_info);
  data.at(kTopicsKey).get_to(info.topics);
  data.at(kServiceUrl).get_to(info.service_url);
}
}  // namespace memgraph::query::stream
