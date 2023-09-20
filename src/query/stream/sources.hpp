// Copyright 2023 Memgraph Ltd.
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

#include "query/stream/common.hpp"

#include "integrations/kafka/consumer.hpp"
#include "integrations/pulsar/consumer.hpp"

namespace memgraph::query::stream {

struct KafkaStream {
  struct StreamInfo {
    CommonStreamInfo common_info;
    std::vector<std::string> topics;
    std::string consumer_group;
    std::string bootstrap_servers;
    std::unordered_map<std::string, std::string> configs;
    std::unordered_map<std::string, std::string> credentials;
  };

  using Message = integrations::kafka::Message;

  KafkaStream(std::string stream_name, StreamInfo stream_info,
              ConsumerFunction<integrations::kafka::Message> consumer_function);

  StreamInfo Info(std::string transformation_name) const;

  void Start();
  void StartWithLimit(uint64_t batch_limit, std::optional<std::chrono::milliseconds> timeout) const;
  void Stop();
  bool IsRunning() const;

  void Check(std::optional<std::chrono::milliseconds> timeout, std::optional<uint64_t> batch_limit,
             ConsumerFunction<Message> consumer_function) const;

  utils::BasicResult<std::string> SetStreamOffset(int64_t offset);

 private:
  using Consumer = integrations::kafka::Consumer;
  std::optional<Consumer> consumer_;
};

void to_json(nlohmann::json &data, KafkaStream::StreamInfo &&info);
void from_json(const nlohmann::json &data, KafkaStream::StreamInfo &info);

template <>
inline StreamSourceType StreamType(const KafkaStream & /*stream*/) {
  return StreamSourceType::KAFKA;
}

struct PulsarStream {
  struct StreamInfo {
    CommonStreamInfo common_info;
    std::vector<std::string> topics;
    std::string service_url;
  };

  using Message = integrations::pulsar::Message;

  PulsarStream(std::string stream_name, StreamInfo stream_info, ConsumerFunction<Message> consumer_function);

  StreamInfo Info(std::string transformation_name) const;

  void Start();
  void StartWithLimit(uint64_t batch_limit, std::optional<std::chrono::milliseconds> timeout) const;
  void Stop();
  bool IsRunning() const;

  void Check(std::optional<std::chrono::milliseconds> timeout, std::optional<uint64_t> batch_limit,
             ConsumerFunction<Message> consumer_function) const;

 private:
  using Consumer = integrations::pulsar::Consumer;
  std::optional<Consumer> consumer_;
};

void to_json(nlohmann::json &data, PulsarStream::StreamInfo &&info);
void from_json(const nlohmann::json &data, PulsarStream::StreamInfo &info);

template <>
inline StreamSourceType StreamType(const PulsarStream & /*stream*/) {
  return StreamSourceType::PULSAR;
}

}  // namespace memgraph::query::stream
