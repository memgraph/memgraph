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

#pragma once

#include "query/stream/common.hpp"

#include "integrations/kafka/consumer.hpp"

namespace query {

struct KafkaStream {
  struct StreamInfo {
    CommonStreamInfo common_info;
    std::vector<std::string> topics;
    std::string consumer_group;
    std::string bootstrap_servers;
  };

  using Message = integrations::kafka::Message;

  KafkaStream(std::string stream_name, StreamInfo stream_info,
              ConsumerFunction<integrations::kafka::Message> consumer_function);

  StreamInfo Info(std::string transformation_name) const;

  void Start();
  void Stop();
  bool IsRunning() const;

  void Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> batch_limit,
             const ConsumerFunction<Message> &consumer_function) const;

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

}  // namespace query
