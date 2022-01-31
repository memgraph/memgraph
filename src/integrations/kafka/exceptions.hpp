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

#pragma once

#include <string_view>

#include "utils/exceptions.hpp"

namespace integrations::kafka {
class KafkaStreamException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class ConsumerFailedToInitializeException : public KafkaStreamException {
 public:
  ConsumerFailedToInitializeException(const std::string_view consumer_name, const std::string_view error)
      : KafkaStreamException("Failed to initialize Kafka consumer {} : {}", consumer_name, error) {}
};

class SettingCustomConfigFailed : public ConsumerFailedToInitializeException {
 public:
  SettingCustomConfigFailed(const std::string_view consumer_name, const std::string_view error,
                            const std::string_view key, const std::string_view value)
      : ConsumerFailedToInitializeException(
            consumer_name,
            fmt::format(R"(failed to set custom config ("{}": "{}"), because of error {})", key, value, error)) {}
};

class ConsumerRunningException : public KafkaStreamException {
 public:
  explicit ConsumerRunningException(const std::string_view consumer_name)
      : KafkaStreamException("Kafka consumer {} is already running", consumer_name) {}
};

class ConsumerStoppedException : public KafkaStreamException {
 public:
  explicit ConsumerStoppedException(const std::string_view consumer_name)
      : KafkaStreamException("Kafka consumer {} is already stopped", consumer_name) {}
};

class ConsumerCheckFailedException : public KafkaStreamException {
 public:
  explicit ConsumerCheckFailedException(const std::string_view consumer_name, const std::string_view error)
      : KafkaStreamException("Kafka consumer {} check failed: {}", consumer_name, error) {}
};

class ConsumerStartFailedException : public KafkaStreamException {
 public:
  explicit ConsumerStartFailedException(const std::string_view consumer_name, const std::string_view error)
      : KafkaStreamException("Starting Kafka consumer {} failed: {}", consumer_name, error) {}
};

class TopicNotFoundException : public KafkaStreamException {
 public:
  TopicNotFoundException(const std::string_view consumer_name, const std::string_view topic_name)
      : KafkaStreamException("Kafka consumer {} cannot find topic {}", consumer_name, topic_name) {}
};
}  // namespace integrations::kafka
