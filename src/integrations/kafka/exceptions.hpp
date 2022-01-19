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

#include <string>

#include "utils/exceptions.hpp"

namespace integrations::kafka {
class KafkaStreamException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class ConsumerFailedToInitializeException : public KafkaStreamException {
 public:
  ConsumerFailedToInitializeException(const std::string &consumer_name, const std::string &error)
      : KafkaStreamException("Failed to initialize Kafka consumer {} : {}", consumer_name, error) {}
};

class SettingCustomConfigFailed : public KafkaStreamException {
 public:
  SettingCustomConfigFailed(const std::string &consumer_name, const std::string &error, const std::string &key,
                            const std::string &value)
      : KafkaStreamException(R"(Failed to set custom config ("{}": "{}") for Kafka consumer {} : {})", key, value,
                             consumer_name, error) {}
};

class ConsumerRunningException : public KafkaStreamException {
 public:
  explicit ConsumerRunningException(const std::string &consumer_name)
      : KafkaStreamException("Kafka consumer {} is already running", consumer_name) {}
};

class ConsumerStoppedException : public KafkaStreamException {
 public:
  explicit ConsumerStoppedException(const std::string &consumer_name)
      : KafkaStreamException("Kafka consumer {} is already stopped", consumer_name) {}
};

class ConsumerCheckFailedException : public KafkaStreamException {
 public:
  explicit ConsumerCheckFailedException(const std::string &consumer_name, const std::string &error)
      : KafkaStreamException("Kafka consumer {} check failed: {}", consumer_name, error) {}
};

class ConsumerStartFailedException : public KafkaStreamException {
 public:
  explicit ConsumerStartFailedException(const std::string &consumer_name, const std::string &error)
      : KafkaStreamException("Starting Kafka consumer {} failed: {}", consumer_name, error) {}
};

class TopicNotFoundException : public KafkaStreamException {
 public:
  TopicNotFoundException(const std::string &consumer_name, const std::string &topic_name)
      : KafkaStreamException("Kafka consumer {} cannot find topic {}", consumer_name, topic_name) {}
};
}  // namespace integrations::kafka
