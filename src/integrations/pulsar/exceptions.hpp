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

namespace integrations::pulsar {
class PulsarStreamException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class ConsumerFailedToInitializeException : public PulsarStreamException {
 public:
  ConsumerFailedToInitializeException(const std::string &consumer_name, const std::string &error)
      : PulsarStreamException("Failed to initialize Pulsar consumer {} : {}", consumer_name, error) {}
};

class ConsumerRunningException : public PulsarStreamException {
 public:
  explicit ConsumerRunningException(const std::string &consumer_name)
      : PulsarStreamException("Pulsar consumer {} is already running", consumer_name) {}
};

class ConsumerStoppedException : public PulsarStreamException {
 public:
  explicit ConsumerStoppedException(const std::string &consumer_name)
      : PulsarStreamException("Pulsar consumer {} is already stopped", consumer_name) {}
};

class ConsumerCheckFailedException : public PulsarStreamException {
 public:
  explicit ConsumerCheckFailedException(const std::string &consumer_name, const std::string &error)
      : PulsarStreamException("Pulsar consumer {} check failed: {}", consumer_name, error) {}
};

class ConsumerStartFailedException : public PulsarStreamException {
 public:
  explicit ConsumerStartFailedException(const std::string &consumer_name, const std::string &error)
      : PulsarStreamException("Starting Pulsar consumer {} failed: {}", consumer_name, error) {}
};

class TopicNotFoundException : public PulsarStreamException {
 public:
  TopicNotFoundException(const std::string &consumer_name, const std::string &topic_name)
      : PulsarStreamException("Pulsar consumer {} cannot find topic {}", consumer_name, topic_name) {}
};
}  // namespace integrations::pulsar
