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

#include <string>

#include "utils/exceptions.hpp"

namespace memgraph::integrations::pulsar {
class PulsarStreamException : public utils::BasicException {
  using utils::BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(PulsarStreamException)
};

class ConsumerFailedToInitializeException : public PulsarStreamException {
 public:
  ConsumerFailedToInitializeException(const std::string &consumer_name, const std::string &error)
      : PulsarStreamException("Failed to initialize Pulsar consumer {} : {}", consumer_name, error) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConsumerFailedToInitializeException)
};

class ConsumerRunningException : public PulsarStreamException {
 public:
  explicit ConsumerRunningException(const std::string &consumer_name)
      : PulsarStreamException("Pulsar consumer {} is already running", consumer_name) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConsumerRunningException)
};

class ConsumerStoppedException : public PulsarStreamException {
 public:
  explicit ConsumerStoppedException(const std::string &consumer_name)
      : PulsarStreamException("Pulsar consumer {} is already stopped", consumer_name) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConsumerStoppedException)
};

class ConsumerCheckFailedException : public PulsarStreamException {
 public:
  explicit ConsumerCheckFailedException(const std::string &consumer_name, const std::string &error)
      : PulsarStreamException("Pulsar consumer {} check failed: {}", consumer_name, error) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConsumerCheckFailedException)
};

class ConsumerStartFailedException : public PulsarStreamException {
 public:
  explicit ConsumerStartFailedException(const std::string &consumer_name, const std::string &error)
      : PulsarStreamException("Starting Pulsar consumer {} failed: {}", consumer_name, error) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConsumerStartFailedException)
};

class TopicNotFoundException : public PulsarStreamException {
 public:
  TopicNotFoundException(const std::string &consumer_name, const std::string &topic_name)
      : PulsarStreamException("Pulsar consumer {} cannot find topic {}", consumer_name, topic_name) {}
  SPECIALIZE_GET_EXCEPTION_NAME(TopicNotFoundException)
};

class ConsumerReadMessagesFailedException : public PulsarStreamException {
 public:
  ConsumerReadMessagesFailedException(const std::string_view consumer_name, const std::string_view error)
      : PulsarStreamException("Error happened in consumer {} while fetching messages: {}", consumer_name, error) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConsumerReadMessagesFailedException)
};

class ConsumerAcknowledgeMessagesFailedException : public PulsarStreamException {
 public:
  explicit ConsumerAcknowledgeMessagesFailedException(const std::string_view consumer_name)
      : PulsarStreamException("Acknowledging a message of consumer {} has failed!", consumer_name) {}
  SPECIALIZE_GET_EXCEPTION_NAME(ConsumerAcknowledgeMessagesFailedException)
};
}  // namespace memgraph::integrations::pulsar
