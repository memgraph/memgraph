#pragma once

#include <string>

#include "utils/exceptions.hpp"

class KafkaStreamException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class ConsumerFailedToInitializeException : public KafkaStreamException {
 public:
  ConsumerFailedToInitializeException(const std::string &consumer_name, const std::string &error)
      : KafkaStreamException("Failed to initialize Kafka consumer {} : {}", consumer_name, error) {}
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

class ConsumerTestFailedException : public KafkaStreamException {
 public:
  explicit ConsumerTestFailedException(const std::string &consumer_name, const std::string &error)
      : KafkaStreamException("Kafka consumer {} test failed: {}", consumer_name, error) {}
};

class TopicNotFoundException : public KafkaStreamException {
 public:
  TopicNotFoundException(const std::string &consumer_name, const std::string &topic_name)
      : KafkaStreamException("Kafka consumer {} cannot find topic {}", consumer_name, topic_name) {}
};
