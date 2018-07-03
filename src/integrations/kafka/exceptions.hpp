#pragma once

#include "utils/exceptions.hpp"

#include <fmt/format.h>

class KafkaStreamException : public utils::BasicException {
  using utils::BasicException::BasicException;
};

class StreamExistsException : public KafkaStreamException {
 public:
  explicit StreamExistsException(const std::string &stream_name)
      : KafkaStreamException(
            fmt::format("Kafka stream {} already exists.", stream_name)) {}
};

class StreamDoesntExistException : public KafkaStreamException {
 public:
  explicit StreamDoesntExistException(const std::string &stream_name)
      : KafkaStreamException(
            fmt::format("Kafka stream {} doesn't exist.", stream_name)) {}
};

class ConsumerFailedToInitializeException : public KafkaStreamException {
 public:
  ConsumerFailedToInitializeException(const std::string &stream_name,
                                      const std::string &error)
      : KafkaStreamException(fmt::format(
            "Failed to initialize kafka stream {} : {}", stream_name, error)) {}
};

class ConsumerNotAvailableException : public KafkaStreamException {
 public:
  explicit ConsumerNotAvailableException(const std::string &stream_name)
      : KafkaStreamException(
            fmt::format("Kafka stream {} not available", stream_name)) {}
};

class ConsumerRunningException : public KafkaStreamException {
 public:
  explicit ConsumerRunningException(const std::string &stream_name)
      : KafkaStreamException(
            fmt::format("Kafka stream {} is already running", stream_name)) {}
};

class ConsumerStoppedException : public KafkaStreamException {
 public:
  explicit ConsumerStoppedException(const std::string &stream_name)
      : KafkaStreamException(
            fmt::format("Kafka stream {} is already stopped", stream_name)) {}
};

class TopicNotFoundException : public KafkaStreamException {
 public:
  explicit TopicNotFoundException(const std::string &stream_name)
      : KafkaStreamException(
            fmt::format("Kafka stream {}, topic not found", stream_name)) {}
};
