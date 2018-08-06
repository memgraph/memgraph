#pragma once

#include "utils/exceptions.hpp"

#include <fmt/format.h>

namespace integrations::kafka {

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

class StreamSerializationException : public KafkaStreamException {
 public:
  StreamSerializationException()
      : KafkaStreamException("Failed to serialize stream data!") {}
};

class StreamDeserializationException : public KafkaStreamException {
 public:
  StreamDeserializationException()
      : KafkaStreamException("Failed to deserialize stream data!") {}
};

class StreamMetadataCouldNotBeStored : public KafkaStreamException {
 public:
  explicit StreamMetadataCouldNotBeStored(const std::string &stream_name)
      : KafkaStreamException(fmt::format(
            "Couldn't persist stream metadata for stream {}", stream_name)) {}
};

class StreamMetadataCouldNotBeDeleted : public KafkaStreamException {
 public:
  explicit StreamMetadataCouldNotBeDeleted(const std::string &stream_name)
      : KafkaStreamException(fmt::format(
            "Couldn't delete persisted stream metadata for stream {}",
            stream_name)) {}
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

class TransformScriptNotFoundException : public KafkaStreamException {
 public:
  explicit TransformScriptNotFoundException(const std::string &stream_name)
      : KafkaStreamException(fmt::format(
            "Couldn't find transform script for {}", stream_name)) {}
};

class TransformScriptDownloadException : public KafkaStreamException {
 public:
  explicit TransformScriptDownloadException(const std::string &transform_uri)
      : KafkaStreamException(fmt::format(
            "Couldn't get the transform script from {}", transform_uri)) {}
};

class TransformScriptCouldNotBeCreatedException : public KafkaStreamException {
 public:
  explicit TransformScriptCouldNotBeCreatedException(
      const std::string &stream_name)
      : KafkaStreamException(fmt::format(
            "Couldn't create transform script for stream {}", stream_name)) {}
};

class TransformScriptCouldNotBeDeletedException : public KafkaStreamException {
 public:
  explicit TransformScriptCouldNotBeDeletedException(
      const std::string &stream_name)
      : KafkaStreamException(fmt::format(
            "Couldn't delete transform script for stream {}", stream_name)) {}
};

class TransformExecutionException : public KafkaStreamException {
  using KafkaStreamException::KafkaStreamException;
};

}  // namespace integrations::kafka
