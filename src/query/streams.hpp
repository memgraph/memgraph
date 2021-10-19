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

#include <concepts>
#include <functional>
#include <map>
#include <optional>
#include <type_traits>
#include <unordered_map>

#include <json/json.hpp>

#include "integrations/kafka/consumer.hpp"
#include "kvstore/kvstore.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/event_counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace EventCounter {
extern const Event MessagesConsumed;
}  // namespace EventCounter

namespace query {

class StreamsException : public utils::BasicException {
 public:
  using BasicException::BasicException;
};

enum class StreamSourceType : uint8_t { KAFKA };

// TODO(antonio2368): Add a concept
template <typename T>
using SynchronizedStreamSource = utils::Synchronized<T, utils::WritePrioritizedRWLock>;

template <typename TMessage>
using ConsumerFunction = std::function<void(const std::vector<TMessage> &)>;

struct CommonStreamInfo {
  std::optional<std::chrono::milliseconds> batch_interval;
  std::optional<int64_t> batch_size;
  std::string transformation_name;
};

struct KafkaStream {
  struct StreamInfo {
    CommonStreamInfo common_info;
    std::vector<std::string> topics;
    std::string consumer_group;
    std::string bootstrap_servers;
  };

  using Consumer = integrations::kafka::Consumer;

  KafkaStream(std::string stream_name, StreamInfo stream_info,
              ConsumerFunction<integrations::kafka::Message> consumer_function) {
    integrations::kafka::ConsumerInfo consumer_info{
        .consumer_name = std::move(stream_name),
        .topics = std::move(stream_info.topics),
        .consumer_group = std::move(stream_info.consumer_group),
        .batch_interval = stream_info.common_info.batch_interval,
        .batch_size = stream_info.common_info.batch_size,
    };

    consumer_.emplace(std::move(stream_info.bootstrap_servers), std::move(consumer_info), std::move(consumer_function));
  }

  StreamInfo Info(std::string transformation_name) const {
    const auto &info = consumer_->Info();
    return {{.batch_interval = info.batch_interval,
             .batch_size = info.batch_size,
             .transformation_name = std::move(transformation_name)},
            .topics = info.topics,
            .consumer_group = info.consumer_group};
  }

  void Start() { consumer_->Start(); }
  void Stop() { consumer_->Stop(); }
  bool IsRunning() const { return consumer_->IsRunning(); }

  void Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> batch_limit,
             const ConsumerFunction<integrations::kafka::Message> &consumer_function) const {
    consumer_->Check(timeout, batch_limit, consumer_function);
  }

  std::optional<Consumer> consumer_;
};

using StreamVariant = std::variant<KafkaStream>;

template <typename T>
StreamSourceType StreamType(const T & /*stream*/) {
  static_assert(std::same_as<T, KafkaStream>);
  return StreamSourceType::KAFKA;
}

template <typename T>
struct StreamInfo;

template <>
struct StreamInfo<void> {
  using Type = CommonStreamInfo;
};

template <typename T>
concept Stream = utils::SameAsAnyOf<T, KafkaStream>;

template <Stream TStream>
struct StreamInfo<TStream> {
  using Type = typename TStream::StreamInfo;
};

template <typename T = void>
struct StreamStatus {
  std::string name;
  StreamSourceType type;
  bool is_running;
  typename StreamInfo<T>::Type info;
  std::optional<std::string> owner;
};

using TransformationResult = std::vector<std::vector<TypedValue>>;
using TransformFunction = std::function<TransformationResult(const std::vector<integrations::kafka::Message> &)>;

// TODO(antonio2368): Add a concept
template <typename T>
struct StreamData {
  std::string transformation_name;
  std::optional<std::string> owner;
  std::unique_ptr<SynchronizedStreamSource<T>> stream_source;
};

struct InterpreterContext;

/// Manages Kafka consumers.
///
/// This class is responsible for all query supported actions to happen.
class Streams final {
 public:
  /// Initializes the streams.
  ///
  /// @param interpreter_context context to use to run the result of transformations
  /// @param bootstrap_servers initial list of brokers as a comma separated list of broker host or host:port
  /// @param directory a directory path to store the persisted streams metadata
  Streams(InterpreterContext *interpreter_context, std::string bootstrap_servers, std::filesystem::path directory);

  /// Restores the streams from the persisted metadata.
  /// The restoration is done in a best effort manner, therefore no exception is thrown on failure, but the error is
  /// logged. If a stream was running previously, then after restoration it will be started.
  /// This function should only be called when there are no existing streams.
  void RestoreStreams();

  /// Creates a new import stream.
  /// The create implies connecting to the server to get metadata necessary to initialize the stream. This
  /// method assures there is no other stream with the same name.
  ///
  /// @param stream_name the name of the stream which can be used to uniquely identify the stream
  /// @param stream_info the necessary informations needed to create the Kafka consumer and transform the messages
  ///
  /// @throws StreamsException if the stream with the same name exists or if the creation of Kafka consumer fails
  template <typename T>
  void Create(const std::string &stream_name, typename T::StreamInfo info, std::optional<std::string> owner);

  /// Deletes an existing stream and all the data that was persisted.
  ///
  /// @param stream_name name of the stream that needs to be deleted.
  ///
  /// @throws StreamsException if the stream doesn't exist or if the persisted metadata can't be deleted.
  void Drop(const std::string &stream_name);

  /// Start consuming from a stream.
  ///
  /// @param stream_name name of the stream that needs to be started
  ///
  /// @throws StreamsException if the stream doesn't exist or if the metadata cannot be persisted
  /// @throws ConsumerRunningException if the consumer is already running
  void Start(const std::string &stream_name);

  /// Stop consuming from a stream.
  ///
  /// @param stream_name name of the stream that needs to be stopped
  ///
  /// @throws StreamsException if the stream doesn't exist or if the metadata cannot be persisted
  /// @throws ConsumerStoppedException if the consumer is already stopped
  void Stop(const std::string &stream_name);

  /// Start consuming from all streams that are stopped.
  ///
  /// @throws StreamsException if the metadata cannot be persisted
  void StartAll();

  /// Stop consuming from all streams that are running.
  ///
  /// @throws StreamsException if the metadata cannot be persisted
  void StopAll();

  /// Return current status for all streams.
  /// It might happend that the is_running field is out of date if the one of the streams stops during the invocation of
  /// this function because of an error.
  std::vector<StreamStatus<>> GetStreamInfo() const;

  /// Do a dry-run consume from a stream.
  ///
  /// @param stream_name name of the stream we want to test
  /// @param batch_limit number of batches we want to test before stopping
  ///
  /// @returns A vector of vectors of TypedValue. Each subvector contains two elements, the query string and the
  /// nullable parameters map.
  ///
  /// @throws StreamsException if the stream doesn't exist
  /// @throws ConsumerRunningException if the consumer is alredy running
  /// @throws ConsumerCheckFailedException if the transformation function throws any std::exception during processing
  TransformationResult Check(const std::string &stream_name,
                             std::optional<std::chrono::milliseconds> timeout = std::nullopt,
                             std::optional<int64_t> batch_limit = std::nullopt) const;

  /// Return the configuration value passed to memgraph.
  std::string_view BootstrapServers() const;

 private:
  using StreamDataVariant = std::variant<StreamData<KafkaStream>>;
  using StreamsMap = std::unordered_map<std::string, StreamDataVariant>;
  using SynchronizedStreamsMap = utils::Synchronized<StreamsMap, utils::WritePrioritizedRWLock>;

  template <typename T>
  StreamsMap::iterator CreateConsumer(StreamsMap &map, const std::string &stream_name,
                                      typename T::StreamInfo stream_info, std::optional<std::string> owner);

  template <typename T>
  void Persist(StreamStatus<T> &&status) {
    const std::string stream_name = status.name;
    if (!storage_.Put(stream_name, nlohmann::json(std::move(status)).dump())) {
      throw StreamsException{"Couldn't persist steam data for stream '{}'", stream_name};
    }
  }

  InterpreterContext *interpreter_context_;
  std::string bootstrap_servers_;
  kvstore::KVStore storage_;

  SynchronizedStreamsMap streams_;
};

}  // namespace query
