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

#include <concepts>
#include <functional>
#include <map>
#include <optional>
#include <type_traits>
#include <unordered_map>

#include <json/json.hpp>

#include "integrations/kafka/consumer.hpp"
#include "kvstore/kvstore.hpp"
#include "query/stream/common.hpp"
#include "query/stream/sources.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/event_counter.hpp"
#include "utils/exceptions.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

class StreamsTest;
namespace memgraph::query::stream {

class StreamsException : public utils::BasicException {
 public:
  using BasicException::BasicException;
  SPECIALIZE_GET_EXCEPTION_NAME(StreamsException)
};

template <typename T>
struct StreamInfo;

template <>
struct StreamInfo<void> {
  using Type = CommonStreamInfo;
};

template <Stream TStream>
struct StreamInfo<TStream> {
  using Type = typename TStream::StreamInfo;
};

template <typename T>
using StreamInfoType = typename StreamInfo<T>::Type;

template <typename T = void>
struct StreamStatus {
  std::string name;
  StreamSourceType type;
  bool is_running;
  StreamInfoType<T> info;
  std::optional<std::string> owner;
};

using TransformationResult = std::vector<std::vector<TypedValue>>;

struct IStreamConsumerFactory;

/// Manages Kafka consumers.
///
/// This class is responsible for all query supported actions to happen.
class Streams final {
  friend StreamsTest;

 public:
  /// Initializes the streams.
  ///
  /// @param interpreter_context context to use to run the result of transformations
  /// @param directory a directory path to store the persisted streams metadata
  explicit Streams(std::filesystem::path directory);

  /// Restores the streams from the persisted metadata.
  /// The restoration is done in a best effort manner, therefore no exception is thrown on failure, but the error is
  /// logged. If a stream was running previously, then after restoration it will be started.
  /// This function should only be called when there are no existing streams.
  template <typename TDbAccess>
  void RestoreStreams(TDbAccess db, IStreamConsumerFactory &factory);

  /// Creates a new import stream.
  /// The create implies connecting to the server to get metadata necessary to initialize the stream. This
  /// method assures there is no other stream with the same name.
  ///
  /// @param stream_name the name of the stream which can be used to uniquely identify the stream
  /// @param stream_info the necessary informations needed to create the Kafka consumer and transform the messages
  ///
  /// @throws StreamsException if the stream with the same name exists or if the creation of Kafka consumer fails
  template <Stream TStream, typename TDbAccess>
  void Create(const std::string &stream_name, typename TStream::StreamInfo info, std::optional<std::string> owner,
              TDbAccess db, IStreamConsumerFactory &factory);

  /// Deletes an existing stream and all the data that was persisted.
  ///
  /// @param stream_name name of the stream that needs to be deleted.
  ///
  /// @throws StreamsException if the stream doesn't exist or if the persisted metadata can't be deleted.
  void Drop(const std::string &stream_name);

  /// Deletes all existing streams and all the data that was persisted.
  ///
  /// @throws StreamsException if the persisted metadata can't be deleted.
  void DropAll();

  /// Start consuming from a stream.
  ///
  /// @param stream_name name of the stream that needs to be started
  ///
  /// @throws StreamsException if the stream doesn't exist or if the metadata cannot be persisted
  /// @throws ConsumerRunningException if the consumer is already running
  void Start(const std::string &stream_name);

  /// Start consuming from a stream.
  ///
  /// @param stream_name name of the stream that needs to be started
  /// @param batch_limit number of batches we want to consume before stopping
  /// @param timeout the maximum duration during which the command should run.
  ///
  /// @throws StreamsException if the stream doesn't exist
  /// @throws ConsumerRunningException if the consumer is already running
  void StartWithLimit(const std::string &stream_name, uint64_t batch_limit,
                      std::optional<std::chrono::milliseconds> timeout) const;

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
  /// @param timeout the maximum duration during which the command should run.
  ///
  /// @returns A vector of vectors of TypedValue. Each subvector contains two elements, the query string and the
  /// nullable parameters map.
  ///
  /// @throws StreamsException if the stream doesn't exist
  /// @throws ConsumerRunningException if the consumer is already running
  /// @throws ConsumerCheckFailedException if the transformation function throws any std::exception during processing
  template <typename TDbAccess>
  TransformationResult Check(const std::string &stream_name, TDbAccess db, IStreamConsumerFactory &factory,
                             std::optional<std::chrono::milliseconds> timeout = std::nullopt,
                             std::optional<uint64_t> batch_limit = std::nullopt) const;

 private:
  template <Stream TStream>
  using SynchronizedStreamSource = utils::Synchronized<TStream, utils::WritePrioritizedRWLock>;

  template <Stream TStream>
  struct StreamData {
    using stream_t = TStream;

    std::string transformation_name;
    std::optional<std::string> owner;
    std::unique_ptr<SynchronizedStreamSource<TStream>> stream_source;
  };

  using StreamDataVariant = std::variant<StreamData<KafkaStream>, StreamData<PulsarStream>>;
  using StreamsMap = std::unordered_map<std::string, StreamDataVariant>;
  using SynchronizedStreamsMap = utils::Synchronized<StreamsMap, utils::WritePrioritizedRWLock>;

  template <Stream TStream, typename TDbAccess>
  StreamsMap::iterator CreateConsumer(StreamsMap &map, const std::string &stream_name,
                                      typename TStream::StreamInfo stream_info, std::optional<std::string> owner,
                                      TDbAccess db, IStreamConsumerFactory &factory);

  template <Stream TStream>
  void Persist(StreamStatus<TStream> &&status) {
    const std::string stream_name = status.name;
    if (!storage_.Put(stream_name, nlohmann::json(std::move(status)).dump())) {
      throw StreamsException{"Couldn't persist stream data for stream '{}'", stream_name};
    }
  }

  void RegisterProcedures();
  void RegisterKafkaProcedures();
  void RegisterPulsarProcedures();

  kvstore::KVStore storage_;

  SynchronizedStreamsMap streams_;
};

}  // namespace memgraph::query::stream
