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

#include <functional>
#include <map>
#include <optional>
#include <unordered_map>

#include "integrations/kafka/consumer.hpp"
#include "kvstore/kvstore.hpp"
#include "query/discard_value_stream.hpp"
#include "query/interpreter.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
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

using TransformationResult = std::vector<std::vector<TypedValue>>;
using TransformFunction = std::function<TransformationResult(const std::vector<integrations::kafka::Message> &)>;
using ConsumerFunction = std::function<void(const std::vector<integrations::kafka::Message> &)>;

// TODO(antonio2368): Add a concept
template <typename T>
using SynchronizedStreamSource = utils::Synchronized<T, utils::WritePrioritizedRWLock>;

template <typename T>
struct StreamStatus {
  std::string name;
  typename T::StreamInfo info;
  bool is_running;
};

struct KafkaStream {
  struct StreamInfo {
    std::vector<std::string> topics;
    std::string consumer_group;
    std::optional<std::chrono::milliseconds> batch_interval;
    std::optional<int64_t> batch_size;
    std::string transformation_name;
    std::string bootstrap_servers;
  };

  using Consumer = integrations::kafka::Consumer;

  template <typename T>
  KafkaStream(std::string stream_name, std::string bootstrap_servers, StreamInfo stream_info, T consumer_function) {
    integrations::kafka::ConsumerInfo consumer_info{
        .consumer_name = std::move(stream_name),
        .topics = std::move(stream_info.topics),
        .consumer_group = std::move(stream_info.consumer_group),
        .batch_interval = stream_info.batch_interval,
        .batch_size = stream_info.batch_size,
    };

    consumer_ = std::make_unique<Consumer>(std::move(bootstrap_servers), std::move(consumer_info),
                                           std::move(consumer_function));
  }

  StreamStatus<KafkaStream> CreateStatus(const std::string &name, const std::string &transformation_name,
                                         std::optional<std::string> owner) const {
    const auto &info = consumer_->Info();
    return {name,
            StreamInfo{
                info.topics,
                info.consumer_group,
                info.batch_interval,
                info.batch_size,
                transformation_name,
            },
            consumer_->IsRunning()};
  }

  void Start() const { consumer_->Start(); }
  void Stop() const { consumer_->Stop(); }
  bool IsRunning() const { return consumer_->IsRunning(); }

  void Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> batch_limit,
             const ConsumerFunction &consumer_function) const {
    consumer_->Check(timeout, batch_limit, consumer_function);
  }

  std::unique_ptr<Consumer> consumer_;
};

// TODO(antonio2368): Add a concept
template <typename T>
struct StreamData {
  std::string transformation_name;
  std::optional<std::string> owner;
  SynchronizedStreamSource<T> stream_source;
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
  void Create(const std::string &stream_name, typename T::StreamInfo info, std::optional<std::string> owner) {
    auto locked_streams = streams_.Lock();
    auto it = CreateConsumer<T>(*locked_streams, stream_name, std::move(info), std::move(owner));

    try {
      std::visit(
          [&, this](auto &&stream_data) {
            const auto stream_source_ptr = stream_data.stream_source.ReadLock();
            Persist(stream_source_ptr.CreateStatus(stream_name, it->second.transformation_name, it->second.owner));
          },
          it->second);
    } catch (...) {
      locked_streams->erase(it);
      throw;
    }
  }

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
  std::vector<StreamStatus<KafkaStream>> GetStreamInfo() const;

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
                                      typename T::StreamInfo stream_info, std::optional<std::string> owner) {
    if (map.contains(stream_name)) {
      throw StreamsException{"Stream already exists with name '{}'", stream_name};
    }

    auto *memory_resource = utils::NewDeleteResource();

    auto consumer_function = [interpreter_context = interpreter_context_, memory_resource, stream_name,
                              transformation_name = stream_info.transformation_name, owner = stream_info.owner,
                              interpreter = std::make_shared<Interpreter>(interpreter_context_),
                              result = mgp_result{nullptr, memory_resource}](
                                 const std::vector<integrations::kafka::Message> &messages) mutable {
      auto accessor = interpreter_context->db->Access();
      EventCounter::IncrementCounter(EventCounter::MessagesConsumed, messages.size());
      CallCustomTransformation(transformation_name, messages, result, accessor, *memory_resource, stream_name);

      DiscardValueResultStream stream;

      spdlog::trace("Start transaction in stream '{}'", stream_name);
      utils::OnScopeExit cleanup{[&interpreter, &result]() {
        result.rows.clear();
        interpreter->Abort();
      }};
      interpreter->BeginTransaction();

      const static std::map<std::string, storage::PropertyValue> empty_parameters{};

      for (auto &row : result.rows) {
        spdlog::trace("Processing row in stream '{}'", stream_name);
        auto [query_value, params_value] =
            ExtractTransformationResult(std::move(row.values), transformation_name, stream_name);
        storage::PropertyValue params_prop{params_value};

        std::string query{query_value.ValueString()};
        spdlog::trace("Executing query '{}' in stream '{}'", query, stream_name);
        auto prepare_result =
            interpreter->Prepare(query, params_prop.IsNull() ? empty_parameters : params_prop.ValueMap(), nullptr);
        if (!interpreter_context->auth_checker->IsUserAuthorized(owner, prepare_result.privileges)) {
          throw StreamsException{
              "Couldn't execute query '{}' for stream '{}' becuase the owner is not authorized to execute the "
              "query!",
              query, stream_name};
        }
        interpreter->PullAll(&stream);
      }

      spdlog::trace("Commit transaction in stream '{}'", stream_name);
      interpreter->CommitTransaction();
      result.rows.clear();
    };

    auto bootstrap_servers =
        stream_info.bootstrap_servers.empty() ? bootstrap_servers_ : std::move(stream_info.bootstrap_servers);
    auto insert_result = map.insert_or_assign(
        stream_name,
        StreamData{std::move(stream_info.transformation_name), owner,
                   T(stream_name, std::move(bootstrap_servers), std::move(stream_info), std::move(consumer_function))});
    MG_ASSERT(insert_result.second, "Unexpected error during storing consumer '{}'", stream_name);
    return insert_result.first;
  }

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
