/// @file
#pragma once

#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <unordered_map>

#include "integrations/kafka/consumer.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/exceptions.hpp"

namespace query {

class StreamsException : public utils::BasicException {
 public:
  using BasicException::BasicException;
};

// TODO(antaljanosbenjamin) Replace this with mgp_trans related thing
using TransformationResult = std::map<std::string, std::string>;
using TransformFunction = std::function<TransformationResult(const std::vector<integrations::kafka::Message> &)>;

struct StreamStatus {
  std::string name;
  std::vector<std::string> topics;
  std::string consumer_group;
  std::optional<std::chrono::milliseconds> batch_interval;
  std::optional<int64_t> batch_size;
  bool is_running;
  // TODO(antaljanosbenjamin) How to reference the transformation in a better way?
  std::string transformation_name;
};

struct InterpreterContext;

/// Manages Kafka consumers.
///
/// This class is responsible for all query supported actions to happen.
class Streams final {
 public:
  /// Initialize streams.
  ///
  /// @param streams_directory path on the filesystem where the streams metadata will be persisted.
  ///
  /// @param transformation a function that produces queries based on the received messages

  /// Looks for persisted metadata and tries to recover consumers.
  ///
  /// @throws TransformScriptNotFoundException if the transform script is missing
  /// @throws StreamDeserializationException  if the metadata can't be recovered
  Streams(InterpreterContext *interpreter_context, std::string bootstrap_servers, std::filesystem::path directory);

  void RestoreStreams();

  /// Creates a new import stream.
  /// This method makes sure there is no other stream with the same name.
  ///
  /// @param TODO(antaljanosbenjamin)
  ///
  /// @throws StreamExistsException if the stream with the same name exists
  /// @throws StreamMetadataCouldNotBeStored if it can't persist metadata
  /// @throws TransformScriptCouldNotBeCreatedException if the script could not be created
  void Create(StreamStatus stream_status);

  /// Deletes an existing stream and all the data that was persisted.
  ///
  /// @param stream_name name of the stream that needs to be deleted.
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
  /// @throws StreamMetadataCouldNotBeDeleted if the persisted metadata can't be deleted
  /// @throws TransformScriptNotFoundException if the transform script can't be deleted
  void Drop(const std::string &stream_name);

  /// Start consuming from a stream.
  ///
  /// @param stream_name name of the stream we want to start consuming
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
  /// @throws ConsumerRunningException if the consumer is already running
  /// @throws StreamMetadataCouldNotBeStored if it can't persist metadata
  void Start(const std::string &stream_name);

  /// Stop consuming from a stream.
  ///
  /// @param stream_name name of the stream we wanto to stop consuming
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
  /// @throws ConsumerStoppedException if the consumer is already stopped
  /// @throws StreamMetadataCouldNotBeStored if it can't persist metadata
  void Stop(const std::string &stream_name);

  /// Start consuming from all streams that are stopped.
  ///
  /// @throws StreamMetadataCouldNotBeStored if it can't persist metadata
  void StartAll();

  /// Stop consuming from all streams that are running.
  ///
  /// @throws StreamMetadataCouldNotBeStored if it can't persist metadata
  void StopAll();

  /// Return current status for all streams.
  std::vector<StreamStatus> Show();

  /// Do a dry-run consume from a stream.
  ///
  /// @param stream_name name of the stream we want to test
  /// @param batch_limit number of batches we want to test before stopping
  ///
  /// TODO(antaljanosbenjamin) add type of parameters
  /// @returns A vector of pairs consisting of the query (std::string) and its parameters ...
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
  TransformationResult Test(const std::string &stream_name, std::optional<int64_t> batch_limit = std::nullopt);

 private:
  struct StreamData {
    StreamStatus status;
    // TODO(antaljanosbenjamin) consider propagate_const
    std::unique_ptr<integrations::kafka::Consumer> consumer;
  };
  using StreamsMap = std::unordered_map<std::string, StreamData>;

  StreamsMap::const_iterator CreateConsumer(const std::lock_guard<std::mutex> &lock, StreamStatus stream_status);
  void Persist(const std::string &stream_name, const StreamStatus &status);
  void PersistNoThrow(const std::string &stream_name, const StreamStatus &status);

  InterpreterContext *interpreter_context_;
  std::string bootstrap_servers_;
  /// Key value storage used as a persistent storage for stream metadata.
  kvstore::KVStore storage_;
  // TODO(antaljanosbenjamin) Maybe use SkipList instead of map?
  std::mutex mutex_;
  StreamsMap streams_;
};

}  // namespace query
