/// @file
#pragma once

#include <functional>
#include <map>
#include <optional>
#include <unordered_map>

#include "integrations/kafka/consumer.hpp"
#include "kvstore/kvstore.hpp"
#include "utils/exceptions.hpp"
#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace query {

class StreamsException : public utils::BasicException {
 public:
  using BasicException::BasicException;
};

// TODO(antaljanosbenjamin) Replace this with mgp_trans related thing
using TransformationResult = std::map<std::string, std::string>;
using TransformFunction = std::function<TransformationResult(const std::vector<integrations::kafka::Message> &)>;

struct StreamInfo {
  std::vector<std::string> topics;
  std::string consumer_group;
  std::optional<std::chrono::milliseconds> batch_interval;
  std::optional<int64_t> batch_size;
  // TODO(antaljanosbenjamin) How to reference the transformation in a better way?
  std::string transformation_name;
};

struct StreamStatus {
  std::string name;
  StreamInfo info;
  bool is_running;
};

using SynchronizedConsumer = utils::Synchronized<integrations::kafka::Consumer, utils::WritePrioritizedRWLock>;

struct StreamData {
  // TODO(antaljanosbenjamin) How to reference the transformation in a better way?
  std::string transformation_name;
  // TODO(antaljanosbenjamin) consider propagate_const
  std::unique_ptr<SynchronizedConsumer> consumer;
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
  void Create(const std::string &stream_name, StreamInfo stream_info);

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
  std::vector<StreamStatus> Show() const;

  /// Do a dry-run consume from a stream.
  ///
  /// @param stream_name name of the stream we want to test
  /// @param batch_limit number of batches we want to test before stopping
  ///
  /// TODO(antaljanosbenjamin) add type of parameters
  /// @returns A vector of pairs consisting of the query (std::string) and its parameters ...
  ///
  /// @throws StreamsException if the stream doesn't exist
  /// @throws ConsumerRunningException if the consumer is alredy running
  /// @throws ConsumerTestFailedException if the transformation function throws any std::exception during processing
  TransformationResult Test(const std::string &stream_name, std::optional<int64_t> batch_limit = std::nullopt) const;

 private:
  using StreamsMap = std::unordered_map<std::string, StreamData>;
  using SynchronizedStreamsMap = utils::Synchronized<StreamsMap, utils::WritePrioritizedRWLock>;

  static StreamStatus CreateStatus(const std::string &name, const std::string &transformation_name,
                                   const integrations::kafka::Consumer &consumer);

  StreamsMap::iterator CreateConsumer(StreamsMap &map, const std::string &stream_name, StreamInfo stream_info);

  void Persist(StreamStatus &&status);

  InterpreterContext *interpreter_context_;
  std::string bootstrap_servers_;
  kvstore::KVStore storage_;

  SynchronizedStreamsMap streams_;
};

}  // namespace query
