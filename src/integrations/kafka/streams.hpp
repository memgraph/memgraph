/// @file
#pragma once

#include "integrations/kafka/consumer.hpp"

#include <experimental/optional>
#include <mutex>
#include <unordered_map>

#include "storage/kvstore.hpp"

namespace integrations::kafka {

/// Manages kafka consumers.
///
/// This class is responsible for all query supported actions to happen.
class Streams final {
 public:
  /// Initialize streams.
  ///
  /// @param streams_directory path on the filesystem where the streams metadata
  ///        will be persisted and where the transform scripts will be
  ///        downloaded
  /// @param stream_writer lambda that knows how to write data to the db
  Streams(const std::string &streams_directory,
          std::function<
              void(const std::string &,
                   const std::map<std::string, communication::bolt::Value> &)>
              stream_writer);

  /// Looks for persisted metadata and tries to recover consumers.
  ///
  /// @throws TransformScriptNotFoundException if the transform script is
  //          missing
  /// @throws StreamDeserializationException  if the metadata can't be recovered
  void Recover();

  /// Creates a new import stream.
  /// This method makes sure there is no other stream with the same name,
  /// downloads the given transform script and writes metadata to persisted
  /// store.
  ///
  /// @param info StreamInfo struct with necessary data for a kafka consumer.
  /// @param download_transform_script Denote whether or not the transform
  ///        script should be downloaded.
  ///
  /// @throws StreamExistsException if the stream with the same name exists
  /// @throws StreamMetadataCouldNotBeStored if it can't persist metadata
  /// @throws TransformScriptCouldNotBeCreatedException if the script could not
  ///         be created
  void Create(const StreamInfo &info, bool download_transform_script = true);

  /// Deletes an existing stream and all the data that was persisted.
  ///
  /// @param stream_name name of the stream that needs to be deleted.
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
  /// @throws StreamMetadataCouldNotBeDeleted if the persisted metadata can't be
  ///         delteed
  /// @throws TransformScriptNotFoundException if the transform script can't be
  ///         deleted
  void Drop(const std::string &stream_name);

  /// Start consuming from a stream.
  ///
  /// @param stream_name name of the stream we want to start consuming
  /// @param batch_limit number of batches we want to import before stopping
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
  /// @throws StreamMetadataCouldNotBeStored if it can't persist metadata
  void Start(const std::string &stream_name,
             std::experimental::optional<int64_t> batch_limit =
                 std::experimental::nullopt);

  /// Stop consuming from a stream.
  ///
  /// @param stream_name name of the stream we wanto to stop consuming
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
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
  /// @returns A vector of pairs consisting of the query (std::string) and its
  ///          parameters (std::map<std::string, communication::bolt::Value).
  ///
  /// @throws StreamDoesntExistException if the stream doesn't exist
  std::vector<
      std::pair<std::string, std::map<std::string, communication::bolt::Value>>>
  Test(const std::string &stream_name,
       std::experimental::optional<int64_t> batch_limit =
           std::experimental::nullopt);

 private:
  std::string streams_directory_;
  /// Custom lambda that "knows" how to execute queries.
  std::function<void(const std::string &,
                     const std::map<std::string, communication::bolt::Value> &)>
      stream_writer_;

  /// Key value storage used as a persistent storage for stream metadata.
  storage::KVStore metadata_store_;

  std::mutex mutex_;
  std::unordered_map<std::string, Consumer> consumers_;

  std::string GetTransformScriptDir();
  std::string GetTransformScriptPath(const std::string &stream_name);
};

}  // namespace integrations::kafka
