/// @file
#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "rdkafkacpp.h"

#include "communication/bolt/v1/value.hpp"
#include "integrations/kafka/transform.hpp"

namespace integrations {
namespace kafka {

/// StreamInfo holds all important info about a stream for memgraph.
///
/// The fields inside this struct are used for serialization and
/// deserialization.
struct StreamInfo {
  std::string stream_name;
  std::string stream_uri;
  std::string stream_topic;
  std::string transform_uri;
  std::optional<int64_t> batch_interval_in_ms;
  std::optional<int64_t> batch_size;

  std::optional<int64_t> limit_batches;

  bool is_running = false;
};

/// StreamStatus holds all important info about a stream for a user.
struct StreamStatus {
  std::string stream_name;
  std::string stream_uri;
  std::string stream_topic;
  std::string transform_uri;
  std::string stream_status;
};

/// Memgraphs kafka consumer wrapper.
///
/// Class Consumer wraps around librdkafka Consumer so it's easier to use it.
/// It extends RdKafka::EventCb in order to listen to error events.
class Consumer final : public RdKafka::EventCb {
 public:
  Consumer() = delete;

  /// Creates a new consumer with the given parameters.
  ///
  /// @param info necessary info about a stream
  /// @param script_path path on the filesystem where the transform script
  ///        is stored
  /// @param stream_writer custom lambda that knows how to write data to the
  ///        db
  //
  /// @throws ConsumerFailedToInitializeException if the consumer can't connect
  ///         to the Kafka endpoint.
  Consumer(const StreamInfo &info, const std::string &transform_script_path,
           std::function<
               void(const std::string &,
                    const std::map<std::string, communication::bolt::Value> &)>
               stream_writer);

  Consumer(const Consumer &other) = delete;
  Consumer(Consumer &&other) = delete;

  Consumer &operator=(const Consumer &other) = delete;
  Consumer &operator=(Consumer &&other) = delete;

  /// Starts importing data from a stream to the db.
  /// This method will start a new thread which does the import.
  ///
  /// @param limit_batches if present, the consumer will only import the given
  ///        number of batches in the db, and stop afterwards.
  ///
  /// @throws ConsumerNotAvailableException if the consumer isn't initialized
  /// @throws ConsumerRunningException if the consumer is already running
  void Start(std::optional<int64_t> limit_batches);

  /// Stops importing data from a stream to the db.
  ///
  /// @throws ConsumerNotAvailableException if the consumer isn't initialized
  /// @throws ConsumerStoppedException if the consumer is already stopped
  void Stop();

  /// Starts importing importing from a stream only if the stream is stopped.
  ///
  /// @throws ConsumerNotAvailableException if the consumer isn't initialized
  void StartIfStopped();

  /// Stops importing from a stream only if the stream is running.
  ///
  /// @throws ConsumerNotAvailableException if the consumer isn't initialized
  void StopIfRunning();

  /// Performs a dry-run on a given stream.
  ///
  /// @param limit_batches the consumer will only test on the given number of
  ///        batches. If not present, a default value is used.
  ///
  /// @throws ConsumerNotAvailableException if the consumer isn't initialized
  /// @throws ConsumerRunningException if the consumer is alredy running.
  std::vector<
      std::pair<std::string, std::map<std::string, communication::bolt::Value>>>
  Test(std::optional<int64_t> limit_batches);

  /// Returns the current status of a stream.
  StreamStatus Status();

  /// Returns the info of a stream.
  StreamInfo Info();

 private:
  StreamInfo info_;
  std::string transform_script_path_;
  std::function<void(const std::string &,
                     const std::map<std::string, communication::bolt::Value> &)>
      stream_writer_;

  std::atomic<bool> is_running_{false};
  std::atomic<bool> transform_alive_{false};
  std::thread thread_;

  std::unique_ptr<RdKafka::KafkaConsumer,
                  std::function<void(RdKafka::KafkaConsumer *)>>
      consumer_;

  void event_cb(RdKafka::Event &event) override;

  void StopConsuming();

  void StartConsuming(std::optional<int64_t> limit_batches);

  std::vector<std::unique_ptr<RdKafka::Message>> GetBatch();
};

}  // namespace kafka
}  // namespace integrations
