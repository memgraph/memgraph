#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

namespace integrations::kafka {

/// Wraps the message returned from librdkafka.
///
/// The interface of RdKafka::Message is far from ideal, so this class provides a modern C++ wrapper to it. Some of the
/// problems of RdKafka::Message:
/// - First and foremost, RdKafka::Message might wrap a received message, or an error if something goes wrong during
///   polling. That means some of the getters cannot be called or return rubbish data when it contains an error. Message
///   ensures that the wrapped RdKafka::Message contains a valid message, and not some error.
/// - The topic_name is returned as a string, but the key is returned as a pointer to a string, because it is cached.
///   To unify them Message returns them as string_view without copying them, using the underlying C API.
/// - The payload is returned as void*, so it is better to cast it to char* as soon as possible. Returning the payload
///   as std::span also provides a more idiomatic way to communicate a byte array than returning a raw pointer and a
///   size.
class Message final {
 public:
  explicit Message(std::unique_ptr<RdKafka::Message> &&message);
  Message(Message &&) = default;
  Message &operator=(Message &&) = default;
  ~Message() = default;

  Message(const Message &) = delete;
  Message &operator=(const Message &) = delete;

  /// Returns the key of the message, might be empty.
  std::string_view Key() const;

  /// Returns the name of the topic, might be empty.
  std::string_view TopicName() const;

  /// Returns the payload.
  std::span<const char> Payload() const;

  /// Returns the timestamp of the message.
  ///
  /// The timestamp is the number of milliseconds since the epoch (UTC), or 0 if not available.
  ///
  /// The timestamp might have different semantics based on the configuration of the Kafka cluster. It can be the time
  /// of message creation or appendage to the log. Currently the Kafka integration doesn't support connections to
  /// multiple clusters, the semantics can be figured out from the configuration of the cluster, so the transformations
  /// can be implemented knowing that.
  int64_t Timestamp() const;

 private:
  std::unique_ptr<RdKafka::Message> message_;
};

using ConsumerFunction = std::function<void(const std::vector<Message> &)>;

/// ConsumerInfo holds all the information necessary to create a Consumer.
struct ConsumerInfo {
  ConsumerFunction consumer_function;
  std::string consumer_name;
  std::string bootstrap_servers;
  std::vector<std::string> topics;
  std::string consumer_group;
  std::optional<std::chrono::milliseconds> batch_interval;
  std::optional<int64_t> batch_size;
};

/// Memgraphs Kafka consumer wrapper.
///
/// Consumer wraps around librdkafka Consumer so it's easier to use it.
/// It extends RdKafka::EventCb in order to listen to error events.
class Consumer final : public RdKafka::EventCb {
 public:
  /// Creates a new consumer with the given parameters.
  ///
  /// @throws ConsumerFailedToInitializeException if the consumer can't connect
  ///         to the Kafka endpoint.
  explicit Consumer(ConsumerInfo &&info);
  ~Consumer() override = default;

  Consumer(const Consumer &other) = delete;
  Consumer(Consumer &&other) noexcept = delete;
  Consumer &operator=(const Consumer &other) = delete;
  Consumer &operator=(Consumer &&other) = delete;

  /// Starts consuming messages.
  ///
  /// This method will start a new thread which will poll all the topics for messages.
  ///
  /// @param limit_batches if present, the consumer will only consume the given number of batches and stop afterwards.
  ///
  /// @throws ConsumerRunningException if the consumer is already running
  void Start(std::optional<int64_t> limit_batches);

  /// Starts consuming messages if it is not started already.
  ///
  /// @throws ConsumerNotAvailableException if the consumer isn't initialized
  void StartIfStopped();

  /// Stops consuming messages.
  ///
  /// @throws ConsumerNotAvailableException if the consumer isn't initialized
  /// @throws ConsumerStoppedException if the consumer is already stopped
  void Stop();

  /// Stops consuming messages if it is not stopped alread.
  void StopIfRunning();

  /// Performs a synchronous dry-run.
  ///
  /// This function doesn't have any persistent effect on the consumer. The messages are fetched synchronously, so the
  /// function returns only when the test run is done, unlike Start, which returns after starting a thread.
  ///
  /// @param limit_batches the consumer will only test the given number of batches. If not present, a default value is
  ///                      used.
  /// @param test_consumer_function a function to feed the received messages in, only used during this dry-run.
  ///
  /// @throws ConsumerRunningException if the consumer is alredy running.
  void Test(std::optional<int64_t> limit_batches, const ConsumerFunction &test_consumer_function);

  /// Returns true if the consumer is actively consuming messages.
  bool IsRunning() const;

 private:
  void event_cb(RdKafka::Event &event) override;

  void StartConsuming(std::optional<int64_t> limit_batches);

  void StopConsuming();

  std::pair<std::vector<Message>, std::optional<std::string> /*error*/> GetBatch();

  // TODO(antaljanosbenjamin) Maybe split this to store only the necessary information
  ConsumerInfo info_;
  mutable std::atomic<bool> is_running_{false};
  std::optional<int64_t> limit_batches_{std::nullopt};
  std::thread thread_;
  std::unique_ptr<RdKafka::KafkaConsumer, std::function<void(RdKafka::KafkaConsumer *)>> consumer_;
};
}  // namespace integrations::kafka
