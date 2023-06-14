// Copyright 2022 Memgraph Ltd.
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

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include "utils/result.hpp"

namespace memgraph::integrations::kafka {

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
  std::span<const char> Key() const;

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

  /// Returns the offset of the message
  int64_t Offset() const;

 private:
  std::unique_ptr<RdKafka::Message> message_;
};

using ConsumerFunction = std::function<void(const std::vector<Message> &)>;

/// ConsumerInfo holds all the information necessary to create a Consumer.
struct ConsumerInfo {
  std::string consumer_name;
  std::vector<std::string> topics;
  std::string consumer_group;
  std::string bootstrap_servers;
  std::chrono::milliseconds batch_interval;
  int64_t batch_size;
  std::unordered_map<std::string, std::string> public_configs;
  std::unordered_map<std::string, std::string> private_configs;
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
  Consumer(ConsumerInfo info, ConsumerFunction consumer_function);
  ~Consumer() override;

  Consumer(const Consumer &other) = delete;
  Consumer(Consumer &&other) noexcept = delete;
  Consumer &operator=(const Consumer &other) = delete;
  Consumer &operator=(Consumer &&other) = delete;

  /// Starts consuming messages.
  ///
  /// This method will start a new thread which will poll all the topics for messages.
  ///
  /// @throws ConsumerRunningException if the consumer is already running
  /// @throws ConsumerStartFailedException if the committed offsets cannot be restored
  void Start();

  /// Starts consuming messages.
  ///
  /// This method will start a new thread which will poll all the topics for messages.
  ///
  /// @param limit_batches the consumer will only consume the given number of batches.
  /// @param timeout the maximum duration during which the command should run.
  ///
  /// @throws ConsumerRunningException if the consumer is already running
  /// @throws ConsumerStartFailedException if the committed offsets cannot be restored
  void StartWithLimit(uint64_t limit_batches, std::optional<std::chrono::milliseconds> timeout) const;

  /// Stops consuming messages.
  ///
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
  /// @param check_consumer_function a function to feed the received messages in, only used during this dry-run.
  ///
  /// @throws ConsumerRunningException if the consumer is already running.
  /// @throws ConsumerCheckFailedException if check isn't successful.
  void Check(std::optional<std::chrono::milliseconds> timeout, std::optional<uint64_t> limit_batches,
             const ConsumerFunction &check_consumer_function) const;

  /// Returns true if the consumer is actively consuming messages.
  bool IsRunning() const;

  /// Sets the consumer's offset.
  ///
  /// This function returns the empty string on success or an error message otherwise.
  ///
  /// @param offset: the offset to set.
  [[nodiscard]] utils::BasicResult<std::string> SetConsumerOffsets(int64_t offset);

  const ConsumerInfo &Info() const;

 private:
  void event_cb(RdKafka::Event &event) override;

  void StartConsuming();
  void StartConsumingWithLimit(uint64_t limit_batches, std::optional<std::chrono::milliseconds> timeout) const;

  void StopConsuming();

  class ConsumerRebalanceCb : public RdKafka::RebalanceCb {
   public:
    ConsumerRebalanceCb(std::string consumer_name);

    void rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
                      std::vector<RdKafka::TopicPartition *> &partitions) override final;

    void set_offset(int64_t offset);

   private:
    std::optional<int64_t> offset_;
    std::string consumer_name_;
  };

  ConsumerInfo info_;
  ConsumerFunction consumer_function_;
  mutable std::atomic<bool> is_running_{false};
  mutable std::vector<RdKafka::TopicPartition *> last_assignment_;  // Protected by is_running_
  std::unique_ptr<RdKafka::KafkaConsumer, std::function<void(RdKafka::KafkaConsumer *)>> consumer_;
  std::thread thread_;
  ConsumerRebalanceCb cb_;
};
}  // namespace memgraph::integrations::kafka
