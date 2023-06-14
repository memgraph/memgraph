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

#include "integrations/kafka/consumer.hpp"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <unordered_set>

#include <librdkafka/rdkafkacpp.h>
#include <spdlog/spdlog.h>

#include "integrations/constants.hpp"
#include "integrations/kafka/exceptions.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/thread.hpp"

namespace memgraph::integrations::kafka {

namespace {
utils::BasicResult<std::string, std::vector<Message>> GetBatch(RdKafka::KafkaConsumer &consumer,
                                                               const ConsumerInfo &info,
                                                               std::atomic<bool> &is_running) {
  std::vector<Message> batch{};

  batch.reserve(info.batch_size);

  auto remaining_timeout_in_ms = info.batch_interval.count();
  auto start = std::chrono::steady_clock::now();

  bool run_batch = true;
  for (int64_t i = 0; remaining_timeout_in_ms > 0 && i < info.batch_size && is_running.load(); ++i) {
    std::unique_ptr<RdKafka::Message> msg(consumer.consume(remaining_timeout_in_ms));
    switch (msg->err()) {
      case RdKafka::ERR__TIMED_OUT:
        run_batch = false;
        break;

      case RdKafka::ERR_NO_ERROR:
        batch.emplace_back(std::move(msg));
        break;
      case RdKafka::ERR__MAX_POLL_EXCEEDED:
        // max.poll.interval.ms reached between two calls of poll, just continue
        spdlog::info("Consumer {} reached the max.poll.interval.ms.", info.consumer_name);
        break;
      default:
        auto error = msg->errstr();
        spdlog::warn("Unexpected error while consuming message in consumer {}, error: {} (code {})!",
                     info.consumer_name, msg->errstr(), msg->err());
        return {std::move(error)};
    }

    if (!run_batch) {
      break;
    }

    auto now = std::chrono::steady_clock::now();
    auto took = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
    remaining_timeout_in_ms = remaining_timeout_in_ms - took.count();
    start = now;
  }

  return std::move(batch);
}

void CheckAndDestroyLastAssignmentIfNeeded(RdKafka::KafkaConsumer &consumer, const ConsumerInfo &info,
                                           std::vector<RdKafka::TopicPartition *> &last_assignment) {
  if (!last_assignment.empty()) {
    if (const auto err = consumer.assign(last_assignment); err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerStartFailedException(info.consumer_name,
                                         fmt::format("Couldn't restore committed offsets: '{}'", RdKafka::err2str(err)));
    }
    RdKafka::TopicPartition::destroy(last_assignment);
  }
}

void TryToConsumeBatch(RdKafka::KafkaConsumer &consumer, const ConsumerInfo &info,
                       const ConsumerFunction &consumer_function, const std::vector<Message> &batch) {
  consumer_function(batch);
  std::vector<RdKafka::TopicPartition *> partitions;
  utils::OnScopeExit clear_partitions([&]() { RdKafka::TopicPartition::destroy(partitions); });

  if (const auto err = consumer.assignment(partitions); err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerCommitFailedException(
        info.consumer_name, fmt::format("Couldn't get assignment to commit offsets: {}", RdKafka::err2str(err)));
  }
  if (const auto err = consumer.position(partitions); err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerCommitFailedException(info.consumer_name,
                                        fmt::format("Couldn't get offsets from librdkafka {}", RdKafka::err2str(err)));
  }
  if (const auto err = consumer.commitSync(partitions); err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerCommitFailedException(info.consumer_name, RdKafka::err2str(err));
  }
}
}  // namespace

Message::Message(std::unique_ptr<RdKafka::Message> &&message) : message_{std::move(message)} {
  // Because of these asserts, the message can be safely accessed in the member function functions, because it cannot
  // be null and always points to a valid message (not to a wrapped error)
  MG_ASSERT(message_.get() != nullptr, "Kafka message cannot be null!");
  MG_ASSERT(message_->err() == 0 && message_->c_ptr() != nullptr, "Invalid kafka message!");
};

std::span<const char> Message::Key() const {
  const auto *c_message = message_->c_ptr();
  return {static_cast<const char *>(c_message->key), c_message->key_len};
}

std::string_view Message::TopicName() const {
  const auto *c_message = message_->c_ptr();
  return c_message->rkt == nullptr ? std::string_view{} : rd_kafka_topic_name(c_message->rkt);
}

std::span<const char> Message::Payload() const {
  const auto *c_message = message_->c_ptr();
  return {static_cast<const char *>(c_message->payload), c_message->len};
}

int64_t Message::Timestamp() const {
  const auto *c_message = message_->c_ptr();
  return rd_kafka_message_timestamp(c_message, nullptr);
}

int64_t Message::Offset() const {
  const auto *c_message = message_->c_ptr();
  return c_message->offset;
}

Consumer::Consumer(ConsumerInfo info, ConsumerFunction consumer_function)
    : info_{std::move(info)}, consumer_function_(std::move(consumer_function)), cb_(info_.consumer_name) {
  MG_ASSERT(consumer_function_, "Empty consumer function for Kafka consumer");
  // NOLINTNEXTLINE (modernize-use-nullptr)
  if (info_.batch_interval < kMinimumInterval) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, "Batch interval has to be positive!");
  }
  if (info_.batch_size < kMinimumSize) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, "Batch size has to be positive!");
  }

  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (conf == nullptr) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, "Couldn't create Kafka configuration!");
  }

  std::string error;

  for (const auto &[key, value] : info_.public_configs) {
    if (conf->set(key, value, error) != RdKafka::Conf::CONF_OK) {
      throw SettingCustomConfigFailed(info_.consumer_name, error, key, value);
    }
  }

  for (const auto &[key, value] : info_.private_configs) {
    if (conf->set(key, value, error) != RdKafka::Conf::CONF_OK) {
      throw SettingCustomConfigFailed(info_.consumer_name, error, key, kReducted);
    }
  }

  if (conf->set("event_cb", this, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("rebalance_cb", &cb_, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("enable.partition.eof", "false", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("enable.auto.commit", "false", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("bootstrap.servers", info_.bootstrap_servers, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("group.id", info_.consumer_group, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  consumer_ = std::unique_ptr<RdKafka::KafkaConsumer, std::function<void(RdKafka::KafkaConsumer *)>>(
      RdKafka::KafkaConsumer::create(conf.get(), error), [this](auto *consumer) {
        this->StopConsuming();
        consumer->close();
        delete consumer;
      });

  if (consumer_ == nullptr) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  RdKafka::Metadata *raw_metadata = nullptr;
  if (const auto err = consumer_->metadata(true, nullptr, &raw_metadata, 1000); err != RdKafka::ERR_NO_ERROR) {
    delete raw_metadata;
    throw ConsumerFailedToInitializeException(info_.consumer_name, RdKafka::err2str(err));
  }
  std::unique_ptr<RdKafka::Metadata> metadata(raw_metadata);

  std::unordered_set<std::string> topic_names_from_metadata{};
  std::transform(metadata->topics()->begin(), metadata->topics()->end(),
                 std::inserter(topic_names_from_metadata, topic_names_from_metadata.begin()),
                 [](const auto topic_metadata) { return topic_metadata->topic(); });

  static constexpr size_t max_topic_name_length = 249;
  static constexpr auto is_valid_topic_name = [](const auto c) {
    return std::isalnum(c) || c == '.' || c == '_' || c == '-';
  };

  for (const auto &topic_name : info_.topics) {
    if (topic_name.size() > max_topic_name_length ||
        std::any_of(topic_name.begin(), topic_name.end(), [&](const auto c) { return !is_valid_topic_name(c); })) {
      throw ConsumerFailedToInitializeException(info_.consumer_name,
                                                fmt::format("'{}' is an invalid topic name", topic_name));
    }

    if (!topic_names_from_metadata.contains(topic_name)) {
      throw TopicNotFoundException(info_.consumer_name, topic_name);
    }
  }

  if (const auto err = consumer_->subscribe(info_.topics); err != RdKafka::ERR_NO_ERROR) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, RdKafka::err2str(err));
  }
}

Consumer::~Consumer() {
  StopIfRunning();
  consumer_->close();
  RdKafka::TopicPartition::destroy(last_assignment_);
}

void Consumer::Start() {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  StartConsuming();
}

void Consumer::StartWithLimit(const uint64_t limit_batches, std::optional<std::chrono::milliseconds> timeout) const {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }
  if (limit_batches < kMinimumStartBatchLimit) {
    throw ConsumerStartFailedException(
        info_.consumer_name, fmt::format("Batch limit has to be greater than or equal to {}", kMinimumStartBatchLimit));
  }
  if (timeout.value_or(kMinimumInterval) < kMinimumInterval) {
    throw ConsumerStartFailedException(
        info_.consumer_name,
        fmt::format("Timeout has to be greater than or equal to {} milliseconds", kMinimumInterval.count()));
  }

  StartConsumingWithLimit(limit_batches, timeout);
}

void Consumer::Stop() {
  if (!is_running_) {
    throw ConsumerStoppedException(info_.consumer_name);
  }

  StopConsuming();
}

void Consumer::StopIfRunning() {
  if (is_running_) {
    StopConsuming();
  }
  if (thread_.joinable()) {
    thread_.join();
  }
}

void Consumer::Check(std::optional<std::chrono::milliseconds> timeout, std::optional<uint64_t> limit_batches,
                     const ConsumerFunction &check_consumer_function) const {
  // NOLINTNEXTLINE (modernize-use-nullptr)
  if (timeout.value_or(kMinimumInterval) < kMinimumInterval) {
    throw ConsumerCheckFailedException(info_.consumer_name, "Timeout has to be positive!");
  }
  if (limit_batches.value_or(kMinimumSize) < kMinimumSize) {
    throw ConsumerCheckFailedException(info_.consumer_name, "Batch limit has to be positive!");
  }
  // The implementation of this function is questionable: it is const qualified, though it changes the inner state of
  // KafkaConsumer. Though it changes the inner state, it saves the current assignment for future Check/Start calls to
  // restore the current state, so the changes made by this function shouldn't be visible for the users of the class. It
  // also passes a non const reference of KafkaConsumer to GetBatch function. That means the object is bitwise const
  // (KafkaConsumer is stored in unique_ptr) and internally mostly synchronized. Mostly, because as Start/Stop requires
  // exclusive access to consumer, so we don't have to deal with simultaneous calls to those functions. The only concern
  // in this function is to prevent executing this function on multiple threads simultaneously.
  if (is_running_.exchange(true)) {
    throw ConsumerRunningException(info_.consumer_name);
  }
  utils::OnScopeExit restore_is_running([this] { is_running_.store(false); });

  if (last_assignment_.empty()) {
    auto throw_consumer_check_failed = [this](const auto err) {
      throw ConsumerCheckFailedException(info_.consumer_name,
                                         fmt::format("Couldn't save committed offsets: '{}'", RdKafka::err2str(err)));
    };
    if (const auto err = consumer_->assignment(last_assignment_); err != RdKafka::ERR_NO_ERROR) {
      spdlog::warn("Saving the assignment of consumer {} failed: {}", info_.consumer_name, RdKafka::err2str(err));
      throw_consumer_check_failed(err);
    }
    if (const auto err = consumer_->position(last_assignment_); err != RdKafka::ERR_NO_ERROR) {
      spdlog::warn("Saving the position offset assignment of consumer {} failed: {}", info_.consumer_name,
                   RdKafka::err2str(err));
      throw_consumer_check_failed(err);
    }
  } else {
    if (const auto err = consumer_->assign(last_assignment_); err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerCheckFailedException(info_.consumer_name,
                                         fmt::format("Couldn't restore committed offsets: '{}'", RdKafka::err2str(err)));
    }
  }

  const auto num_of_batches = limit_batches.value_or(kDefaultCheckBatchLimit);
  const auto timeout_to_use = timeout.value_or(kDefaultCheckTimeout);
  const auto start = std::chrono::steady_clock::now();
  for (int64_t i = 0; i < num_of_batches;) {
    const auto now = std::chrono::steady_clock::now();
    // NOLINTNEXTLINE (modernize-use-nullptr)
    if (now - start >= timeout_to_use) {
      throw ConsumerCheckFailedException(info_.consumer_name, "timeout reached");
    }
    auto maybe_batch = GetBatch(*consumer_, info_, is_running_);

    if (maybe_batch.HasError()) {
      throw ConsumerCheckFailedException(info_.consumer_name, maybe_batch.GetError());
    }

    const auto &batch = maybe_batch.GetValue();

    if (batch.empty()) {
      continue;
    }
    ++i;

    try {
      check_consumer_function(batch);
    } catch (const std::exception &e) {
      spdlog::warn("Kafka consumer {} check failed with error {}", info_.consumer_name, e.what());
      throw ConsumerCheckFailedException(info_.consumer_name, e.what());
    }
  }
}

bool Consumer::IsRunning() const { return is_running_; }

const ConsumerInfo &Consumer::Info() const { return info_; }

void Consumer::event_cb(RdKafka::Event &event) {
  switch (event.type()) {
    case RdKafka::Event::Type::EVENT_ERROR:
      spdlog::warn("Kafka consumer {} received an error: {}", info_.consumer_name, RdKafka::err2str(event.err()));
      break;
    case RdKafka::Event::Type::EVENT_STATS:
    case RdKafka::Event::Type::EVENT_LOG:
    case RdKafka::Event::Type::EVENT_THROTTLE:
      break;
  }
}

void Consumer::StartConsuming() {
  MG_ASSERT(!is_running_, "Cannot start already running consumer!");

  if (thread_.joinable()) {
    // This can happen if the thread just finished its last batch, already set is_running_ to false and currently
    // shutting down.
    thread_.join();
  };

  is_running_.store(true);

  CheckAndDestroyLastAssignmentIfNeeded(*consumer_, info_, last_assignment_);

  thread_ = std::thread([this] {
    static constexpr auto kMaxThreadNameSize = utils::GetMaxThreadNameSize();
    const auto full_thread_name = "Cons#" + info_.consumer_name;

    utils::ThreadSetName(full_thread_name.substr(0, kMaxThreadNameSize));

    while (is_running_) {
      auto maybe_batch = GetBatch(*consumer_, info_, is_running_);
      if (maybe_batch.HasError()) {
        throw ConsumerReadMessagesFailedException(info_.consumer_name, maybe_batch.GetError());
      }
      const auto &batch = maybe_batch.GetValue();

      if (batch.empty()) {
        continue;
      }

      spdlog::info("Kafka consumer {} is processing a batch", info_.consumer_name);

      try {
        TryToConsumeBatch(*consumer_, info_, consumer_function_, batch);
      } catch (const std::exception &e) {
        spdlog::warn("Error happened in consumer {} while processing a batch: {}!", info_.consumer_name, e.what());
        break;
      }
      spdlog::info("Kafka consumer {} finished processing", info_.consumer_name);
    }
    is_running_.store(false);
  });
}

void Consumer::StartConsumingWithLimit(uint64_t limit_batches, std::optional<std::chrono::milliseconds> timeout) const {
  MG_ASSERT(!is_running_, "Cannot start already running consumer!");

  if (is_running_.exchange(true)) {
    throw ConsumerRunningException(info_.consumer_name);
  }
  utils::OnScopeExit restore_is_running([this] { is_running_.store(false); });

  CheckAndDestroyLastAssignmentIfNeeded(*consumer_, info_, last_assignment_);

  const auto timeout_to_use = timeout.value_or(kDefaultCheckTimeout);
  const auto start = std::chrono::steady_clock::now();

  for (uint64_t batch_count = 0; batch_count < limit_batches;) {
    const auto now = std::chrono::steady_clock::now();
    if (now - start >= timeout_to_use) {
      throw ConsumerStartFailedException(info_.consumer_name, "Timeout reached");
    }

    const auto maybe_batch = GetBatch(*consumer_, info_, is_running_);
    if (maybe_batch.HasError()) {
      throw ConsumerReadMessagesFailedException(info_.consumer_name, maybe_batch.GetError());
    }
    const auto &batch = maybe_batch.GetValue();

    if (batch.empty()) {
      continue;
    }
    ++batch_count;

    spdlog::info("Kafka consumer {} is processing a batch", info_.consumer_name);

    TryToConsumeBatch(*consumer_, info_, consumer_function_, batch);

    spdlog::info("Kafka consumer {} finished processing", info_.consumer_name);
  }
}

void Consumer::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) thread_.join();
}

utils::BasicResult<std::string> Consumer::SetConsumerOffsets(int64_t offset) {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  if (offset == -1) {
    offset = RD_KAFKA_OFFSET_BEGINNING;
  } else if (offset == -2) {
    offset = RD_KAFKA_OFFSET_END;
  }

  cb_.set_offset(offset);
  if (const auto err = consumer_->subscribe(info_.topics); err != RdKafka::ERR_NO_ERROR) {
    return fmt::format("Could not set offset of consumer: {}. Error: {}", info_.consumer_name, RdKafka::err2str(err));
  }
  return {};
}

Consumer::ConsumerRebalanceCb::ConsumerRebalanceCb(std::string consumer_name)
    : consumer_name_(std::move(consumer_name)) {}

void Consumer::ConsumerRebalanceCb::rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err,
                                                 std::vector<RdKafka::TopicPartition *> &partitions) {
  if (err == RdKafka::ERR__REVOKE_PARTITIONS) {
    consumer->unassign();
    return;
  }
  if (err != RdKafka::ERR__ASSIGN_PARTITIONS) {
    spdlog::critical("Consumer {} received an unexpected error {}", consumer_name_, RdKafka::err2str(err));
    return;
  }
  if (offset_) {
    for (auto &partition : partitions) {
      partition->set_offset(*offset_);
    }
    offset_.reset();
  }
  auto maybe_error = consumer->assign(partitions);
  if (maybe_error != RdKafka::ErrorCode::ERR_NO_ERROR) {
    spdlog::warn("Assigning offset of consumer {} failed: {}", consumer_name_, RdKafka::err2str(maybe_error));
  }
  maybe_error = consumer->commitSync(partitions);
  if (maybe_error != RdKafka::ErrorCode::ERR_NO_ERROR) {
    spdlog::warn("Committing offsets of consumer {} failed: {}", consumer_name_, RdKafka::err2str(maybe_error));
  }
}
void Consumer::ConsumerRebalanceCb::set_offset(int64_t offset) { offset_ = offset; }
}  // namespace memgraph::integrations::kafka
