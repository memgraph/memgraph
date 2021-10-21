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

#include "integrations/kafka/consumer.hpp"

#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <unordered_set>

#include <librdkafka/rdkafkacpp.h>
#include <spdlog/spdlog.h>
#include "integrations/kafka/exceptions.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/thread.hpp"

namespace integrations::kafka {

constexpr std::chrono::milliseconds kDefaultBatchInterval{100};
constexpr int64_t kDefaultBatchSize = 1000;
constexpr int64_t kDefaultCheckBatchLimit = 1;
constexpr std::chrono::milliseconds kDefaultCheckTimeout{30000};
constexpr std::chrono::milliseconds kMinimumInterval{1};
constexpr int64_t kMinimumSize{1};

namespace {
utils::BasicResult<std::string, std::pair<int64_t, std::vector<Message>>> GetBatch(RdKafka::KafkaConsumer &consumer,
                                                                                   const ConsumerInfo &info,
                                                                                   std::atomic<bool> &is_running) {
  std::vector<Message> batch{};

  int64_t batch_size = info.batch_size.value_or(kDefaultBatchSize);
  batch.reserve(batch_size);

  auto remaining_timeout_in_ms = info.batch_interval.value_or(kDefaultBatchInterval).count();
  auto start = std::chrono::steady_clock::now();

  bool run_batch = true;
  int64_t offset = 0;
  for (int64_t i = 0; remaining_timeout_in_ms > 0 && i < batch_size && is_running.load(); ++i) {
    std::unique_ptr<RdKafka::Message> msg(consumer.consume(remaining_timeout_in_ms));
    switch (msg->err()) {
      case RdKafka::ERR__TIMED_OUT:
        run_batch = false;
        break;

      case RdKafka::ERR_NO_ERROR:
        offset = msg->offset();
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

  return std::make_pair(offset, std::move(batch));
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

Consumer::Consumer(const std::string &bootstrap_servers, ConsumerInfo info, ConsumerFunction consumer_function)
    : info_{std::move(info)}, consumer_function_(std::move(consumer_function)) {
  MG_ASSERT(consumer_function_, "Empty consumer function for Kafka consumer");
  // NOLINTNEXTLINE (modernize-use-nullptr)
  if (info.batch_interval.value_or(kMinimumInterval) < kMinimumInterval) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, "Batch interval has to be positive!");
  }
  if (info.batch_size.value_or(kMinimumSize) < kMinimumSize) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, "Batch size has to be positive!");
  }

  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  if (conf == nullptr) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, "Couldn't create Kafka configuration!");
  }

  std::string error;

  if (conf->set("event_cb", this, error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("enable.partition.eof", "false", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("enable.auto.commit", "false", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("enable.auto.offset.store", "false", error) != RdKafka::Conf::CONF_OK) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, error);
  }

  if (conf->set("bootstrap.servers", bootstrap_servers, error) != RdKafka::Conf::CONF_OK) {
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

  for (const auto &topic_name : info_.topics) {
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

void Consumer::StartIfStopped() {
  if (!is_running_) {
    StartConsuming();
  }
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

void Consumer::Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> limit_batches,
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
    if (const auto err = consumer_->assignment(last_assignment_); err != RdKafka::ERR_NO_ERROR) {
      spdlog::warn("Saving the commited offset of consumer {} failed: {}", info_.consumer_name, RdKafka::err2str(err));
      throw ConsumerCheckFailedException(info_.consumer_name,
                                         fmt::format("Couldn't save commited offsets: '{}'", RdKafka::err2str(err)));
    }
  } else {
    if (const auto err = consumer_->assign(last_assignment_); err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerCheckFailedException(info_.consumer_name,
                                         fmt::format("Couldn't restore commited offsets: '{}'", RdKafka::err2str(err)));
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

    if (batch.second.empty()) {
      continue;
    }
    ++i;

    try {
      check_consumer_function(batch.second);
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

  if (!last_assignment_.empty()) {
    if (const auto err = consumer_->assign(last_assignment_); err != RdKafka::ERR_NO_ERROR) {
      throw ConsumerStartFailedException(info_.consumer_name,
                                         fmt::format("Couldn't restore commited offsets: '{}'", RdKafka::err2str(err)));
    }
    RdKafka::TopicPartition::destroy(last_assignment_);
  }

  thread_ = std::thread([this] {
    constexpr auto kMaxThreadNameSize = utils::GetMaxThreadNameSize();
    const auto full_thread_name = "Cons#" + info_.consumer_name;

    utils::ThreadSetName(full_thread_name.substr(0, kMaxThreadNameSize));

    while (is_running_) {
      auto maybe_batch = GetBatch(*consumer_, info_, is_running_);
      if (maybe_batch.HasError()) {
        spdlog::warn("Error happened in consumer {} while fetching messages: {}!", info_.consumer_name,
                     maybe_batch.GetError());
        break;
      }
      const auto &batch = maybe_batch.GetValue();

      if (batch.second.empty()) continue;

      spdlog::info("Kafka consumer {} is processing a batch", info_.consumer_name);

      try {
        consumer_function_(batch.second);
        std::vector<RdKafka::TopicPartition *> partitions;
        if (const auto err = consumer_->assignment(partitions); err != RdKafka::ERR_NO_ERROR) {
          spdlog::warn("Saving the commited offset of consumer {} failed: {}", info_.consumer_name,
                       RdKafka::err2str(err));
          throw ConsumerCheckFailedException(
              info_.consumer_name, fmt::format("Couldn't save commited offsets: '{}'", RdKafka::err2str(err)));
        }

        for (auto *partition : partitions) {
          partition->set_offset(batch.first + 1);
        }

        if (const auto err = consumer_->offsets_store(partitions); err != RdKafka::ERR_NO_ERROR) {
          spdlog::warn("store offset of consumer {} failed: {}", info_.consumer_name, RdKafka::err2str(err));
        }
        if (const auto err = consumer_->commitSync(partitions); err != RdKafka::ERR_NO_ERROR) {
          spdlog::warn("Committing offset of consumer {} failed: {}", info_.consumer_name, RdKafka::err2str(err));
          break;
        }
      } catch (const std::exception &e) {
        spdlog::warn("Error happened in consumer {} while processing a batch: {}!", info_.consumer_name, e.what());
        break;
      }
      spdlog::info("Kafka consumer {} finished processing", info_.consumer_name);
    }
    is_running_.store(false);
  });
}

void Consumer::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) thread_.join();
}

std::string Consumer::SetConsumerOffsets(const std::string_view stream_name, int64_t offset) {
  std::vector<RdKafka::TopicPartition *> partitions;
  auto maybe_error = consumer_->assignment(partitions);
  if (maybe_error != RdKafka::ErrorCode::ERR_NO_ERROR) {
    return fmt::format("Can't access assigned topic partitions to the consumer: {}", maybe_error);
  }

  std::vector<std::unique_ptr<RdKafka::TopicPartition>> owners(partitions.begin(), partitions.end());
  if (offset == -1) {
    offset = RD_KAFKA_OFFSET_BEGINNING;
  }
  // set partition id's
  for (auto *partition : partitions) {
    partition->set_offset(offset);
    consumer_->seek(*partition, 0);
  }

  return "";
}
}  // namespace integrations::kafka
