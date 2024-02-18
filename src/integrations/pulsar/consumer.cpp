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

#include "integrations/pulsar/consumer.hpp"

#include <algorithm>
#include <chrono>
#include <thread>

#include <pulsar/Client.h>
#include <pulsar/InitialPosition.h>

#include "integrations/constants.hpp"
#include "integrations/pulsar/exceptions.hpp"
#include "integrations/pulsar/fmt.hpp"
#include "utils/concepts.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/result.hpp"
#include "utils/thread.hpp"

namespace memgraph::integrations::pulsar {

namespace {

template <typename T>
concept PulsarConsumer = utils::SameAsAnyOf<T, pulsar_client::Consumer, pulsar_client::Reader>;

template <typename TFunc>
concept PulsarMessageGetter =
    std::same_as<const pulsar_client::Message &, std::invoke_result_t<TFunc, const Message &>>;

pulsar_client::Result ConsumeMessage(pulsar_client::Consumer &consumer, pulsar_client::Message &message,
                                     int remaining_timeout_in_ms) {
  return consumer.receive(message, remaining_timeout_in_ms);
}

pulsar_client::Result ConsumeMessage(pulsar_client::Reader &reader, pulsar_client::Message &message,
                                     int remaining_timeout_in_ms) {
  return reader.readNext(message, remaining_timeout_in_ms);
}

template <PulsarConsumer TConsumer>
utils::BasicResult<std::string, std::vector<Message>> GetBatch(TConsumer &consumer, const ConsumerInfo &info,
                                                               std::atomic<bool> &is_running,
                                                               const pulsar_client::MessageId &last_message_id) {
  std::vector<Message> batch{};

  batch.reserve(info.batch_size);

  auto remaining_timeout_in_ms = info.batch_interval.count();
  auto start = std::chrono::steady_clock::now();

  while (remaining_timeout_in_ms > 0 && batch.size() < info.batch_size && is_running) {
    pulsar_client::Message message;
    const auto result = ConsumeMessage(consumer, message, remaining_timeout_in_ms);
    switch (result) {
      case pulsar_client::Result::ResultTimeout:
        return std::move(batch);
      case pulsar_client::Result::ResultOk:
        if (message.getMessageId() != last_message_id) {
          batch.emplace_back(std::move(message));
        }
        break;
      default:
        spdlog::warn(fmt::format("Unexpected error while consuming message from consumer {}, error: {}",
                                 info.consumer_name, result));
        return {pulsar_client::strResult(result)};
    }

    auto now = std::chrono::steady_clock::now();
    auto took = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
    remaining_timeout_in_ms = remaining_timeout_in_ms - took.count();
    start = now;
  }

  return std::move(batch);
}

class SpdlogLogger : public pulsar_client::Logger {
  bool isEnabled(Level /*level*/) override { return spdlog::should_log(spdlog::level::trace); }

  void log(Level /*level*/, int /*line*/, const std::string &message) override {
    spdlog::trace("[Pulsar] {}", message);
  }
};

class SpdlogLoggerFactory : public pulsar_client::LoggerFactory {
  pulsar_client::Logger *getLogger(const std::string & /*file_name*/) override { return new SpdlogLogger; }
};

pulsar_client::Client CreateClient(const std::string &service_url) {
  pulsar_client::ClientConfiguration conf;
  conf.setLogger(new SpdlogLoggerFactory);
  return {service_url, conf};
}

template <PulsarConsumer TConsumer, PulsarMessageGetter TPulsarMessageGetter>
void TryToConsumeBatch(TConsumer &consumer, const ConsumerInfo &info, const ConsumerFunction &consumer_function,
                       pulsar_client::MessageId &last_message_id, const std::vector<Message> &batch,
                       const TPulsarMessageGetter &message_getter) {
  consumer_function(batch);

  auto has_message_failed = [&consumer, &info, &last_message_id, &message_getter](const auto &message) {
    if (const auto result = consumer.acknowledge(message_getter(message)); result != pulsar_client::ResultOk) {
      spdlog::warn("Acknowledging a message of consumer {} failed: {}", info.consumer_name, result);
      return true;
    }
    last_message_id = message_getter(message).getMessageId();
    return false;
  };

  if (std::ranges::any_of(batch, has_message_failed)) {
    throw ConsumerAcknowledgeMessagesFailedException(info.consumer_name);
  }
}
}  // namespace

Message::Message(pulsar_client::Message &&message) : message_{std::move(message)} {}

std::span<const char> Message::Payload() const {
  return {static_cast<const char *>(message_.getData()), message_.getLength()};
}

std::string_view Message::TopicName() const { return message_.getTopicName(); }

Consumer::Consumer(ConsumerInfo info, ConsumerFunction consumer_function)
    : info_{std::move(info)},
      client_{CreateClient(info_.service_url)},
      consumer_function_{std::move(consumer_function)} {
  pulsar_client::ConsumerConfiguration config;
  config.setSubscriptionInitialPosition(pulsar_client::InitialPositionLatest);
  config.setConsumerType(pulsar_client::ConsumerType::ConsumerExclusive);
  if (pulsar_client::Result result = client_.subscribe(info_.topics, info_.consumer_name, config, consumer_);
      result != pulsar_client::ResultOk) {
    throw ConsumerFailedToInitializeException(info_.consumer_name, pulsar_client::strResult(result));
  }
}
Consumer::~Consumer() {
  StopIfRunning();
  consumer_.close();
  client_.close();
}

bool Consumer::IsRunning() const { return is_running_; }

const ConsumerInfo &Consumer::Info() const { return info_; }

void Consumer::Start() {
  if (is_running_) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  StartConsuming();
}

void Consumer::StartWithLimit(const uint64_t limit_batches,
                              const std::optional<std::chrono::milliseconds> timeout) const {
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
  // PulsarConsumer. Though it changes the inner state, it saves the current assignment for future Check/Start calls to
  // restore the current state, so the changes made by this function shouldn't be visible for the users of the class. It
  // also passes a non const reference of PulsarConsumer to GetBatch function. That means the object is bitwise const
  // (PulsarConsumer is stored in unique_ptr) and internally mostly synchronized. Mostly, because as Start/Stop requires
  // exclusive access to consumer, so we don't have to deal with simultaneous calls to those functions. The only concern
  // in this function is to prevent executing this function on multiple threads simultaneously.
  if (is_running_.exchange(true)) {
    throw ConsumerRunningException(info_.consumer_name);
  }

  utils::OnScopeExit restore_is_running([this] { is_running_.store(false); });

  const auto num_of_batches = limit_batches.value_or(kDefaultCheckBatchLimit);
  const auto timeout_to_use = timeout.value_or(kDefaultCheckTimeout);
  const auto start = std::chrono::steady_clock::now();

  if (info_.topics.size() != 1) {
    throw ConsumerCheckFailedException(info_.consumer_name, "Check cannot be used for consumers with multiple topics.");
  }

  std::vector<std::string> partitions;
  const auto &topic = info_.topics.front();
  client_.getPartitionsForTopic(topic, partitions);
  if (partitions.size() > 1) {
    throw ConsumerCheckFailedException(info_.consumer_name, "Check cannot be used for topics with multiple partitions");
  }
  pulsar_client::Reader reader;
  client_.createReader(topic, last_message_id_, {}, reader);
  for (int64_t i = 0; i < num_of_batches;) {
    const auto now = std::chrono::steady_clock::now();
    // NOLINTNEXTLINE (modernize-use-nullptr)
    if (now - start >= timeout_to_use) {
      throw ConsumerCheckFailedException(info_.consumer_name, "Timeout reached");
    }

    auto maybe_batch = GetBatch(reader, info_, is_running_, last_message_id_);

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
      spdlog::warn("Pulsar consumer {} check failed with error {}", info_.consumer_name, e.what());
      throw ConsumerCheckFailedException(info_.consumer_name, e.what());
    }
  }
  reader.close();
}

void Consumer::StartConsuming() {
  MG_ASSERT(!is_running_, "Cannot start already running consumer!");
  if (thread_.joinable()) {
    thread_.join();
  }

  is_running_.store(true);

  thread_ = std::thread([this] {
    static constexpr auto kMaxThreadNameSize = utils::GetMaxThreadNameSize();
    const auto full_thread_name = "Cons#" + info_.consumer_name;

    utils::ThreadSetName(full_thread_name.substr(0, kMaxThreadNameSize));

    while (is_running_) {
      auto maybe_batch = GetBatch(consumer_, info_, is_running_, last_message_id_);

      if (maybe_batch.HasError()) {
        throw ConsumerReadMessagesFailedException(info_.consumer_name, maybe_batch.GetError());
      }

      const auto &batch = maybe_batch.GetValue();

      if (batch.empty()) {
        continue;
      }

      spdlog::info("Pulsar consumer {} is processing a batch", info_.consumer_name);

      try {
        TryToConsumeBatch(consumer_, info_, consumer_function_, last_message_id_, batch,
                          [&](const Message &message) -> const pulsar_client::Message & { return message.message_; });
      } catch (const std::exception &e) {
        spdlog::warn("Error happened in consumer {} while processing a batch: {}!", info_.consumer_name, e.what());
        break;
      }

      spdlog::info("Pulsar consumer {} finished processing", info_.consumer_name);
    }
    is_running_.store(false);
  });
}

void Consumer::StartConsumingWithLimit(uint64_t limit_batches, std::optional<std::chrono::milliseconds> timeout) const {
  if (is_running_.exchange(true)) {
    throw ConsumerRunningException(info_.consumer_name);
  }
  utils::OnScopeExit restore_is_running([this] { is_running_.store(false); });

  const auto timeout_to_use = timeout.value_or(kDefaultCheckTimeout);
  const auto start = std::chrono::steady_clock::now();

  for (uint64_t batch_count = 0; batch_count < limit_batches;) {
    const auto now = std::chrono::steady_clock::now();
    if (now - start >= timeout_to_use) {
      throw ConsumerCheckFailedException(info_.consumer_name, "Timeout reached");
    }

    const auto maybe_batch = GetBatch(consumer_, info_, is_running_, last_message_id_);

    if (maybe_batch.HasError()) {
      throw ConsumerReadMessagesFailedException(info_.consumer_name, maybe_batch.GetError());
    }

    const auto &batch = maybe_batch.GetValue();

    if (batch.empty()) {
      continue;
    }
    ++batch_count;

    spdlog::info("Pulsar consumer {} is processing a batch", info_.consumer_name);

    TryToConsumeBatch(consumer_, info_, consumer_function_, last_message_id_, batch,
                      [](const Message &message) -> const pulsar_client::Message & { return message.message_; });

    spdlog::info("Pulsar consumer {} finished processing", info_.consumer_name);
  }
}

void Consumer::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) {
    thread_.join();
  }
}

}  // namespace memgraph::integrations::pulsar
