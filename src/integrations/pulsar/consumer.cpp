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

#include "integrations/pulsar/consumer.hpp"

#include <fmt/format.h>
#include <pulsar/Client.h>

#include <chrono>
#include <thread>

#include "integrations/constants.hpp"
#include "integrations/pulsar/exceptions.hpp"
#include "utils/concepts.hpp"
#include "utils/logging.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/result.hpp"
#include "utils/thread.hpp"

namespace integrations::pulsar {

namespace pulsar_client = ::pulsar;

namespace {

class SpdlogLogger : public pulsar_client::Logger {
  bool isEnabled(Level /*level*/) override { return spdlog::should_log(spdlog::level::trace); }

  void log(Level /*level*/, int /*line*/, const std::string &message) override {
    spdlog::trace("[Pulsar] {}", message);
  }
};

class SpdlogLoggerFactory : public pulsar_client::LoggerFactory {
  pulsar_client::Logger *getLogger(const std::string & /*file_name*/) override {
    if (!logger_) {
      logger_ = std::make_unique<SpdlogLogger>();
    }
    return logger_.get();
  }

 private:
  std::unique_ptr<SpdlogLogger> logger_;
};

pulsar_client::Client CreateClient(const std::string &service_url) {
  static SpdlogLoggerFactory logger_factory;
  pulsar_client::ClientConfiguration conf;
  conf.setLogger(&logger_factory);
  return {service_url, conf};
}
}  // namespace

struct Message::MessageImpl {
  explicit MessageImpl(pulsar_client::Message &&message) : message_{std::move(message)} {}
  pulsar_client::Message message_;
};

std::span<const char> Message::Payload() const {
  return {static_cast<const char *>(impl_->message_.getData()), impl_->message_.getLength()};
}

struct Consumer::ConsumerImpl {
  friend Consumer;

  ConsumerImpl(ConsumerInfo info, ConsumerFunction consumer_function)
      : info_{std::move(info)}, consumer_function_{std::move(consumer_function)} {
    client_.emplace(CreateClient(info_.service_url));
    pulsar_client::ConsumerConfiguration config;
    config.setSubscriptionInitialPosition(pulsar_client::InitialPositionEarliest);
    config.setConsumerType(pulsar_client::ConsumerType::ConsumerExclusive);
    if (pulsar_client::Result result = client_->subscribe(info_.topic, info_.consumer_name, config, consumer_);
        result != pulsar_client::ResultOk) {
      throw ConsumerFailedToInitializeException(info_.consumer_name, pulsar_client::strResult(result));
    }
  }

  void StartConsuming();
  void StopConsuming();

  static utils::BasicResult<std::string, std::vector<Message>> GetBatch(
      pulsar_client::Consumer &consumer, const ConsumerInfo &info, std::atomic<bool> &is_running,
      std::optional<uint64_t> &next_message_timestamp);

  ConsumerInfo info_;
  std::optional<pulsar_client::Client> client_;
  mutable pulsar_client::Consumer consumer_;
  ConsumerFunction consumer_function_;

  mutable std::atomic<bool> is_running_{false};
  mutable std::optional<uint64_t> next_message_timestamp_;
  std::thread thread_;
};

utils::BasicResult<std::string, std::vector<Message>> Consumer::ConsumerImpl::GetBatch(
    pulsar_client::Consumer &consumer, const ConsumerInfo &info, std::atomic<bool> &is_running,
    std::optional<uint64_t> &next_message_timestamp) {
  std::vector<Message> batch{};

  const auto batch_size = info.batch_size.value_or(kDefaultBatchSize);
  batch.reserve(batch_size);

  auto remaining_timeout_in_ms = info.batch_interval.value_or(kDefaultBatchInterval).count();
  auto start = std::chrono::steady_clock::now();

  bool run_batch = true;
  for (int64_t i = 0; remaining_timeout_in_ms > 0 && i < batch_size && is_running.load(); ++i) {
    pulsar_client::Message message;
    const auto result = consumer.receive(message, static_cast<int>(remaining_timeout_in_ms));
    switch (result) {
      case pulsar_client::Result::ResultTimeout:
        run_batch = false;
        break;
      case pulsar_client::Result::ResultOk:
        if (next_message_timestamp) {
          if (*next_message_timestamp == message.getPublishTimestamp()) {
            batch.emplace_back(Message{std::move(message)});
            next_message_timestamp.reset();
          }
        } else {
          batch.emplace_back(Message{std::move(message)});
        }
        break;
      default:
        spdlog::warn(fmt::format("Unexpected error while consuming message from consumer {}, error: {}",
                                 info.consumer_name, result));
        return {pulsar_client::strResult(result)};
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

Consumer::Consumer(ConsumerInfo info, ConsumerFunction consumer_function)
    : impl_(std::make_unique<ConsumerImpl>(std::move(info), std::move(consumer_function))) {}

Consumer::~Consumer() {
  StopIfRunning();
  impl_->client_->close();
}

bool Consumer::IsRunning() const { return impl_->is_running_; }

const ConsumerInfo &Consumer::Info() const { return impl_->info_; }

void Consumer::Start() {
  if (impl_->is_running_) {
    throw ConsumerRunningException(impl_->info_.consumer_name);
  }

  impl_->StartConsuming();
}

void Consumer::Stop() {
  if (!impl_->is_running_) {
    throw ConsumerStoppedException(impl_->info_.consumer_name);
  }
  impl_->StopConsuming();
}

void Consumer::StopIfRunning() {
  if (impl_->is_running_) {
    impl_->StopConsuming();
  }

  if (impl_->thread_.joinable()) {
    impl_->thread_.join();
  }
}

void Consumer::Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> limit_batches,
                     const ConsumerFunction &check_consumer_function) const {
  // NOLINTNEXTLINE (modernize-use-nullptr)
  if (timeout.value_or(kMinimumInterval) < kMinimumInterval) {
    throw ConsumerCheckFailedException(impl_->info_.consumer_name, "Timeout has to be positive!");
  }
  if (limit_batches.value_or(kMinimumSize) < kMinimumSize) {
    throw ConsumerCheckFailedException(impl_->info_.consumer_name, "Batch limit has to be positive!");
  }
  // The implementation of this function is questionable: it is const qualified, though it changes the inner state of
  // PulsarConsumer. Though it changes the inner state, it saves the current assignment for future Check/Start calls to
  // restore the current state, so the changes made by this function shouldn't be visible for the users of the class. It
  // also passes a non const reference of PulsarConsumer to GetBatch function. That means the object is bitwise const
  // (PulsarConsumer is stored in unique_ptr) and internally mostly synchronized. Mostly, because as Start/Stop requires
  // exclusive access to consumer, so we don't have to deal with simultaneous calls to those functions. The only concern
  // in this function is to prevent executing this function on multiple threads simultaneously.
  if (impl_->is_running_.exchange(true)) {
    throw ConsumerRunningException(impl_->info_.consumer_name);
  }

  utils::OnScopeExit restore_is_running([this] { impl_->is_running_.store(false); });

  const auto num_of_batches = limit_batches.value_or(kDefaultCheckBatchLimit);
  const auto timeout_to_use = timeout.value_or(kDefaultCheckTimeout);
  const auto start = std::chrono::steady_clock::now();

  for (int64_t i = 0; i < num_of_batches;) {
    const auto now = std::chrono::steady_clock::now();
    // NOLINTNEXTLINE (modernize-use-nullptr)
    if (now - start >= timeout_to_use) {
      throw ConsumerCheckFailedException(impl_->info_.consumer_name, "Timeout reached");
    }

    auto maybe_batch =
        ConsumerImpl::GetBatch(impl_->consumer_, impl_->info_, impl_->is_running_, impl_->next_message_timestamp_);

    if (maybe_batch.HasError()) {
      throw ConsumerCheckFailedException(impl_->info_.consumer_name, maybe_batch.GetError());
    }

    const auto &batch = maybe_batch.GetValue();

    if (batch.empty()) {
      continue;
    }
    ++i;

    try {
      check_consumer_function(batch);
      if (i == 0) {
        impl_->next_message_timestamp_ = batch.front().impl_->message_.getPublishTimestamp();
      }
    } catch (const std::exception &e) {
      spdlog::warn("Pulsar consumer {} check failed with error {}", impl_->info_.consumer_name, e.what());
      throw ConsumerCheckFailedException(impl_->info_.consumer_name, e.what());
    }
  }

  impl_->consumer_.redeliverUnacknowledgedMessages();
}

void Consumer::ConsumerImpl::StartConsuming() {
  MG_ASSERT(!is_running_, "Cannot start already running consumer!");
  if (thread_.joinable()) {
    thread_.join();
  }

  is_running_.store(true);

  thread_ = std::thread([this] {
    constexpr auto kMaxThreadNameSize = utils::GetMaxThreadNameSize();
    const auto full_thread_name = "Cons#" + info_.consumer_name;

    utils::ThreadSetName(full_thread_name.substr(0, kMaxThreadNameSize));

    while (is_running_) {
      auto maybe_batch = GetBatch(consumer_, info_, is_running_, next_message_timestamp_);

      if (maybe_batch.HasError()) {
        spdlog::warn("Error happened in consumer {} while fetching messages: {}!", info_.consumer_name,
                     maybe_batch.GetError());
        break;
      }

      const auto &batch = maybe_batch.GetValue();

      if (batch.empty()) continue;

      spdlog::info("Pulsar consumer {} is processing a batch", info_.consumer_name);

      try {
        consumer_function_(batch);

        if (const auto result = consumer_.acknowledgeCumulative(batch.back().impl_->message_);
            result != pulsar_client::ResultOk) {
          spdlog::warn("Acknowledging a message of consumer {} failed: {}", info_.consumer_name, result);
          break;
        }
      } catch (const std::exception &e) {
        spdlog::warn("Error happened in consumer {} while processing a batch: {}!", info_.consumer_name, e.what());
        break;
      }

      spdlog::info("Pulsar consumer {} finished processing", info_.consumer_name);
    }
    is_running_.store(false);
  });
}

void Consumer::ConsumerImpl::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) {
    thread_.join();
  }
}

}  // namespace integrations::pulsar
