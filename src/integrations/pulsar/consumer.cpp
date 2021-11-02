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

#include "utils/logging.hpp"
#include "utils/result.hpp"
#include "utils/thread.hpp"

namespace integrations::pulsar {

constexpr std::chrono::milliseconds kDefaultBatchInterval{100};
constexpr int64_t kDefaultBatchSize = 1000;
constexpr int64_t kDefaultCheckBatchLimit = 1;
constexpr std::chrono::milliseconds kDefaultCheckTimeout{30000};
constexpr std::chrono::milliseconds kMinimumInterval{1};
constexpr int64_t kMinimumSize{1};

namespace {
utils::BasicResult<std::string, std::vector<Message>> GetBatch(pulsar_client::Consumer consumer,
                                                               const ConsumerInfo &info,
                                                               std::atomic<bool> &is_running) {
  std::vector<Message> batch{};

  const auto batch_size = info.batch_size.value_or(kDefaultBatchSize);
  batch.reserve(batch_size);

  auto remaining_timeout_in_ms = info.batch_interval.value_or(kDefaultBatchInterval).count();
  auto start = std::chrono::steady_clock::now();

  bool run_batch = true;
  pulsar_client::Message message;
  for (int64_t i = 0; remaining_timeout_in_ms > 0 && i < batch_size && is_running.load(); ++i) {
    const auto result = consumer.receive(message, static_cast<int>(remaining_timeout_in_ms));
    switch (result) {
      case pulsar_client::Result::ResultTimeout:
        run_batch = false;
      case pulsar_client::Result::ResultOk:
        batch.emplace_back(std::move(message));
        break;
      default:
        spdlog::warn(fmt::format("Unexpected error while consuming message in subscription {}, error: {}",
                                 info.subscription_name, result));
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
}  // namespace

Message::Message(pulsar_client::Message &&message) : message_{std::move(message)} {}

Consumer::Consumer(const std::string &cluster, ConsumerInfo info, ConsumerFunction consumer_function)
    : info_{std::move(info)}, client_{cluster}, consumer_function_{std::move(consumer_function)} {
  pulsar_client::ConsumerConfiguration config;
  config.setSubscriptionInitialPosition(pulsar_client::InitialPositionEarliest);
  if (pulsar_client::Result result = client_.subscribe(info_.topic, info_.subscription_name, config, consumer_);
      result != pulsar_client::ResultOk) {
    throw std::runtime_error(fmt::format("Failed to create a consumer: {}", result));
  }
}

bool Consumer::IsRunning() const { return is_running_; }

void Consumer::Start() {
  if (is_running_) {
    throw std::runtime_error("Already running");
  }

  StartConsuming();
}

void Consumer::Stop() {
  if (!is_running_) {
    throw std::runtime_error("Already stopped");
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

void Consumer::StartConsuming() {
  MG_ASSERT(!is_running_, "Cannot start already running consumer!");
  if (thread_.joinable()) {
    thread_.join();
  }

  is_running_.store(true);

  thread_ = std::thread([this] {
    constexpr auto kMaxThreadNameSize = utils::GetMaxThreadNameSize();
    const auto full_thread_name = "Cons#" + info_.subscription_name;

    utils::ThreadSetName(full_thread_name.substr(0, kMaxThreadNameSize));

    while (is_running_) {
      auto maybe_batch = GetBatch(consumer_, info_, is_running_);

      if (maybe_batch.HasError()) {
        spdlog::warn("Error happened in subscription {} while fetching messages: {}!", info_.subscription_name,
                     maybe_batch.GetError());
        break;
      }

      const auto &batch = maybe_batch.GetValue();

      if (batch.empty()) continue;

      spdlog::info("Pulsar consumer {} is processing a batch", info_.subscription_name);

      try {
        consumer_function_(batch);

        for (const auto &message : batch) {
          if (const auto result = consumer_.acknowledge(message.message_); result != pulsar_client::ResultOk) {
            spdlog::warn("Acknowledging a message of consumer {} failed: {}", info_.subscription_name, result);
          }
        }
      } catch (const std::exception &e) {
        spdlog::warn("Error happened in consumer {} while processing a batch: {}!", info_.subscription_name, e.what());
        break;
      }

      spdlog::info("Pulsar consumer {} finished processing", info_.subscription_name);
    }
    is_running_.store(false);
  });
}

void Consumer::StopConsuming() {
  is_running_.store(false);
  if (thread_.joinable()) {
    thread_.join();
  }
}

}  // namespace integrations::pulsar
