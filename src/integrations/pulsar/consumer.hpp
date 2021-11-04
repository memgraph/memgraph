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

#pragma once
#include <atomic>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <thread>
#include <vector>

namespace integrations::pulsar {

class Consumer;

class Message final {
 public:
  std::span<const char> Payload() const;

 private:
  struct MessageImpl;

  template <typename... Args>
  explicit Message(Args &&...args) : impl_(std::make_shared<MessageImpl>(std::forward<Args>(args)...)) {}

  std::shared_ptr<MessageImpl> impl_;

  friend Consumer;
};

using ConsumerFunction = std::function<void(const std::vector<Message> &)>;

struct ConsumerInfo {
  std::optional<int64_t> batch_size;
  std::optional<std::chrono::milliseconds> batch_interval;
  std::string topic;
  std::string consumer_name;
  std::string service_url;
};

class Consumer final {
 public:
  Consumer(ConsumerInfo info, ConsumerFunction consumer_function);
  ~Consumer();

  Consumer(const Consumer &) = delete;
  Consumer(Consumer &&) noexcept = delete;
  Consumer &operator=(const Consumer &) = delete;
  Consumer &operator=(Consumer &&) = delete;

  bool IsRunning() const;
  void Start();
  void Stop();
  void StopIfRunning();

  void Check(std::optional<std::chrono::milliseconds> timeout, std::optional<int64_t> limit_batches,
             const ConsumerFunction &check_consumer_function) const;

  const ConsumerInfo &Info() const;

 private:
  struct ConsumerImpl;

  std::unique_ptr<ConsumerImpl> impl_;
};
}  // namespace integrations::pulsar
