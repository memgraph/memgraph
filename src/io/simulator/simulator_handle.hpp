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

#include <compare>

#include <any>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <utility>
#include <variant>
#include <vector>

#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/simulator/simulator_stats.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::io::simulator {

using memgraph::io::Duration;
using memgraph::io::Time;

struct OpaqueMessage {
  Address from_address;
  uint64_t request_id;
  std::any message;

  /// Recursively tries to match a specific type from the outer
  /// variant's parameter pack against the type of the std::any,
  /// and if it matches, make it concrete and return it. Otherwise,
  /// move on and compare the any with the next type from the
  /// parameter pack.
  ///
  /// Return is the full std::variant<Ts...> type that holds the
  /// full parameter pack without interfering with recursive
  /// narrowing expansion.
  template <typename Return, Message Head, Message... Rest>
  std::optional<Return> Unpack(std::any &&a) {
    if (typeid(Head) == a.type()) {
      Head concrete = std::any_cast<Head>(std::move(a));
      return concrete;
    }

    if constexpr (sizeof...(Rest) > 0) {
      return Unpack<Return, Rest...>(std::move(a));
    } else {
      return std::nullopt;
    }
  }

  /// High level "user-facing" conversion function that lets
  /// people interested in conversion only supply a single
  /// parameter pack for the types that they want to compare
  /// with the any and potentially include in the returned
  /// variant.
  template <Message... Ms>
  requires(sizeof...(Ms) > 0) std::optional<std::variant<Ms...>> VariantFromAny(std::any &&a) {
    return Unpack<std::variant<Ms...>, Ms...>(std::move(a));
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) std::optional<RequestEnvelope<Ms...>> Take() {
    std::optional<std::variant<Ms...>> m_opt = VariantFromAny<Ms...>(std::move(message));

    if (m_opt) {
      return RequestEnvelope<Ms...>{
          .message = std::move(*m_opt),
          .request_id = request_id,
          .from_address = from_address,
      };
    } else {
      return std::nullopt;
    }
  }
};

struct PromiseKey {
  Address requester_address;
  uint64_t request_id;
  Address replier_address;

 public:
  bool operator<(const PromiseKey &other) const {
    if (requester_address == other.requester_address) {
      return request_id < other.request_id;
    } else {
      return requester_address < other.requester_address;
    }
  }
};

class OpaquePromise {
  const std::type_info *ti_;
  void *ptr_;
  std::function<void(void *)> dtor_;
  std::function<bool(void *)> is_awaited_;
  std::function<void(void *, OpaqueMessage)> fill_;
  std::function<void(void *)> time_out_;

 public:
  OpaquePromise(OpaquePromise &&old)
      : ti_(old.ti_),
        ptr_(old.ptr_),
        dtor_(old.dtor_),
        is_awaited_(old.is_awaited_),
        fill_(old.fill_),
        time_out_(old.time_out_) {
    old.ptr_ = nullptr;
  }

  OpaquePromise &operator=(OpaquePromise &&old) {
    MG_ASSERT(this != &old);

    ptr_ = old.ptr_;
    ti_ = old.ti_;
    dtor_ = old.dtor_;
    is_awaited_ = old.is_awaited_;
    fill_ = old.fill_;
    time_out_ = old.time_out_;

    old.ptr_ = nullptr;

    return *this;
  }

  OpaquePromise(const OpaquePromise &) = delete;
  OpaquePromise &operator=(const OpaquePromise &) = delete;

  template <typename T>
  std::unique_ptr<ResponsePromise<T>> Take() {
    MG_ASSERT(typeid(T) == *ti_);
    MG_ASSERT(ptr_ != nullptr);

    ResponsePromise<T> *ptr = static_cast<ResponsePromise<T> *>(ptr_);

    ptr_ = nullptr;

    return std::unique_ptr<T>(ptr);
  }

  template <typename T>
  explicit OpaquePromise(std::unique_ptr<ResponsePromise<T>> promise)
      : ti_(&typeid(T)),
        ptr_(static_cast<void *>(promise.release())),
        dtor_([](void *ptr) { static_cast<ResponsePromise<T> *>(ptr)->~ResponsePromise<T>(); }),
        is_awaited_([](void *ptr) { return static_cast<ResponsePromise<T> *>(ptr)->IsAwaited(); }),
        fill_([](void *this_ptr, OpaqueMessage opaque_message) {
          T message = std::any_cast<T>(std::move(opaque_message.message));
          auto response_envelope = ResponseEnvelope<T>{.message = std::move(message),
                                                       .request_id = opaque_message.request_id,
                                                       .from_address = opaque_message.from_address};
          ResponsePromise<T> *promise = static_cast<ResponsePromise<T> *>(this_ptr);
          auto unique_promise = std::unique_ptr<ResponsePromise<T>>(promise);
          unique_promise->Fill(std::move(response_envelope));
        }),
        time_out_([](void *ptr) {
          ResponsePromise<T> *promise = static_cast<ResponsePromise<T> *>(ptr);
          auto unique_promise = std::unique_ptr<ResponsePromise<T>>(promise);
          ResponseResult<T> result = TimedOut{};
          unique_promise->Fill(std::move(result));
        }) {}

  bool IsAwaited() {
    MG_ASSERT(ptr_ != nullptr);
    return is_awaited_(ptr_);
  }

  void TimeOut() {
    MG_ASSERT(ptr_ != nullptr);
    time_out_(ptr_);
    ptr_ = nullptr;
  }

  void Fill(OpaqueMessage &&opaque_message) {
    MG_ASSERT(ptr_ != nullptr);
    fill_(ptr_, std::move(opaque_message));
    ptr_ = nullptr;
  }

  ~OpaquePromise() {
    if (nullptr != ptr_) {
      dtor_(ptr_);
    }
  }
};

struct DeadlineAndOpaquePromise {
  Time deadline;
  OpaquePromise promise;
};

class SimulatorHandle {
  std::mutex mu_{};
  std::condition_variable cv_;

  // messages that have not yet been scheduled or dropped
  std::vector<std::pair<Address, OpaqueMessage>> in_flight_;

  // the responses to requests that are being waited on
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;

  // messages that are sent to servers that may later receive them
  std::map<Address, std::vector<OpaqueMessage>> can_receive_;

  Time cluster_wide_time_microseconds_;
  bool should_shut_down_ = false;
  SimulatorStats stats_;
  size_t blocked_on_receive_ = 0;
  std::set<Address> server_addresses_;
  std::mt19937 rng_;
  SimulatorConfig config_;

 public:
  explicit SimulatorHandle(SimulatorConfig config)
      : cluster_wide_time_microseconds_(config.start_time), rng_(config.rng_seed), config_(config) {}

  void IncrementServerCountAndWaitForQuiescentState(Address address) {
    std::unique_lock<std::mutex> lock(mu_);
    server_addresses_.insert(address);

    while (true) {
      size_t blocked_servers = blocked_on_receive_;

      for (auto &[promise_key, opaque_promise] : promises_) {
        if (opaque_promise.promise.IsAwaited()) {
          if (server_addresses_.contains(promise_key.requester_address)) {
            blocked_servers++;
          }
        }
      }

      bool all_servers_blocked = blocked_servers == server_addresses_.size();

      if (all_servers_blocked) {
        return;
      }

      cv_.wait(lock);
    }
  }

  void TimeoutPromisesPastDeadline() {
    const Time now = cluster_wide_time_microseconds_;

    for (auto &[promise_key, dop] : promises_) {
      // TODO(tyler) queue this up and drop it after its deadline
      if (dop.deadline < now) {
        std::cout << "timing out request" << std::endl;
        DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
        promises_.erase(promise_key);

        stats_.timed_out_requests++;

        dop.promise.TimeOut();
      }
    }
  }

  bool MaybeTickSimulator() {
    std::unique_lock<std::mutex> lock(mu_);

    size_t blocked_servers = blocked_on_receive_;

    for (auto &[promise_key, opaque_promise] : promises_) {
      if (opaque_promise.promise.IsAwaited()) {
        if (server_addresses_.contains(promise_key.requester_address)) {
          blocked_servers++;
        }
      }
    }

    if (blocked_servers < server_addresses_.size()) {
      // we only need to advance the simulator when all
      // servers have reached a quiescent state, blocked
      // on their own futures or receive methods.
      return false;
    }

    stats_.simulator_ticks++;

    cv_.notify_all();

    TimeoutPromisesPastDeadline();

    if (in_flight_.empty()) {
      // return early here because there are no messages to schedule

      // We tick the clock forward when all servers are blocked but
      // there are no in-flight messages to schedule delivery of.
      std::poisson_distribution<> time_distrib(50);
      Duration clock_advance = std::chrono::microseconds{time_distrib(rng_)};
      cluster_wide_time_microseconds_ += clock_advance;

      MG_ASSERT(cluster_wide_time_microseconds_ < config_.abort_time,
                "Cluster has executed beyond its configured abort_time, and something may be failing to make progress "
                "in an expected amount of time.");
      return true;
    }

    if (config_.scramble_messages) {
      // scramble messages
      std::uniform_int_distribution<size_t> swap_distrib(0, in_flight_.size() - 1);
      size_t swap_index = swap_distrib(rng_);
      std::swap(in_flight_[swap_index], in_flight_.back());
    }

    auto [to_address, opaque_message] = std::move(in_flight_.back());
    in_flight_.pop_back();

    std::uniform_int_distribution<int> drop_distrib(0, 99);
    int drop_threshold = drop_distrib(rng_);
    bool should_drop = drop_threshold < config_.drop_percent;

    if (should_drop) {
      stats_.dropped_messages++;
    }

    PromiseKey promise_key{.requester_address = to_address,
                           .request_id = opaque_message.request_id,
                           .replier_address = opaque_message.from_address};

    if (promises_.contains(promise_key)) {
      // complete waiting promise if it's there
      DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
      promises_.erase(promise_key);

      const bool normal_timeout = config_.perform_timeouts && (dop.deadline < cluster_wide_time_microseconds_);

      if (should_drop || normal_timeout) {
        stats_.timed_out_requests++;
        dop.promise.TimeOut();
      } else {
        stats_.total_responses++;
        dop.promise.Fill(std::move(opaque_message));
      }
    } else if (should_drop) {
      // don't add it anywhere, let it drop
    } else {
      // add to can_receive_ if not
      const auto &[om_vec, inserted] = can_receive_.try_emplace(to_address, std::vector<OpaqueMessage>());
      om_vec->second.emplace_back(std::move(opaque_message));
    }

    return true;
  }

  void ShutDown() {
    std::unique_lock<std::mutex> lock(mu_);
    should_shut_down_ = true;
    cv_.notify_all();
  }

  bool ShouldShutDown() {
    std::unique_lock<std::mutex> lock(mu_);
    return should_shut_down_;
  }

  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, uint64_t request_id, Request &&request, Duration timeout,
                     ResponsePromise<Response> &&promise) {
    std::unique_lock<std::mutex> lock(mu_);

    const Time deadline = cluster_wide_time_microseconds_ + timeout;

    std::any message(std::move(request));
    OpaqueMessage om{.from_address = from_address, .request_id = request_id, .message = std::move(message)};
    in_flight_.emplace_back(std::make_pair(std::move(to_address), std::move(om)));

    PromiseKey promise_key{.requester_address = from_address, .request_id = request_id, .replier_address = to_address};
    OpaquePromise opaque_promise(std::move(promise).ToUnique());
    DeadlineAndOpaquePromise dop{.deadline = deadline, .promise = std::move(opaque_promise)};
    promises_.emplace(std::move(promise_key), std::move(dop));

    stats_.total_messages++;
    stats_.total_requests++;

    cv_.notify_all();

    return;
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(const Address &receiver, Duration timeout) {
    std::unique_lock<std::mutex> lock(mu_);

    blocked_on_receive_ += 1;

    const Time deadline = cluster_wide_time_microseconds_ + timeout;

    while (!should_shut_down_ && (cluster_wide_time_microseconds_ < deadline)) {
      if (can_receive_.contains(receiver)) {
        std::vector<OpaqueMessage> &can_rx = can_receive_.at(receiver);
        if (!can_rx.empty()) {
          OpaqueMessage message = std::move(can_rx.back());
          can_rx.pop_back();

          // TODO(tyler) search for item in can_receive_ that matches the desired types, rather
          // than asserting that the last item in can_rx matches.
          auto m_opt = message.Take<Ms...>();

          blocked_on_receive_ -= 1;

          return std::move(m_opt).value();
        }
      }

      lock.unlock();
      bool made_progress = MaybeTickSimulator();
      lock.lock();
      if (!should_shut_down_ && !made_progress) {
        cv_.wait(lock);
      }
    }

    blocked_on_receive_ -= 1;

    return TimedOut{};
  }

  template <Message M>
  void Send(Address to_address, Address from_address, uint64_t request_id, M message) {
    std::unique_lock<std::mutex> lock(mu_);
    std::any message_any(std::move(message));
    OpaqueMessage om{.from_address = from_address, .request_id = request_id, .message = std::move(message_any)};
    in_flight_.emplace_back(std::make_pair(std::move(to_address), std::move(om)));

    stats_.total_messages++;

    cv_.notify_all();
  }

  Time Now() {
    std::unique_lock<std::mutex> lock(mu_);
    return cluster_wide_time_microseconds_;
  }

  template <class D = std::poisson_distribution<>, class Return = uint64_t>
  Return Rand(D distrib) {
    std::unique_lock<std::mutex> lock(mu_);
    return distrib(rng_);
  }

  SimulatorStats Stats() {
    std::unique_lock<std::mutex> lock(mu_);
    return stats_;
  }
};
};  // namespace memgraph::io::simulator
