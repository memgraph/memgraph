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

#include <any>
#include <compare>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <variant>
#include <vector>

#include "address.hpp"
#include "errors.hpp"
#include "simulator_config.hpp"
#include "simulator_stats.hpp"
#include "transport.hpp"

// TODO enforce this around std::any usage
template <typename T>
concept SameAsDecayed = std::same_as<T, std::decay_t<T>>;

struct OpaqueMessage {
  Address from_address;
  uint64_t request_id;
  std::any message;

  template <typename Return, Message Head, Message... Rest>
  std::optional<Return> Unpack(std::any &&a) {
    if (typeid(Head) == a.type()) {
      Head concrete = std::any_cast<Head>(std::move(a));
      return concrete;
    }

    if constexpr (sizeof...(Rest) > 0) {
      return Unpack<Return, Rest...>(a);
    } else {
      return std::nullopt;
    }
  }

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

// TODO delete copy ctor & copy assignment operator if possible
class OpaquePromise {
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
  OpaquePromise(std::unique_ptr<ResponsePromise<T>> promise)
      : ti_(&typeid(T)),
        ptr_((void *)promise.release()),
        dtor_([](void *ptr) { static_cast<ResponsePromise<T> *>(ptr)->~ResponsePromise<T>(); }),
        is_awaited_([](void *ptr) { return static_cast<ResponsePromise<T> *>(ptr)->IsAwaited(); }),
        fill_([](void *this_ptr, OpaqueMessage opaque_message) {
          std::cout << "expecting typeid " << typeid(T).name() << std::endl;
          std::cout << "got typeid " << opaque_message.message.type().name() << std::endl;
          T message = std::any_cast<T>(std::move(opaque_message.message));
          auto response_envelope = ResponseEnvelope<T>{.message = std::move(message),
                                                       .request_id = opaque_message.request_id,
                                                       .from_address = opaque_message.from_address};
          ResponsePromise<T> *promise = static_cast<ResponsePromise<T> *>(this_ptr);
          promise->Fill(std::move(response_envelope));
        }),
        time_out_([](void *ptr) {
          ResponseResult<T> result = TimedOut{};
          static_cast<ResponsePromise<T> *>(ptr)->Fill(result);
        }) {}

  bool IsAwaited() {
    MG_ASSERT(ptr_ != nullptr);
    return is_awaited_(ptr_);
  }

  void TimeOut() {
    MG_ASSERT(ptr_ != nullptr);
    time_out_(ptr_);
  }

  void Fill(OpaqueMessage &&opaque_message) {
    MG_ASSERT(ptr_ != nullptr);
    fill_(ptr_, std::move(opaque_message));
  }

  ~OpaquePromise() {
    if (nullptr != ptr_) {
      dtor_(ptr_);
    }
  }

 private:
  const std::type_info *ti_;
  void *ptr_;
  std::function<void(void *)> dtor_;
  std::function<bool(void *)> is_awaited_;
  std::function<void(void *, OpaqueMessage)> fill_;
  std::function<void(void *)> time_out_;
};

struct DeadlineAndOpaquePromise {
  uint64_t deadline;
  OpaquePromise promise;
};

class SimulatorHandle {
 public:
  void IncrementServerCount(Address address) {
    std::unique_lock<std::mutex> lock(mu_);
    servers_++;
    server_addresses_.insert(address);
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

    std::cout << "wait count: " << blocked_servers << std::endl;
    std::cout << "srv count: " << servers_ << std::endl;

    if (blocked_servers < servers_) {
      // we only need to advance the simulator when all
      // servers have reached a quiescent state, blocked
      // on their own futures or receive methods.
      return false;
    }

    if (in_flight_.empty()) {
      return false;
    }

    auto [to_address, opaque_message] = std::move(in_flight_.back());
    in_flight_.pop_back();

    // TODO drop this message for fault injection and close associated promises sometimes.

    PromiseKey promise_key{.requester_address = to_address,
                           .request_id = opaque_message.request_id,
                           .replier_address = opaque_message.from_address};

    if (promises_.contains(promise_key)) {
      // complete waiting promise if it's there
      DeadlineAndOpaquePromise dop = std::move(promises_.at(promise_key));
      promises_.erase(promise_key);

      if (config_.perform_timeouts && dop.deadline > cluster_wide_time_microseconds_) {
        dop.promise.TimeOut();
      } else {
        dop.promise.Fill(std::move(opaque_message));
      }
    } else {
      // add to can_receive_ if not
      const auto &[om_vec, inserted] = can_receive_.try_emplace(to_address, std::vector<OpaqueMessage>());
      om_vec->second.emplace_back(std::move(opaque_message));
    }

    cv_.notify_all();

    return true;
  }

  void SetConfig(SimulatorConfig config) {
    std::unique_lock<std::mutex> lock(mu_);
    config_ = config;
  }

  bool ShouldShutDown() {
    std::unique_lock<std::mutex> lock(mu_);
    return should_shut_down_;
  }

  template <Message Request, Message Response>
  void SubmitRequest(Address to_address, Address from_address, uint64_t request_id, Request &&request,
                     uint64_t timeout_microseconds, ResponsePromise<Response> &&promise) {
    std::unique_lock<std::mutex> lock(mu_);

    uint64_t deadline = cluster_wide_time_microseconds_ + timeout_microseconds;

    std::any message(std::move(request));
    OpaqueMessage om{.from_address = from_address, .request_id = request_id, .message = std::move(message)};
    in_flight_.emplace_back(std::make_pair(std::move(to_address), std::move(om)));

    PromiseKey promise_key{.requester_address = from_address, .request_id = request_id, .replier_address = to_address};
    OpaquePromise opaque_promise(std::move(promise).ToUnique());
    DeadlineAndOpaquePromise dop{.deadline = deadline, .promise = std::move(opaque_promise)};
    promises_.emplace(std::move(promise_key), std::move(dop));

    stats_.total_messages_++;
    stats_.total_requests_++;

    return;
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Address &receiver, uint64_t timeout_microseconds) {
    std::unique_lock<std::mutex> lock(mu_);

    uint64_t deadline = cluster_wide_time_microseconds_ + timeout_microseconds;

    while (!(should_shut_down_ && cluster_wide_time_microseconds_ < deadline)) {
      if (can_receive_.contains(receiver)) {
        std::vector<OpaqueMessage> &can_rx = can_receive_.at(receiver);
        if (!can_rx.empty()) {
          OpaqueMessage message = std::move(can_rx.back());
          can_rx.pop_back();

          // TODO search for item in can_receive_ that returns non-nullopt
          auto m_opt = message.Take<Ms...>();
          return std::move(m_opt).value();
        }
      }

      blocked_on_receive_ += 1;
      lock.unlock();
      bool made_progress = MaybeTickSimulator();
      lock.lock();
      if (!made_progress) {
        cv_.wait(lock);
      }
      blocked_on_receive_ -= 1;
    }

    return TimedOut{};
  }

  template <Message M>
  void Send(Address to_address, Address from_address, uint64_t request_id, M message) {
    std::unique_lock<std::mutex> lock(mu_);
    std::any message_any(std::move(message));
    OpaqueMessage om{.from_address = from_address, .request_id = request_id, .message = std::move(message_any)};
    in_flight_.emplace_back(std::make_pair(std::move(to_address), std::move(om)));
  }

 private:
  std::mutex mu_{};
  std::condition_variable cv_;
  std::vector<std::pair<Address, OpaqueMessage>> in_flight_;
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;
  std::map<Address, std::vector<OpaqueMessage>> can_receive_;
  uint64_t cluster_wide_time_microseconds_ = 0;
  bool should_shut_down_ = false;
  SimulatorStats stats_;
  size_t blocked_on_receive_ = 0;
  size_t servers_ = 0;
  std::set<Address> server_addresses_;
  SimulatorConfig config_;
};
