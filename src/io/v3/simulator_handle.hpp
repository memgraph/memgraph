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
#include <map>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

#include "address.hpp"
#include "errors.hpp"
#include "simulator_stats.hpp"
#include "transport.hpp"

template <typename T>
concept SameAsDecayed = std::same_as<T, std::decay_t<T>>;

struct OpaqueMessage {
  Address address;
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
          .from = address,
      };
    } else {
      return std::nullopt;
    }
  }
};

struct PromiseKey {
  Address requester;
  uint64_t request_id;
  Address replier;

 public:
  bool operator<(const PromiseKey &other) const {
    if (requester == other.requester) {
      return request_id < other.request_id;
    } else {
      return requester < other.requester;
    }
  }
};

// TODO delete copy ctor & copy assignment operator if possible
class OpaquePromise {
 public:
  OpaquePromise(OpaquePromise &&old) : ti_(old.ti_) {
    ptr_ = old.ptr_;
    old.ptr_ = nullptr;
  }

  OpaquePromise &operator=(OpaquePromise &&old) {
    MG_ASSERT(this != &old);

    ptr_ = old.ptr_;
    ti_ = old.ti_;
    old.ptr_ = nullptr;

    return *this;
  }

  OpaquePromise(const OpaquePromise &) = delete;
  OpaquePromise &operator=(const OpaquePromise &) = delete;

  template <typename T>
  std::unique_ptr<MgPromise<T>> Take() {
    MG_ASSERT(typeid(T) == *ti_);
    MG_ASSERT(ptr_ != nullptr);

    MgPromise<T> *ptr = static_cast<MgPromise<T> *>(ptr_);

    ptr_ = nullptr;

    return std::unique_ptr<T>(ptr);
  }

  template <typename T>
  OpaquePromise(std::unique_ptr<MgPromise<T>> promise)
      : ti_(&typeid(T)),
        ptr_((void *)promise.release()),
        dtor_([](void *ptr) { static_cast<MgPromise<T> *>(ptr)->~MgPromise<T>(); }) {}

  ~OpaquePromise() {
    if (nullptr != ptr_) {
      dtor_(ptr_);
    }
  }

 private:
  const std::type_info *ti_;
  void *ptr_;
  std::function<void(void *)> dtor_;
};

struct DeadlineAndOpaquePromise {
  uint64_t deadline;
  OpaquePromise promise;
};

class SimulatorHandle {
 public:
  void NotifySimulator() {
    std::unique_lock<std::mutex> lock(mu_);
    cv_sim_.notify_all();
  }

  bool ShouldShutDown() {
    std::unique_lock<std::mutex> lock(mu_);
    return should_shut_down_;
  }

  template <Message Request, Message Response>
  void SubmitRequest(Address to_addr, Address from_addr, uint64_t request_id, Request &&request,
                     uint64_t timeout_microseconds, MgPromise<ResponseResult<Response>> &&promise) {
    std::unique_lock<std::mutex> lock(mu_);

    uint64_t deadline = cluster_wide_time_microseconds_ + timeout_microseconds;

    std::any message(std::move(request));
    OpaqueMessage om{.address = from_addr, .request_id = request_id, .message = std::move(message)};
    in_flight_.emplace_back(std::make_pair(std::move(to_addr), std::move(om)));

    OpaquePromise opaque_promise(std::move(promise).ToUnique());
    PromiseKey pk{.requester = from_addr, .request_id = request_id, .replier = to_addr};
    DeadlineAndOpaquePromise op{.deadline = deadline, .promise = std::move(opaque_promise)};
    promises_.emplace(std::move(pk), std::move(op));

    stats_.total_messages_++;
    stats_.total_requests_++;

    return;
  }

  template <Message... Ms>
  requires(sizeof...(Ms) > 0) RequestResult<Ms...> Receive(Address &receiver, uint64_t timeout_microseconds) {
    std::unique_lock<std::mutex> lock(mu_);

    uint64_t deadline = cluster_wide_time_microseconds_ + timeout_microseconds;

    while (!(should_shut_down_ && cluster_wide_time_microseconds_ < deadline)) {
      try {
        std::vector<OpaqueMessage> &can_rx = can_receive_.at(receiver);
        if (!can_rx.empty()) {
          OpaqueMessage message = std::move(can_rx.back());
          can_rx.pop_back();

          // TODO search for item in can_receive_ that returns non-nullopt
          auto m_opt = message.Take<Ms...>();
          return std::move(m_opt).value();
        }
      } catch (const std::out_of_range &oor) {
        // no-op
      }

      blocked_on_receive_ += 1;
      cv_sim_.notify_all();
      cv_srv_.wait(lock);
      blocked_on_receive_ -= 1;
    }

    return Timeout{};
  }

  template <Message M>
  void Send(Address to, Address from, uint64_t request_id, M message) {
    std::abort();
  }

 private:
  std::mutex mu_{};
  std::condition_variable cv_sim_;
  std::condition_variable cv_srv_;
  std::vector<std::pair<Address, OpaqueMessage>> in_flight_;
  std::map<PromiseKey, DeadlineAndOpaquePromise> promises_;
  std::map<Address, std::vector<OpaqueMessage>> can_receive_;
  uint64_t cluster_wide_time_microseconds_ = 0;
  bool should_shut_down_;
  SimulatorStats stats_;
  size_t blocked_on_receive_;
};
