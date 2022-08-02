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

#include "io/transport.hpp"

namespace memgraph::io::simulator {

using memgraph::io::Duration;
using memgraph::io::Message;
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
  requires(sizeof...(Ms) > 0) std::optional<RequestEnvelope<Ms...>> Take() && {
    std::optional<std::variant<Ms...>> m_opt = VariantFromAny<Ms...>(std::move(message));

    if (m_opt) {
      return RequestEnvelope<Ms...>{
          .message = std::move(*m_opt),
          .request_id = request_id,
          .from_address = from_address,
      };
    }

    return std::nullopt;
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
  OpaquePromise(OpaquePromise &&old) noexcept
      : ti_(old.ti_),
        ptr_(old.ptr_),
        dtor_(std::move(old.dtor_)),
        is_awaited_(std::move(old.is_awaited_)),
        fill_(std::move(old.fill_)),
        time_out_(std::move(old.time_out_)) {
    old.ptr_ = nullptr;
  }

  OpaquePromise &operator=(OpaquePromise &&old) noexcept {
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
  std::unique_ptr<ResponsePromise<T>> Take() && {
    MG_ASSERT(typeid(T) == *ti_);
    MG_ASSERT(ptr_ != nullptr);

    auto ptr = static_cast<ResponsePromise<T> *>(ptr_);

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
          auto promise = static_cast<ResponsePromise<T> *>(this_ptr);
          auto unique_promise = std::unique_ptr<ResponsePromise<T>>(promise);
          unique_promise->Fill(std::move(response_envelope));
        }),
        time_out_([](void *ptr) {
          auto promise = static_cast<ResponsePromise<T> *>(ptr);
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

}  // namespace memgraph::io::simulator
