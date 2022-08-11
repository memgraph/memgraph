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

namespace memgraph::io {

struct PromiseKey {
  Address requester_address;
  uint64_t request_id;
  // TODO(tyler) possibly remove replier_address from promise key
  // once we want to support DSR.
  Address replier_address;

 public:
  friend bool operator<(const PromiseKey &lhs, const PromiseKey &rhs) {
    if (lhs.requester_address != rhs.requester_address) {
      return lhs.requester_address < rhs.requester_address;
    }

    if (lhs.request_id != rhs.request_id) {
      return lhs.request_id < rhs.request_id;
    }

    return lhs.replier_address < rhs.replier_address;
  }
};

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

class OpaquePromiseTraitBase {
 public:
  virtual const std::type_info *TypeInfo() const = 0;
  virtual bool IsAwaited(void *ptr) const = 0;
  virtual void Fill(void *ptr, OpaqueMessage &&) const = 0;
  virtual void TimeOut(void *ptr) const = 0;

  virtual ~OpaquePromiseTraitBase() = default;
  OpaquePromiseTraitBase() = default;
  OpaquePromiseTraitBase(const OpaquePromiseTraitBase &) = delete;
  OpaquePromiseTraitBase &operator=(const OpaquePromiseTraitBase &) = delete;
  OpaquePromiseTraitBase(OpaquePromiseTraitBase &&old) = delete;
  OpaquePromiseTraitBase &operator=(OpaquePromiseTraitBase &&) = delete;
};

template <typename T>
class OpaquePromiseTrait : public OpaquePromiseTraitBase {
 public:
  const std::type_info *TypeInfo() const override { return &typeid(T); };

  bool IsAwaited(void *ptr) const override { return static_cast<ResponsePromise<T> *>(ptr)->IsAwaited(); };

  void Fill(void *ptr, OpaqueMessage &&opaque_message) const override {
    T message = std::any_cast<T>(std::move(opaque_message.message));
    auto response_envelope = ResponseEnvelope<T>{.message = std::move(message),
                                                 .request_id = opaque_message.request_id,
                                                 .from_address = opaque_message.from_address};
    auto promise = static_cast<ResponsePromise<T> *>(ptr);
    auto unique_promise = std::unique_ptr<ResponsePromise<T>>(promise);
    unique_promise->Fill(std::move(response_envelope));
  };

  void TimeOut(void *ptr) const override {
    auto promise = static_cast<ResponsePromise<T> *>(ptr);
    auto unique_promise = std::unique_ptr<ResponsePromise<T>>(promise);
    ResponseResult<T> result = TimedOut{};
    unique_promise->Fill(std::move(result));
  }
};

class OpaquePromise {
  void *ptr_;
  std::unique_ptr<OpaquePromiseTraitBase> trait_;

 public:
  OpaquePromise(OpaquePromise &&old) noexcept : ptr_(old.ptr_), trait_(std::move(old.trait_)) { old.ptr_ = nullptr; }

  OpaquePromise &operator=(OpaquePromise &&old) noexcept {
    MG_ASSERT(this != &old);
    ptr_ = old.ptr_;
    trait_ = std::move(old.trait_);
    old.ptr_ = nullptr;
    return *this;
  }

  OpaquePromise(const OpaquePromise &) = delete;
  OpaquePromise &operator=(const OpaquePromise &) = delete;

  template <typename T>
  std::unique_ptr<ResponsePromise<T>> Take() && {
    MG_ASSERT(typeid(T) == *trait_->TypeInfo());
    MG_ASSERT(ptr_ != nullptr);

    auto ptr = static_cast<ResponsePromise<T> *>(ptr_);

    ptr_ = nullptr;

    return std::unique_ptr<T>(ptr);
  }

  template <typename T>
  explicit OpaquePromise(std::unique_ptr<ResponsePromise<T>> promise)
      : ptr_(static_cast<void *>(promise.release())), trait_(std::make_unique<OpaquePromiseTrait<T>>()) {}

  bool IsAwaited() {
    MG_ASSERT(ptr_ != nullptr);
    return trait_->IsAwaited(ptr_);
  }

  void TimeOut() {
    MG_ASSERT(ptr_ != nullptr);
    trait_->TimeOut(ptr_);
    ptr_ = nullptr;
  }

  void Fill(OpaqueMessage &&opaque_message) {
    MG_ASSERT(ptr_ != nullptr);
    trait_->Fill(ptr_, std::move(opaque_message));
    ptr_ = nullptr;
  }

  ~OpaquePromise() {
    MG_ASSERT(ptr_ == nullptr, "OpaquePromise destroyed without being explicitly timed out or filled");
  }
};

struct DeadlineAndOpaquePromise {
  Time deadline;
  OpaquePromise promise;
};

}  // namespace memgraph::io
