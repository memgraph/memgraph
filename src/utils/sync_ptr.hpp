// Copyright 2023 Memgraph Ltd.
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

#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include "utils/exceptions.hpp"

namespace memgraph::utils {

/**
 * @brief
 *
 * @tparam TContext
 * @tparam TConfig
 */
template <typename TContext, typename TConfig = void>
struct SyncPtr {
  template <typename T>
  struct is_raw_ptr : std::false_type {};
  template <>
  struct is_raw_ptr<TContext *> : std::true_type {};

  /**
   * @brief Construct a new synched pointer.
   *
   * @tparam TArgs variable templates used by the TContext constructor
   * @param config Additional metadata associated with context
   * @param args Arguments to pass to TContext constructor
   */
  template <typename... TArgs>
  requires(sizeof...(TArgs) != 1 || !is_raw_ptr<TArgs...>::value) explicit SyncPtr(TConfig config, TArgs &&...args)
      : timeout_{1000}, config_{config}, ptr_{new TContext(std::forward<TArgs>(args)...), [this](TContext *ptr) {
                                                this->OnDelete(ptr);
                                              }} {}

  /**
   * @brief Construct a new synched pointer from a raw pointer.
   *
   * @param config Additional metadata associated with context
   * @param ptr
   */
  SyncPtr(TConfig config, TContext *ptr)
      : timeout_{1000}, config_{config}, ptr_{ptr, [this](TContext *ptr) { this->OnDelete(ptr); }} {}

  ~SyncPtr() = default;

  SyncPtr(const SyncPtr &) = delete;
  SyncPtr &operator=(const SyncPtr &) = delete;
  SyncPtr(SyncPtr &&) noexcept = delete;
  SyncPtr &operator=(SyncPtr &&) noexcept = delete;

  /**
   * @brief Destroy the synched pointer and wait for all copies to get destroyed.
   *
   */
  void DestroyAndSync() {
    ptr_.reset();
    SyncOnDelete();
  }

  /**
   * @brief Get (copy) the underlying shared pointer.
   *
   * @return std::shared_ptr<TContext>
   */
  std::shared_ptr<TContext> get() { return ptr_; }
  std::shared_ptr<const TContext> get() const { return ptr_; }

  TContext *operator->() const { return ptr_.get(); }

  /**
   * @brief Return saved configuration (metadata)
   *
   * @return TConfig
   */
  TConfig config() { return config_; }
  const TConfig &config() const { return config_; }

  void timeout(const std::chrono::milliseconds to) { timeout_ = to; }
  std::chrono::milliseconds timeout() const { return timeout_; }

 private:
  /**
   * @brief Block until OnDelete gets called.
   *
   */
  void SyncOnDelete() {
    std::unique_lock<std::mutex> lock(in_use_mtx_);
    if (!in_use_cv_.wait_for(lock, timeout_, [this] { return !in_use_; })) {
      throw utils::BasicException("Syncronization timeout!");
    }
  }

  /**
   * @brief Custom destructor used to sync the shared_ptr release.
   *
   * @param p Pointer to the undelying object.
   */
  void OnDelete(TContext *p) {
    delete p;
    {
      std::lock_guard<std::mutex> lock(in_use_mtx_);
      in_use_ = false;
    }
    in_use_cv_.notify_all();
  }

  bool in_use_{true};                          //!< Flag used to signal sync
  mutable std::mutex in_use_mtx_;              //!< Mutex used in the cv sync
  mutable std::condition_variable in_use_cv_;  //!< cv used to signal a sync
  std::chrono::milliseconds timeout_;          //!< Synchronization timeout in ms
  TConfig config_;                             //!< Additional metadata associated with the context
  std::shared_ptr<TContext> ptr_;              //!< Pointer being synced
};

template <typename TContext>
class SyncPtr<TContext, void> {
  template <typename T>
  struct is_raw_ptr : std::false_type {};
  template <>
  struct is_raw_ptr<TContext *> : std::true_type {};

 public:
  /**
   * @brief Construct a new synched pointer.
   *
   * @tparam TArgs variable templates used by the TContext constructor
   * @param args Arguments to pass to TContext constructor
   */
  template <typename... TArgs>
  requires(sizeof...(TArgs) != 1 || !is_raw_ptr<TArgs...>::value) explicit SyncPtr(TArgs &&...args)
      : timeout_{1000}, ptr_{new TContext(std::forward<TArgs>(args)...), [this](TContext *ptr) {
                               this->OnDelete(ptr);
                             }} {}

  /**
   * @brief Construct a new synched pointer from a raw pointer.
   *
   * @param ptr
   */
  explicit SyncPtr(TContext *ptr) : timeout_{1000}, ptr_{ptr, [this](TContext *ptr) { this->OnDelete(ptr); }} {}

  ~SyncPtr() = default;

  SyncPtr(const SyncPtr &) = delete;
  SyncPtr &operator=(const SyncPtr &) = delete;
  SyncPtr(SyncPtr &&) noexcept = delete;
  SyncPtr &operator=(SyncPtr &&) noexcept = delete;

  /**
   * @brief Destroy the synched pointer and wait for all copies to get destroyed.
   *
   */
  void DestroyAndSync() {
    ptr_.reset();
    SyncOnDelete();
  }

  /**
   * @brief Get (copy) the underlying shared pointer.
   *
   * @return std::shared_ptr<TContext>
   */
  std::shared_ptr<TContext> get() { return ptr_; }
  std::shared_ptr<const TContext> get() const { return ptr_; }

  void timeout(const std::chrono::milliseconds to) { timeout_ = to; }
  std::chrono::milliseconds timeout() const { return timeout_; }

 private:
  /**
   * @brief Block until OnDelete gets called.
   *
   */
  void SyncOnDelete() {
    std::unique_lock<std::mutex> lock(in_use_mtx_);
    if (!in_use_cv_.wait_for(lock, timeout_, [this] { return !in_use_; })) {
      throw utils::BasicException("Syncronization timeout!");
    }
  }

  /**
   * @brief Custom destructor used to sync the shared_ptr release.
   *
   * @param p Pointer to the undelying object.
   */
  void OnDelete(TContext *p) {
    delete p;
    {
      std::lock_guard<std::mutex> lock(in_use_mtx_);
      in_use_ = false;
    }
    in_use_cv_.notify_all();
  }

  bool in_use_{true};                          //!< Flag used to signal sync
  mutable std::mutex in_use_mtx_;              //!< Mutex used in the cv sync
  mutable std::condition_variable in_use_cv_;  //!< cv used to signal a sync
  std::chrono::milliseconds timeout_;          //!< Synchronization timeout in ms
  std::shared_ptr<TContext> ptr_;              //!< Pointer being synced
};

}  // namespace memgraph::utils
