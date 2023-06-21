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

#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>

namespace memgraph::utils {

/**
 * @brief
 *
 * @tparam TContext
 * @tparam TConfig
 */
template <typename TContext, typename TConfig = void>
struct SyncPtr {
  /**
   * @brief Construct a new synched pointer.
   *
   * @tparam TArgs variable templates used by the TContext constructor
   * @param config Additional metadata associated with context
   * @param args Arguments to pass to TContext constructor
   */
  template <typename... TArgs>
  explicit SyncPtr(TConfig config, TArgs &&...args)
      : in_use_(true),
        config_{config},
        ptr_{new TContext(std::forward<TArgs>(args)...), std::bind(&SyncPtr::OnDelete, this, std::placeholders::_1)} {}

  /**
   * @brief Destroy the synched pointer and wait for all copies to get destroyed.
   *
   */
  ~SyncPtr() {
    ptr_.reset();
    SyncOnDelete();
  }

  SyncPtr(const SyncPtr &) = delete;
  SyncPtr &operator=(const SyncPtr &) = delete;
  SyncPtr(SyncPtr &&) noexcept = delete;
  SyncPtr &operator=(SyncPtr &&) noexcept = delete;

  /**
   * @brief Get (copy) the underlying shared pointer.
   *
   * @return std::shared_ptr<TContext>
   */
  std::shared_ptr<TContext> get() { return ptr_; }
  std::shared_ptr<const TContext> get() const { return ptr_; }

  /**
   * @brief Return saved configuration (metadata)
   *
   * @return TConfig
   */
  TConfig config() { return config_; }
  const TConfig &config() const { return config_; }

 private:
  /**
   * @brief Block until OnDelete gets called.
   *
   */
  void SyncOnDelete() {
    std::unique_lock<std::mutex> lock(in_use_mtx_);
    in_use_cv_.wait(lock, [this] { return !in_use_; });
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
    in_use_cv_.notify_one();
  }

  bool in_use_;                                //!< Flag used to signal sync
  mutable std::mutex in_use_mtx_;              //!< Mutex used in the cv sync
  mutable std::condition_variable in_use_cv_;  //!< cv used to signal a sync
  TConfig config_;                             //!< Additional metadata associated with the context
  std::shared_ptr<TContext> ptr_;              //!< Pointer being synced
};

template <typename TContext>
class SyncPtr<TContext, void> {
 public:
  /**
   * @brief Construct a new synched pointer.
   *
   * @tparam TArgs variable templates used by the TContext constructor
   * @param config Additional metadata associated with context
   * @param args Arguments to pass to TContext constructor
   */
  template <typename... TArgs>
  explicit SyncPtr(TArgs &&...args)
      : in_use_(true),
        ptr_{new TContext(std::forward<TArgs>(args)...), std::bind(&SyncPtr::OnDelete, this, std::placeholders::_1)} {}

  /**
   * @brief Destroy the synched pointer and wait for all copies to get destroyed.
   *
   */
  ~SyncPtr() {
    ptr_.reset();
    SyncOnDelete();
  }

  SyncPtr(const SyncPtr &) = delete;
  SyncPtr &operator=(const SyncPtr &) = delete;
  SyncPtr(SyncPtr &&) noexcept = delete;
  SyncPtr &operator=(SyncPtr &&) noexcept = delete;

  /**
   * @brief Get (copy) the underlying shared pointer.
   *
   * @return std::shared_ptr<TContext>
   */
  std::shared_ptr<TContext> get() { return ptr_; }
  std::shared_ptr<const TContext> get() const { return ptr_; }

 private:
  /**
   * @brief Block until OnDelete gets called.
   *
   */
  void SyncOnDelete() {
    std::unique_lock<std::mutex> lock(in_use_mtx_);
    in_use_cv_.wait(lock, [this] { return !in_use_; });
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
    in_use_cv_.notify_one();
  }

  bool in_use_;                                //!< Flag used to signal sync
  mutable std::mutex in_use_mtx_;              //!< Mutex used in the cv sync
  mutable std::condition_variable in_use_cv_;  //!< cv used to signal a sync
  std::shared_ptr<TContext> ptr_;              //!< Pointer being synced
};

}  // namespace memgraph::utils
