// Copyright 2026 Memgraph Ltd.
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
#include <memory>

namespace memgraph::metrics {

/// Shared control block for a prometheus metric pointer. The underlying raw
/// pointer can be atomically invalidated (nulled) during metric rebind while
/// long-lived consumers safely hold their own copy of the shared_ptr.
template <typename T>
using MetricPtr = std::shared_ptr<std::atomic<T *>>;

template <typename T>
MetricPtr<T> MakeMetricPtr(T *raw) {
  return std::make_shared<std::atomic<T *>>(raw);
}

/// Atomically detach a metric pointer: returns the raw pointer and nulls the
/// atomic in one step. Used to get the raw pointer for Family::Remove() while
/// simultaneously making in-flight consumers safe.
template <typename T>
T *Detach(MetricPtr<T> const &ref) noexcept {
  return ref ? ref->exchange(nullptr, std::memory_order_acq_rel) : nullptr;
}

}  // namespace memgraph::metrics
