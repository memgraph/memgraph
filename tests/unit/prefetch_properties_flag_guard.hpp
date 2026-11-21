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

#include "flags/general.hpp"

/// Restores the flag whatever the test does, so a failing EXPECT cannot leak the rewrite into later tests.
class PrefetchPropertiesFlagGuard {
 public:
  explicit PrefetchPropertiesFlagGuard(bool value) : previous_(FLAGS_query_prefetch_properties) {
    FLAGS_query_prefetch_properties = value;
  }

  ~PrefetchPropertiesFlagGuard() { FLAGS_query_prefetch_properties = previous_; }

  PrefetchPropertiesFlagGuard(const PrefetchPropertiesFlagGuard &) = delete;
  PrefetchPropertiesFlagGuard &operator=(const PrefetchPropertiesFlagGuard &) = delete;

 private:
  bool previous_;
};
