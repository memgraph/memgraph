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
class CachePropertiesFlagGuard {
 public:
  explicit CachePropertiesFlagGuard(bool value) : previous_(FLAGS_query_cache_properties) {
    FLAGS_query_cache_properties = value;
  }

  ~CachePropertiesFlagGuard() { FLAGS_query_cache_properties = previous_; }

  CachePropertiesFlagGuard(const CachePropertiesFlagGuard &) = delete;
  CachePropertiesFlagGuard &operator=(const CachePropertiesFlagGuard &) = delete;

 private:
  bool previous_;
};
