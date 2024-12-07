// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#define EBR_DIAGNOSTIC_MODE
#include "utils/epoch_based_reclaimation.hpp"

#include "gtest/gtest.h"

#include <optional>

using memgraph::utils::EpochBasedReclaimation;

struct EbrHandle;
struct S {
  using ebr_handle_t = EbrHandle;
  friend struct EbrHandle;

 private:
  S *next;
};

struct EbrHandle {
  static auto next(S const *obj) -> S const * { return obj->next; }
  static void set_next(S *obj, S *to_set) { obj->next = to_set; }
};

TEST(a, b) {
  auto ebr = EpochBasedReclaimation<int>{};
  auto pin = std::optional{ebr.pin()};
  ebr.release(new int{42});
  ASSERT_EQ(ebr.current_count(), 1);
  pin.reset();
  ebr.gc_run();
  ASSERT_EQ(ebr.current_count(), 0);
}
