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

#pragma once

#include "storage/v2/delta.hpp"

namespace memgraph::storage {

struct TcoDelta {
  TcoDelta(Delta *in) : ptr_{in} { deleted(false); }

  bool deleted() const { return (uint64_t)ptr_ & 0x01; }
  void deleted(bool in) { ptr_ = delta() + in; }
  Delta *delta() const { return (Delta *)((uint64_t)ptr_ & 0xFFFFFFFFFFFFFFFE); }
  void delta(Delta *in) { ptr_ = in + deleted(); }

 private:
  Delta *ptr_;
};

}  // namespace memgraph::storage
