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

#include <cmath>

#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {

/// Constant-identity equality: do two constants denote the same value for
/// analysis purposes? Distinct from runtime comparison - type-then-structure
/// (so `1` and `1.0` are different constants), `NaN` equals `NaN`, `null` equals
/// `null`, deep on `List`/`Map`. Allocator-agnostic and non-allocating.
struct ConstantIdentityEq {
  auto operator()(this ConstantIdentityEq self, storage::ExternalPropertyValue const &a,
                  storage::ExternalPropertyValue const &b) -> bool {
    // Identity is type-then-structure: no numeric coercion, so `1` (Int) and
    // `1.0` (Double) are different constants. PropertyValue's own `==` coerces.
    if (a.type() != b.type()) return false;
    if (a.IsDouble()) {
      // Structural, not IEEE: a folded `0.0/0.0` is the same constant as any
      // other NaN, so NaN compares equal to NaN here.
      double const da = a.ValueDouble();
      double const db = b.ValueDouble();
      if (std::isnan(da) && std::isnan(db)) return true;
      return da == db;
    }
    if (a.IsList()) {
      // Recurse so the identity rules (type-then-structure, NaN) apply to
      // elements too; PropertyValue's list `==` would coerce and mis-handle NaN.
      auto const &la = a.ValueList();
      auto const &lb = b.ValueList();
      if (la.size() != lb.size()) return false;
      for (std::size_t i = 0; i < la.size(); ++i) {
        if (!self(la[i], lb[i])) return false;
      }
      return true;
    }
    if (a.IsMap()) {
      // Same reason as lists: recurse on values, key-wise.
      auto const &ma = a.ValueMap();
      auto const &mb = b.ValueMap();
      if (ma.size() != mb.size()) return false;
      auto ita = ma.begin();
      auto itb = mb.begin();
      for (; ita != ma.end(); ++ita, ++itb) {
        if (ita->first != itb->first) return false;
        if (!self(ita->second, itb->second)) return false;
      }
      return true;
    }
    return a == b;
  }
};

}  // namespace memgraph::query::plan::v2
