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
#include <cstddef>
#include <functional>

#include <boost/container_hash/hash.hpp>

import memgraph.storage.property_value;

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

/// Hash companion to `ConstantIdentityEq`. Mixes the type tag into every hash
/// (load-bearing: it partitions `1` from `1.0`, which `std::hash` coerces),
/// unifies every NaN and normalizes `-0.0` to `+0.0` to match the equality,
/// and hashes Int/Bool/List/Map without the coercing fallback. Other scalars
/// fall back to `std::hash`, where it agrees with `==`. The typed list variants
/// would not (e.g. `DoubleList{-0.0}` vs `{0.0}`) - revisit if those become
/// iterable.
struct ConstantIdentityHash {
  auto operator()(this ConstantIdentityHash self, storage::ExternalPropertyValue const &v) -> std::size_t {
    std::size_t h = static_cast<std::size_t>(v.type());  // seed with the type tag: partitions `1` from `1.0`
    if (v.IsDouble()) {
      double const d = v.ValueDouble();
      // Unify every NaN (fold a fixed sentinel, not the varying bit pattern) and
      // normalize -0.0 to +0.0, so doubles equal under Eq hash alike.
      if (std::isnan(d)) {
        boost::hash_combine(h, std::size_t{0x7ff8000000000000ULL});
      } else {
        boost::hash_combine(h, d == 0.0 ? 0.0 : d);
      }
      return h;
    }
    // Hash Int/Bool by their own type: std::hash<ExternalPropertyValue> hashes
    // Int as double, so large int64s (>2^53) would collide as doubles.
    if (v.IsInt()) {
      boost::hash_combine(h, v.ValueInt());
      return h;
    }
    if (v.IsBool()) {
      boost::hash_combine(h, v.ValueBool());
      return h;
    }
    if (v.IsList()) {
      for (auto const &elem : v.ValueList()) boost::hash_combine(h, self(elem));
      return h;
    }
    if (v.IsMap()) {
      for (auto const &[key, value] : v.ValueMap()) {
        boost::hash_combine(h, key);
        boost::hash_combine(h, self(value));
      }
      return h;
    }
    boost::hash_combine(h, std::hash<storage::ExternalPropertyValue>{}(v));
    return h;
  }
};

}  // namespace memgraph::query::plan::v2
