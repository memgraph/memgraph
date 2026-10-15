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
#include <utility>

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
    using enum storage::PropertyValueType;
    // Identity is type-then-structure: no numeric coercion, so `1` (Int) and
    // `1.0` (Double) are different constants. PropertyValue's own `==` coerces.
    if (a.type() != b.type()) return false;
    switch (a.type()) {
      case Double: {
        // Structural, not IEEE: NaN equals NaN, but signed zero stays distinct
        // (`1.0/x` flips ±inf), so plain `==` isn't enough.
        double const da = a.ValueDouble();
        double const db = b.ValueDouble();
        if (std::isnan(da) && std::isnan(db)) return true;
        return da == db && std::signbit(da) == std::signbit(db);
      }
      case List: {
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
      case Map: {
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
      // Scalars fall back to PropertyValue's own `==`, exact once the types
      // match. The typed list variants (IntList/DoubleList/NumericList) also land
      // here, where `==` ignores signed zero - a known gap noted on the hash
      // companion, to revisit if they become element-iterable. Enumerated with no
      // `default` so a new type must be triaged here rather than silently coerce.
      case Null:
      case Bool:
      case Int:
      case String:
      case TemporalData:
      case ZonedTemporalData:
      case Enum:
      case Point2d:
      case Point3d:
      case IntList:
      case DoubleList:
      case NumericList:
      case VectorIndexId:
        return a == b;
    }
    std::unreachable();
  }
};

/// Hash companion to `ConstantIdentityEq`. Mixes the type tag into every hash
/// (load-bearing: it partitions `1` from `1.0`, which `std::hash` coerces),
/// unifies every NaN and folds in the sign of zero so `-0.0` and `+0.0` hash
/// apart to match the equality, and hashes Int/Bool/List/Map without the
/// coercing fallback. Other scalars fall back to `std::hash`, where it agrees
/// with `==`. The typed list variants would not (e.g. `DoubleList{-0.0}` vs
/// `{0.0}`) - revisit if those become iterable.
struct ConstantIdentityHash {
  auto operator()(this ConstantIdentityHash self, storage::ExternalPropertyValue const &v) -> std::size_t {
    using enum storage::PropertyValueType;
    std::size_t h = static_cast<std::size_t>(v.type());  // seed with the type tag: partitions `1` from `1.0`
    switch (v.type()) {
      case Double: {
        double const d = v.ValueDouble();
        // Unify every NaN (fold a fixed sentinel, not the varying bit pattern).
        if (std::isnan(d)) {
          boost::hash_combine(h, std::size_t{0x7ff8000000000000ULL});
        } else {
          boost::hash_combine(h, d);
          boost::hash_combine(h, std::signbit(d));  // so `-0.0`/`+0.0` hash apart, matching Eq
        }
        return h;
      }
      // Hash Int/Bool by their own type: std::hash<ExternalPropertyValue> hashes
      // Int as double, so large int64s (>2^53) would collide as doubles.
      case Int:
        boost::hash_combine(h, v.ValueInt());
        return h;
      case Bool:
        boost::hash_combine(h, v.ValueBool());
        return h;
      case List:
        for (auto const &elem : v.ValueList()) boost::hash_combine(h, self(elem));
        return h;
      case Map:
        for (auto const &[key, value] : v.ValueMap()) {
          boost::hash_combine(h, key);
          boost::hash_combine(h, self(value));
        }
        return h;
      // Other scalars fall back to std::hash, where it agrees with `==`. The
      // typed list variants would not (e.g. `DoubleList{-0.0}` vs `{0.0}`) -
      // revisit if those become iterable. Enumerated with no `default` so a new
      // type must be triaged here.
      case Null:
      case String:
      case TemporalData:
      case ZonedTemporalData:
      case Enum:
      case Point2d:
      case Point3d:
      case IntList:
      case DoubleList:
      case NumericList:
      case VectorIndexId:
        boost::hash_combine(h, std::hash<storage::ExternalPropertyValue>{}(v));
        return h;
    }
    std::unreachable();
  }
};

}  // namespace memgraph::query::plan::v2
