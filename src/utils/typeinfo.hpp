// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file
#pragma once

#include <cstdint>

namespace utils {

/// Type information on a C++ type.
///
/// You should embed this structure as a static constant member `kType` and make
/// sure you generate a unique ID for it. Also, if your type has inheritance,
/// you may want to add a `virtual utils::TypeInfo GetType();` method to get the
/// runtime type.
struct TypeInfo {
  /// Unique ID for the type.
  uint64_t id;
  /// Pretty name of the type.
  const char *name;
  /// `TypeInfo *` for superclass of this type.
  /// Multiple inheritance is not supported.
  const TypeInfo *superclass{nullptr};
};

inline bool operator==(const TypeInfo &a, const TypeInfo &b) { return a.id == b.id; }
inline bool operator!=(const TypeInfo &a, const TypeInfo &b) { return a.id != b.id; }
inline bool operator<(const TypeInfo &a, const TypeInfo &b) { return a.id < b.id; }
inline bool operator<=(const TypeInfo &a, const TypeInfo &b) { return a.id <= b.id; }
inline bool operator>(const TypeInfo &a, const TypeInfo &b) { return a.id > b.id; }
inline bool operator>=(const TypeInfo &a, const TypeInfo &b) { return a.id >= b.id; }

/// Return true if `a` is subtype or the same type as `b`.
inline bool IsSubtype(const TypeInfo &a, const TypeInfo &b) {
  if (a == b) return true;
  const TypeInfo *super_a = a.superclass;
  while (super_a) {
    if (*super_a == b) return true;
    super_a = super_a->superclass;
  }
  return false;
}

template <class T>
bool IsSubtype(const T &a, const TypeInfo &b) {
  return IsSubtype(a.GetTypeInfo(), b);
}

/// Downcast `a` to `TDerived` using static_cast.
///
/// If `a` is `nullptr` or `TBase` is not a subtype of `TDerived`, then a
/// `nullptr` is returned.
///
/// This downcast is ill-formed if TBase is ambiguous, inaccessible, or virtual
/// base (or a base of a virtual base) of TDerived.
template <class TDerived, class TBase>
TDerived *Downcast(TBase *a) {
  if (!a) return nullptr;
  if (IsSubtype(*a, TDerived::kType)) return static_cast<TDerived *>(a);
  return nullptr;
}

}  // namespace utils
