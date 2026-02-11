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

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "query/plan_v2/egraph.hpp"
#include "query/plan_v2/private_symbol.hpp"
#include "storage/v2/property_value.hpp"

namespace memgraph::query::plan::v2 {

// ========================================================================
// symbol_make_traits - each defines storage_type and make()
// make() receives only its storage + an emplacer callback
// ========================================================================

template <symbol S>
struct symbol_make_traits;

/// Once: auto-incrementing counter
template <>
struct symbol_make_traits<symbol::Once> {
  struct storage_type {
    uint64_t counter = 0;
  };

  template <typename Emplacer>
  static auto make(storage_type &s, Emplacer &&emplace) -> eclass {
    return emplace(s.counter++);
  }
};

/// Symbol: position -> name mapping
template <>
struct symbol_make_traits<symbol::Symbol> {
  struct storage_type {
    std::map<int32_t, std::string> store;
  };

  template <typename Emplacer>
  static auto make(storage_type &s, Emplacer &&emplace, int32_t pos, std::string_view name) -> eclass {
    s.store.try_emplace(pos, std::string{name});
    return emplace(static_cast<uint64_t>(pos));
  }
};

/// Literal: value -> id mapping
template <>
struct symbol_make_traits<symbol::Literal> {
  struct storage_type {
    std::map<storage::ExternalPropertyValue, uint64_t> store;
    uint64_t next_id = 0;
  };

  template <typename Emplacer>
  static auto make(storage_type &s, Emplacer &&emplace, storage::ExternalPropertyValue const &value) -> eclass {
    auto [it, inserted] = s.store.try_emplace(value, s.next_id);
    if (inserted) ++s.next_id;
    return emplace(it->second);
  }
};

/// ParamLookup: no storage, position IS the disambiguator
template <>
struct symbol_make_traits<symbol::ParamLookup> {
  struct storage_type {};

  template <typename Emplacer>
  static auto make(storage_type & /*s*/, Emplacer &&emplace, int32_t pos) -> eclass {
    return emplace(static_cast<uint64_t>(pos));
  }
};

/// Bind: no storage, just children
template <>
struct symbol_make_traits<symbol::Bind> {
  struct storage_type {};

  template <typename Emplacer>
  static auto make(storage_type & /*s*/, Emplacer &&emplace, eclass input, eclass sym, eclass expr) -> eclass {
    return emplace(input, sym, expr);
  }
};

/// Identifier: no storage, just child
template <>
struct symbol_make_traits<symbol::Identifier> {
  struct storage_type {};

  template <typename Emplacer>
  static auto make(storage_type & /*s*/, Emplacer &&emplace, eclass sym) -> eclass {
    return emplace(sym);
  }
};

/// Output: no storage, prepends input to children
template <>
struct symbol_make_traits<symbol::Output> {
  struct storage_type {};

  template <typename Emplacer>
  static auto make(storage_type & /*s*/, Emplacer &&emplace, eclass input, std::vector<eclass> named_outputs)
      -> eclass {
    named_outputs.insert(named_outputs.begin(), input);
    return emplace(std::move(named_outputs));
  }
};

/// NamedOutput: name -> id mapping + children
template <>
struct symbol_make_traits<symbol::NamedOutput> {
  struct storage_type {
    std::map<std::string, uint64_t> store;
    uint64_t next_id = 0;
  };

  template <typename Emplacer>
  static auto make(storage_type &s, Emplacer &&emplace, std::string_view name, eclass sym, eclass expr) -> eclass {
    auto [it, inserted] = s.store.try_emplace(std::string{name}, s.next_id);
    if (inserted) ++s.next_id;
    return emplace(it->second, sym, expr);
  }
};

// ========================================================================
// Combined storage using inheritance (like the overloads trick)
// ========================================================================

template <typename... Ts>
struct combined_storage : Ts... {};

template <symbol... Ss>
using symbol_storage_for = combined_storage<typename symbol_make_traits<Ss>::storage_type...>;

using symbol_storage = symbol_storage_for<symbol::Once, symbol::Symbol, symbol::Literal, symbol::ParamLookup,
                                          symbol::Bind, symbol::Identifier, symbol::Output, symbol::NamedOutput>;

}  // namespace memgraph::query::plan::v2
