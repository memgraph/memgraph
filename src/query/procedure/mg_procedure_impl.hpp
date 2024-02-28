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

/// @file
/// Contains private (implementation) declarations and definitions for
/// mg_procedure.h
#pragma once

#include "mg_procedure.h"

#include <optional>
#include <ostream>

#include "integrations/kafka/consumer.hpp"
#include "integrations/pulsar/consumer.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"

#include "query/procedure/cypher_type_ptr.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/map.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/temporal.hpp"
#include "utils/variant_helpers.hpp"
/// Wraps memory resource used in custom procedures.
///
/// This should have been `using mgp_memory = memgraph::utils::MemoryResource`, but that's
/// not valid C++ because we have a forward declare `struct mgp_memory` in
/// mg_procedure.h
/// TODO: Make this extendable in C API, so that custom procedure writer can add
/// their own memory management wrappers.
struct mgp_memory {
  memgraph::utils::MemoryResource *impl;
};

/// Immutable container of various values that appear in openCypher.
struct mgp_value {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = memgraph::utils::Allocator<mgp_value>;

  // Construct MGP_VALUE_TYPE_NULL.
  explicit mgp_value(memgraph::utils::MemoryResource *) noexcept;

  mgp_value(bool, memgraph::utils::MemoryResource *) noexcept;
  mgp_value(int64_t, memgraph::utils::MemoryResource *) noexcept;
  mgp_value(double, memgraph::utils::MemoryResource *) noexcept;
  /// @throw std::bad_alloc
  mgp_value(const char *, memgraph::utils::MemoryResource *);
  /// Take ownership of the mgp_list, MemoryResource must match.
  mgp_value(mgp_list *, memgraph::utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_map, MemoryResource must match.
  mgp_value(mgp_map *, memgraph::utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_vertex, MemoryResource must match.
  mgp_value(mgp_vertex *, memgraph::utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_edge, MemoryResource must match.
  mgp_value(mgp_edge *, memgraph::utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_path, MemoryResource must match.
  mgp_value(mgp_path *, memgraph::utils::MemoryResource *) noexcept;

  mgp_value(mgp_date *, memgraph::utils::MemoryResource *) noexcept;
  mgp_value(mgp_local_time *, memgraph::utils::MemoryResource *) noexcept;
  mgp_value(mgp_local_date_time *, memgraph::utils::MemoryResource *) noexcept;
  mgp_value(mgp_duration *, memgraph::utils::MemoryResource *) noexcept;

  /// Construct by copying memgraph::query::TypedValue using memgraph::utils::MemoryResource.
  /// mgp_graph is needed to construct mgp_vertex and mgp_edge.
  /// @throw std::bad_alloc
  mgp_value(const memgraph::query::TypedValue &, mgp_graph *, memgraph::utils::MemoryResource *);

  /// Construct by copying memgraph::storage::PropertyValue using memgraph::utils::MemoryResource.
  /// @throw std::bad_alloc
  mgp_value(const memgraph::storage::PropertyValue &, memgraph::utils::MemoryResource *);

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_value(const mgp_value &) = delete;

  /// Copy construct using given memgraph::utils::MemoryResource.
  /// @throw std::bad_alloc
  mgp_value(const mgp_value &, memgraph::utils::MemoryResource *);

  /// Move construct using given memgraph::utils::MemoryResource.
  /// @throw std::bad_alloc if MemoryResource is different, so we cannot move.
  mgp_value(mgp_value &&, memgraph::utils::MemoryResource *);

  /// Move construct, memgraph::utils::MemoryResource is inherited.
  mgp_value(mgp_value &&other) noexcept : mgp_value(other, other.memory) {}

  /// Copy-assignment is not allowed to preserve immutability.
  mgp_value &operator=(const mgp_value &) = delete;

  /// Move-assignment is not allowed to preserve immutability.
  mgp_value &operator=(mgp_value &&) = delete;

  ~mgp_value() noexcept;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  mgp_value_type type;
  memgraph::utils::MemoryResource *memory;

  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    memgraph::utils::pmr::string string_v;
    // We use pointers so that taking ownership via C API is easier. Besides,
    // mgp_map cannot use incomplete mgp_value type, because that would be
    // undefined behaviour.
    mgp_list *list_v;
    mgp_map *map_v;
    mgp_vertex *vertex_v;
    mgp_edge *edge_v;
    mgp_path *path_v;
    mgp_date *date_v;
    mgp_local_time *local_time_v;
    mgp_local_date_time *local_date_time_v;
    mgp_duration *duration_v;
  };
};

inline memgraph::utils::DateParameters MapDateParameters(const mgp_date_parameters *parameters) {
  return {.year = parameters->year, .month = parameters->month, .day = parameters->day};
}

struct mgp_date {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = memgraph::utils::Allocator<mgp_date>;

  // Hopefully memgraph::utils::Date copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<memgraph::utils::Date>);

  mgp_date(const memgraph::utils::Date &date, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), date(date) {}

  mgp_date(const std::string_view string, memgraph::utils::MemoryResource *memory)
      : memory(memory), date(memgraph::utils::ParseDateParameters(string).first) {}

  mgp_date(const mgp_date_parameters *parameters, memgraph::utils::MemoryResource *memory)
      : memory(memory), date(MapDateParameters(parameters)) {}

  mgp_date(const int64_t microseconds, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), date(microseconds) {}

  mgp_date(const mgp_date &other, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), date(other.date) {}

  mgp_date(mgp_date &&other, memgraph::utils::MemoryResource *memory) noexcept : memory(memory), date(other.date) {}

  mgp_date(mgp_date &&other) noexcept : memory(other.memory), date(other.date) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_date(const mgp_date &) = delete;

  mgp_date &operator=(const mgp_date &) = delete;
  mgp_date &operator=(mgp_date &&) = delete;

  ~mgp_date() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  memgraph::utils::MemoryResource *memory;
  memgraph::utils::Date date;
};

inline memgraph::utils::LocalTimeParameters MapLocalTimeParameters(const mgp_local_time_parameters *parameters) {
  return {.hour = parameters->hour,
          .minute = parameters->minute,
          .second = parameters->second,
          .millisecond = parameters->millisecond,
          .microsecond = parameters->microsecond};
}

struct mgp_local_time {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = memgraph::utils::Allocator<mgp_local_time>;

  // Hopefully memgraph::utils::LocalTime copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<memgraph::utils::LocalTime>);

  mgp_local_time(const std::string_view string, memgraph::utils::MemoryResource *memory)
      : memory(memory), local_time(memgraph::utils::ParseLocalTimeParameters(string).first) {}

  mgp_local_time(const mgp_local_time_parameters *parameters, memgraph::utils::MemoryResource *memory)
      : memory(memory), local_time(MapLocalTimeParameters(parameters)) {}

  mgp_local_time(const memgraph::utils::LocalTime &local_time, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(local_time) {}

  mgp_local_time(const int64_t microseconds, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(microseconds) {}

  mgp_local_time(const mgp_local_time &other, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(other.local_time) {}

  mgp_local_time(mgp_local_time &&other, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(other.local_time) {}

  mgp_local_time(mgp_local_time &&other) noexcept : memory(other.memory), local_time(other.local_time) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_local_time(const mgp_local_time &) = delete;

  mgp_local_time &operator=(const mgp_local_time &) = delete;
  mgp_local_time &operator=(mgp_local_time &&) = delete;

  ~mgp_local_time() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  memgraph::utils::MemoryResource *memory;
  memgraph::utils::LocalTime local_time;
};

inline memgraph::utils::LocalDateTime CreateLocalDateTimeFromString(const std::string_view string) {
  const auto &[date_parameters, local_time_parameters] = memgraph::utils::ParseLocalDateTimeParameters(string);
  return memgraph::utils::LocalDateTime{date_parameters, local_time_parameters};
}

struct mgp_local_date_time {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = memgraph::utils::Allocator<mgp_local_date_time>;

  // Hopefully memgraph::utils::LocalDateTime copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<memgraph::utils::LocalDateTime>);

  mgp_local_date_time(const memgraph::utils::LocalDateTime &local_date_time,
                      memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(local_date_time) {}

  mgp_local_date_time(const std::string_view string, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(CreateLocalDateTimeFromString(string)) {}

  mgp_local_date_time(const mgp_local_date_time_parameters *parameters, memgraph::utils::MemoryResource *memory)
      : memory(memory),
        local_date_time(MapDateParameters(parameters->date_parameters),
                        MapLocalTimeParameters(parameters->local_time_parameters)) {}

  mgp_local_date_time(const int64_t microseconds, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(microseconds) {}

  mgp_local_date_time(const mgp_local_date_time &other, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(other.local_date_time) {}

  mgp_local_date_time(mgp_local_date_time &&other, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(other.local_date_time) {}

  mgp_local_date_time(mgp_local_date_time &&other) noexcept
      : memory(other.memory), local_date_time(other.local_date_time) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_local_date_time(const mgp_local_date_time &) = delete;

  mgp_local_date_time &operator=(const mgp_local_date_time &) = delete;
  mgp_local_date_time &operator=(mgp_local_date_time &&) = delete;

  ~mgp_local_date_time() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  memgraph::utils::MemoryResource *memory;
  memgraph::utils::LocalDateTime local_date_time;
};

inline memgraph::utils::DurationParameters MapDurationParameters(const mgp_duration_parameters *parameters) {
  return {.day = parameters->day,
          .hour = parameters->hour,
          .minute = parameters->minute,
          .second = parameters->second,
          .millisecond = parameters->millisecond,
          .microsecond = parameters->microsecond};
}

struct mgp_duration {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = memgraph::utils::Allocator<mgp_duration>;

  // Hopefully memgraph::utils::Duration copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<memgraph::utils::Duration>);

  mgp_duration(const std::string_view string, memgraph::utils::MemoryResource *memory)
      : memory(memory), duration(memgraph::utils::ParseDurationParameters(string)) {}

  mgp_duration(const mgp_duration_parameters *parameters, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), duration(MapDurationParameters(parameters)) {}

  mgp_duration(const memgraph::utils::Duration &duration, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), duration(duration) {}

  mgp_duration(const int64_t microseconds, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), duration(microseconds) {}

  mgp_duration(const mgp_duration &other, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), duration(other.duration) {}

  mgp_duration(mgp_duration &&other, memgraph::utils::MemoryResource *memory) noexcept
      : memory(memory), duration(other.duration) {}

  mgp_duration(mgp_duration &&other) noexcept : memory(other.memory), duration(other.duration) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_duration(const mgp_duration &) = delete;

  mgp_duration &operator=(const mgp_duration &) = delete;
  mgp_duration &operator=(mgp_duration &&) = delete;

  ~mgp_duration() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  memgraph::utils::MemoryResource *memory;
  memgraph::utils::Duration duration;
};

struct mgp_list {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = memgraph::utils::Allocator<mgp_list>;

  explicit mgp_list(memgraph::utils::MemoryResource *memory) : elems(memory) {}

  mgp_list(memgraph::utils::pmr::vector<mgp_value> &&elems, memgraph::utils::MemoryResource *memory)
      : elems(std::move(elems), memory) {}

  mgp_list(const mgp_list &other, memgraph::utils::MemoryResource *memory) : elems(other.elems, memory) {}

  mgp_list(mgp_list &&other, memgraph::utils::MemoryResource *memory) : elems(std::move(other.elems), memory) {}

  mgp_list(mgp_list &&other) noexcept : elems(std::move(other.elems)) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_list(const mgp_list &) = delete;

  mgp_list &operator=(const mgp_list &) = delete;
  mgp_list &operator=(mgp_list &&) = delete;

  ~mgp_list() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept {
    return elems.get_allocator().GetMemoryResource();
  }

  // C++17 vector can work with incomplete type.
  memgraph::utils::pmr::vector<mgp_value> elems;
};

struct mgp_map {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = memgraph::utils::Allocator<mgp_map>;

  explicit mgp_map(memgraph::utils::MemoryResource *memory) : items(memory) {}

  mgp_map(memgraph::utils::pmr::map<memgraph::utils::pmr::string, mgp_value> &&items,
          memgraph::utils::MemoryResource *memory)
      : items(std::move(items), memory) {}

  mgp_map(const mgp_map &other, memgraph::utils::MemoryResource *memory) : items(other.items, memory) {}

  mgp_map(mgp_map &&other, memgraph::utils::MemoryResource *memory) : items(std::move(other.items), memory) {}

  mgp_map(mgp_map &&other) noexcept : items(std::move(other.items)) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_map(const mgp_map &) = delete;

  mgp_map &operator=(const mgp_map &) = delete;
  mgp_map &operator=(mgp_map &&) = delete;

  ~mgp_map() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept {
    return items.get_allocator().GetMemoryResource();
  }

  // Unfortunately using incomplete type with map is undefined, so mgp_map
  // needs to be defined after mgp_value.
  memgraph::utils::pmr::map<memgraph::utils::pmr::string, mgp_value> items;
};

struct mgp_map_item {
  const char *key;
  mgp_value *value;
};

struct mgp_map_items_iterator {
  using allocator_type = memgraph::utils::Allocator<mgp_map_items_iterator>;

  mgp_map_items_iterator(mgp_map *map, memgraph::utils::MemoryResource *memory)
      : memory(memory), map(map), current_it(map->items.begin()) {
    if (current_it != map->items.end()) {
      current.key = current_it->first.c_str();
      current.value = &current_it->second;
    }
  }

  mgp_map_items_iterator(const mgp_map_items_iterator &) = delete;
  mgp_map_items_iterator(mgp_map_items_iterator &&) = delete;
  mgp_map_items_iterator &operator=(const mgp_map_items_iterator &) = delete;
  mgp_map_items_iterator &operator=(mgp_map_items_iterator &&) = delete;

  ~mgp_map_items_iterator() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const { return memory; }

  memgraph::utils::MemoryResource *memory;
  mgp_map *map;
  decltype(map->items.begin()) current_it;
  mgp_map_item current;
};

struct mgp_vertex {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = memgraph::utils::Allocator<mgp_vertex>;

  mgp_vertex(memgraph::query::VertexAccessor v, mgp_graph *graph, memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(v), graph(graph) {}

  mgp_vertex(memgraph::query::SubgraphVertexAccessor v, mgp_graph *graph, memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(v), graph(graph) {}

  mgp_vertex(const mgp_vertex &other, memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(other.impl), graph(other.graph) {}

  mgp_vertex(mgp_vertex &&other, memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(other.impl), graph(other.graph) {}

  // NOLINTNEXTLINE(hicpp-noexcept-move, performance-noexcept-move-constructor)
  mgp_vertex(mgp_vertex &&other) : memory(other.memory), impl(other.impl), graph(other.graph) {}

  memgraph::query::VertexAccessor getImpl() const {
    return std::visit(
        memgraph::utils::Overloaded{[](const memgraph::query::VertexAccessor &impl) { return impl; },
                                    [](memgraph::query::SubgraphVertexAccessor impl) { return impl.impl_; }},
        this->impl);
  }

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_vertex(const mgp_vertex &) = delete;

  mgp_vertex &operator=(const mgp_vertex &) = delete;
  mgp_vertex &operator=(mgp_vertex &&) = delete;

  bool operator==(const mgp_vertex &other) const noexcept { return other.getImpl() == this->getImpl(); }

  bool operator!=(const mgp_vertex &other) const noexcept { return !(*this == other); };

  ~mgp_vertex() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  memgraph::utils::MemoryResource *memory;
  std::variant<memgraph::query::VertexAccessor, memgraph::query::SubgraphVertexAccessor> impl;
  mgp_graph *graph;
};

struct mgp_edge {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = memgraph::utils::Allocator<mgp_edge>;

  static mgp_edge *Copy(const mgp_edge &edge, mgp_memory &memory);

  mgp_edge(const memgraph::query::EdgeAccessor &impl, mgp_graph *graph, memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(impl), from(impl.From(), graph, memory), to(impl.To(), graph, memory) {}

  mgp_edge(const memgraph::query::EdgeAccessor &impl, const memgraph::query::VertexAccessor &from_v,
           const memgraph::query::VertexAccessor &to_v, mgp_graph *graph, memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(impl), from(from_v, graph, memory), to(to_v, graph, memory) {}

  mgp_edge(const memgraph::query::EdgeAccessor &impl, const memgraph::query::SubgraphVertexAccessor &from_v,
           const memgraph::query::SubgraphVertexAccessor &to_v, mgp_graph *graph,
           memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(impl), from(from_v, graph, memory), to(to_v, graph, memory) {}

  mgp_edge(const mgp_edge &other, memgraph::utils::MemoryResource *memory)
      : memory(memory), impl(other.impl), from(other.from, memory), to(other.to, memory) {}

  mgp_edge(mgp_edge &&other, memgraph::utils::MemoryResource *memory)
      : memory(other.memory), impl(other.impl), from(std::move(other.from), memory), to(std::move(other.to), memory) {}

  // NOLINTNEXTLINE(hicpp-noexcept-move, performance-noexcept-move-constructor)
  mgp_edge(mgp_edge &&other)
      : memory(other.memory), impl(other.impl), from(std::move(other.from)), to(std::move(other.to)) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_edge(const mgp_edge &) = delete;

  mgp_edge &operator=(const mgp_edge &) = delete;
  mgp_edge &operator=(mgp_edge &&) = delete;
  ~mgp_edge() = default;

  bool operator==(const mgp_edge &other) const noexcept { return this->impl == other.impl; }
  bool operator!=(const mgp_edge &other) const noexcept { return !(*this == other); };

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  memgraph::utils::MemoryResource *memory;
  memgraph::query::EdgeAccessor impl;
  mgp_vertex from;
  mgp_vertex to;
};

struct mgp_path {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = memgraph::utils::Allocator<mgp_path>;

  explicit mgp_path(memgraph::utils::MemoryResource *memory) : vertices(memory), edges(memory) {}

  mgp_path(const mgp_path &other, memgraph::utils::MemoryResource *memory)
      : vertices(other.vertices, memory), edges(other.edges, memory) {}

  mgp_path(mgp_path &&other, memgraph::utils::MemoryResource *memory)
      : vertices(std::move(other.vertices), memory), edges(std::move(other.edges), memory) {}

  mgp_path(mgp_path &&other) noexcept : vertices(std::move(other.vertices)), edges(std::move(other.edges)) {}

  /// Copy construction without memgraph::utils::MemoryResource is not allowed.
  mgp_path(const mgp_path &) = delete;

  mgp_path &operator=(const mgp_path &) = delete;
  mgp_path &operator=(mgp_path &&) = delete;

  ~mgp_path() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const noexcept {
    return vertices.get_allocator().GetMemoryResource();
  }

  memgraph::utils::pmr::vector<mgp_vertex> vertices;
  memgraph::utils::pmr::vector<mgp_edge> edges;
};

struct mgp_graph {
  std::variant<memgraph::query::DbAccessor *, memgraph::query::SubgraphDbAccessor *> impl;
  memgraph::storage::View view;
  // TODO: Merge `mgp_graph` and `mgp_memory` into a single `mgp_context`. The
  // `ctx` field is out of place here.
  memgraph::query::ExecutionContext *ctx;
  memgraph::storage::StorageMode storage_mode;

  static mgp_graph WritableGraph(memgraph::query::DbAccessor &acc, memgraph::storage::View view,
                                 memgraph::query::ExecutionContext &ctx) {
    return mgp_graph{&acc, view, &ctx, acc.GetStorageMode()};
  }

  static mgp_graph NonWritableGraph(memgraph::query::DbAccessor &acc, memgraph::storage::View view) {
    return mgp_graph{&acc, view, nullptr, acc.GetStorageMode()};
  }

  static mgp_graph WritableGraph(memgraph::query::SubgraphDbAccessor &acc, memgraph::storage::View view,
                                 memgraph::query::ExecutionContext &ctx) {
    return mgp_graph{&acc, view, &ctx, acc.GetStorageMode()};
  }

  static mgp_graph NonWritableGraph(memgraph::query::SubgraphDbAccessor &acc, memgraph::storage::View view) {
    return mgp_graph{&acc, view, nullptr, acc.GetStorageMode()};
  }
};

struct mgp_result_record {
  /// Result record signature as defined for mgp_proc.
  const memgraph::utils::pmr::map<memgraph::utils::pmr::string,
                                  std::pair<const memgraph::query::procedure::CypherType *, bool>> *signature;
  memgraph::utils::pmr::map<memgraph::utils::pmr::string, memgraph::query::TypedValue> values;
  bool ignore_deleted_values = false;
  bool has_deleted_values = false;
};

struct mgp_result {
  explicit mgp_result(
      const memgraph::utils::pmr::map<memgraph::utils::pmr::string,
                                      std::pair<const memgraph::query::procedure::CypherType *, bool>> *signature,
      memgraph::utils::MemoryResource *mem)
      : signature(signature), rows(mem) {}

  /// Result record signature as defined for mgp_proc.
  const memgraph::utils::pmr::map<memgraph::utils::pmr::string,
                                  std::pair<const memgraph::query::procedure::CypherType *, bool>> *signature;
  memgraph::utils::pmr::vector<mgp_result_record> rows;
  std::optional<memgraph::utils::pmr::string> error_msg;
  bool is_transactional = true;
};

struct mgp_func_result {
  mgp_func_result() {}
  /// Return Magic function result. If user forgets it, the error is raised
  std::optional<memgraph::query::TypedValue> value;
  /// Return Magic function result with potential error
  std::optional<memgraph::utils::pmr::string> error_msg;
};

// Prevents user to use ExecutionContext in writable callables
struct mgp_func_context {
  memgraph::query::DbAccessor *impl;
  memgraph::storage::View view;
};

struct mgp_properties_iterator {
  using allocator_type = memgraph::utils::Allocator<mgp_properties_iterator>;

  // Define members at the start because we use decltype a lot here, so members
  // need to be visible in method definitions.

  memgraph::utils::MemoryResource *memory;
  mgp_graph *graph;
  std::remove_reference_t<decltype(*std::declval<memgraph::query::VertexAccessor>().Properties(graph->view))> pvs;
  decltype(pvs.begin()) current_it;
  std::optional<std::pair<memgraph::utils::pmr::string, mgp_value>> current;
  mgp_property property{nullptr, nullptr};

  // Construct with no properties.
  explicit mgp_properties_iterator(mgp_graph *graph, memgraph::utils::MemoryResource *memory)
      : memory(memory), graph(graph), current_it(pvs.begin()) {}

  // May throw who the #$@! knows what because PropertyValueStore doesn't
  // document what it throws, and it may surely throw some piece of !@#$
  // exception because it's built on top of STL and other libraries.
  mgp_properties_iterator(mgp_graph *graph, decltype(pvs) pvs, memgraph::utils::MemoryResource *memory)
      : memory(memory), graph(graph), pvs(std::move(pvs)), current_it(this->pvs.begin()) {
    if (current_it != this->pvs.end()) {
      auto value = std::visit(
          [this, memory](const auto *impl) {
            return memgraph::utils::pmr::string(impl->PropertyToName(current_it->first), memory);
          },
          graph->impl);

      current.emplace(value, mgp_value(current_it->second, memory));
      property.name = current->first.c_str();
      property.value = &current->second;
    }
  }

  mgp_properties_iterator(const mgp_properties_iterator &) = delete;
  mgp_properties_iterator(mgp_properties_iterator &&) = delete;

  mgp_properties_iterator &operator=(const mgp_properties_iterator &) = delete;
  mgp_properties_iterator &operator=(mgp_properties_iterator &&) = delete;

  ~mgp_properties_iterator() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const { return memory; }
};

struct mgp_edges_iterator {
  using allocator_type = memgraph::utils::Allocator<mgp_edges_iterator>;

  mgp_edges_iterator(const mgp_vertex &v, memgraph::utils::MemoryResource *memory)
      : memory(memory), source_vertex(v, memory) {}

  // NOLINTNEXTLINE(hicpp-noexcept-move, performance-noexcept-move-constructor)
  mgp_edges_iterator(mgp_edges_iterator &&other)
      : memory(other.memory),
        source_vertex(std::move(other.source_vertex)),
        in(std::move(other.in)),
        in_it(std::move(other.in_it)),
        out(std::move(other.out)),
        out_it(std::move(other.out_it)),
        current_e(std::move(other.current_e)) {}

  mgp_edges_iterator(const mgp_edges_iterator &) = delete;
  mgp_edges_iterator &operator=(const mgp_edges_iterator &) = delete;
  mgp_edges_iterator &operator=(mgp_edges_iterator &&) = delete;

  ~mgp_edges_iterator() = default;

  memgraph::utils::MemoryResource *GetMemoryResource() const { return memory; }

  memgraph::utils::MemoryResource *memory;
  mgp_vertex source_vertex;

  std::optional<std::remove_reference_t<decltype(std::get<memgraph::query::VertexAccessor>(source_vertex.impl)
                                                     .InEdges(source_vertex.graph->view)
                                                     ->edges)>>
      in;
  std::optional<decltype(in->begin())> in_it;
  std::optional<std::remove_reference_t<decltype(std::get<memgraph::query::VertexAccessor>(source_vertex.impl)
                                                     .OutEdges(source_vertex.graph->view)
                                                     ->edges)>>
      out;
  std::optional<decltype(out->begin())> out_it;
  std::optional<mgp_edge> current_e;
};

struct mgp_vertices_iterator {
  using allocator_type = memgraph::utils::Allocator<mgp_vertices_iterator>;

  /// @throw anything VerticesIterable may throw
  mgp_vertices_iterator(mgp_graph *graph, memgraph::utils::MemoryResource *memory);

  memgraph::utils::MemoryResource *GetMemoryResource() const { return memory; }

  memgraph::utils::MemoryResource *memory;
  mgp_graph *graph;
  memgraph::query::VerticesIterable vertices;
  decltype(vertices.begin()) current_it;
  std::optional<mgp_vertex> current_v;
};

struct mgp_type {
  memgraph::query::procedure::CypherTypePtr impl;
};

struct ProcedureInfo {
  bool is_write{false};
  bool is_batched{false};
  std::optional<memgraph::query::AuthQuery::Privilege> required_privilege = std::nullopt;
};

struct mgp_proc {
  using allocator_type = memgraph::utils::Allocator<mgp_proc>;

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const char *name, mgp_proc_cb cb, memgraph::utils::MemoryResource *memory, const ProcedureInfo &info = {})
      : name(name, memory), cb(cb), args(memory), opt_args(memory), results(memory), info(info) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const char *name, mgp_proc_cb cb, mgp_proc_initializer initializer, mgp_proc_cleanup cleanup,
           memgraph::utils::MemoryResource *memory, const ProcedureInfo &info = {})
      : name(name, memory),
        cb(cb),
        initializer(initializer),
        cleanup(cleanup),
        args(memory),
        opt_args(memory),
        results(memory),
        info(info) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const char *name, std::function<void(mgp_list *, mgp_graph *, mgp_result *, mgp_memory *)> cb,
           std::function<void(mgp_list *, mgp_graph *, mgp_memory *)> initializer, std::function<void()> cleanup,
           memgraph::utils::MemoryResource *memory, const ProcedureInfo &info = {})
      : name(name, memory),
        cb(cb),
        initializer(initializer),
        cleanup(cleanup),
        args(memory),
        opt_args(memory),
        results(memory),
        info(info) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const char *name, std::function<void(mgp_list *, mgp_graph *, mgp_result *, mgp_memory *)> cb,
           memgraph::utils::MemoryResource *memory, const ProcedureInfo &info = {})
      : name(name, memory), cb(cb), args(memory), opt_args(memory), results(memory), info(info) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const std::string_view name, std::function<void(mgp_list *, mgp_graph *, mgp_result *, mgp_memory *)> cb,
           memgraph::utils::MemoryResource *memory, const ProcedureInfo &info = {})
      : name(name, memory), cb(cb), args(memory), opt_args(memory), results(memory), info(info) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const mgp_proc &other, memgraph::utils::MemoryResource *memory)
      : name(other.name, memory),
        cb(other.cb),
        initializer(other.initializer),
        cleanup(other.cleanup),
        args(other.args, memory),
        opt_args(other.opt_args, memory),
        results(other.results, memory),
        info(other.info) {}

  mgp_proc(mgp_proc &&other, memgraph::utils::MemoryResource *memory)
      : name(std::move(other.name), memory),
        cb(std::move(other.cb)),
        initializer(other.initializer),
        cleanup(other.cleanup),
        args(std::move(other.args), memory),
        opt_args(std::move(other.opt_args), memory),
        results(std::move(other.results), memory),
        info(other.info) {}

  mgp_proc(const mgp_proc &other) = default;
  mgp_proc(mgp_proc &&other) = default;

  mgp_proc &operator=(const mgp_proc &) = delete;
  mgp_proc &operator=(mgp_proc &&) = delete;

  ~mgp_proc() = default;

  /// Name of the procedure.
  memgraph::utils::pmr::string name;
  /// Entry-point for the procedure.
  std::function<void(mgp_list *, mgp_graph *, mgp_result *, mgp_memory *)> cb;

  /// Initializer for batched procedure.
  std::optional<std::function<void(mgp_list *, mgp_graph *, mgp_memory *)>> initializer;

  /// Dtor for batched procedure.
  std::optional<std::function<void()>> cleanup;

  /// Required, positional arguments as a (name, type) pair.
  memgraph::utils::pmr::vector<std::pair<memgraph::utils::pmr::string, const memgraph::query::procedure::CypherType *>>
      args;
  /// Optional positional arguments as a (name, type, default_value) tuple.
  memgraph::utils::pmr::vector<std::tuple<memgraph::utils::pmr::string, const memgraph::query::procedure::CypherType *,
                                          memgraph::query::TypedValue>>
      opt_args;
  /// Fields this procedure returns, as a (name -> (type, is_deprecated)) map.
  memgraph::utils::pmr::map<memgraph::utils::pmr::string,
                            std::pair<const memgraph::query::procedure::CypherType *, bool>>
      results;
  ProcedureInfo info;
};

struct mgp_trans {
  using allocator_type = memgraph::utils::Allocator<mgp_trans>;

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_trans(const char *name, mgp_trans_cb cb, memgraph::utils::MemoryResource *memory)
      : name(name, memory), cb(cb), results(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_trans(const char *name, std::function<void(mgp_messages *, mgp_graph *, mgp_result *, mgp_memory *)> cb,
            memgraph::utils::MemoryResource *memory)
      : name(name, memory), cb(cb), results(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_trans(const mgp_trans &other, memgraph::utils::MemoryResource *memory)
      : name(other.name, memory), cb(other.cb), results(other.results) {}

  mgp_trans(mgp_trans &&other, memgraph::utils::MemoryResource *memory)
      : name(std::move(other.name), memory), cb(std::move(other.cb)), results(std::move(other.results)) {}

  mgp_trans(const mgp_trans &other) = default;
  mgp_trans(mgp_trans &&other) = default;

  mgp_trans &operator=(const mgp_trans &) = delete;
  mgp_trans &operator=(mgp_trans &&) = delete;

  ~mgp_trans() = default;

  /// Name of the transformation.
  memgraph::utils::pmr::string name;
  /// Entry-point for the transformation.
  std::function<void(mgp_messages *, mgp_graph *, mgp_result *, mgp_memory *)> cb;
  /// Fields this transformation returns.
  memgraph::utils::pmr::map<memgraph::utils::pmr::string,
                            std::pair<const memgraph::query::procedure::CypherType *, bool>>
      results;
};

struct mgp_func {
  using allocator_type = memgraph::utils::Allocator<mgp_func>;

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_func(const char *name, mgp_func_cb cb, memgraph::utils::MemoryResource *memory)
      : name(name, memory), cb(cb), args(memory), opt_args(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_func(const char *name, std::function<void(mgp_list *, mgp_func_context *, mgp_func_result *, mgp_memory *)> cb,
           memgraph::utils::MemoryResource *memory)
      : name(name, memory), cb(cb), args(memory), opt_args(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_func(const mgp_func &other, memgraph::utils::MemoryResource *memory)
      : name(other.name, memory), cb(other.cb), args(other.args, memory), opt_args(other.opt_args, memory) {}

  mgp_func(mgp_func &&other, memgraph::utils::MemoryResource *memory)
      : name(std::move(other.name), memory),
        cb(std::move(other.cb)),
        args(std::move(other.args), memory),
        opt_args(std::move(other.opt_args), memory) {}

  mgp_func(const mgp_func &other) = default;
  mgp_func(mgp_func &&other) = default;

  mgp_func &operator=(const mgp_func &) = delete;
  mgp_func &operator=(mgp_func &&) = delete;

  ~mgp_func() = default;

  /// Name of the function.
  memgraph::utils::pmr::string name;
  /// Entry-point for the function.
  std::function<void(mgp_list *, mgp_func_context *, mgp_func_result *, mgp_memory *)> cb;
  /// Required, positional arguments as a (name, type) pair.
  memgraph::utils::pmr::vector<std::pair<memgraph::utils::pmr::string, const memgraph::query::procedure::CypherType *>>
      args;
  /// Optional positional arguments as a (name, type, default_value) tuple.
  memgraph::utils::pmr::vector<std::tuple<memgraph::utils::pmr::string, const memgraph::query::procedure::CypherType *,
                                          memgraph::query::TypedValue>>
      opt_args;
};

mgp_error MgpTransAddFixedResult(mgp_trans *trans) noexcept;

struct mgp_module {
  using allocator_type = memgraph::utils::Allocator<mgp_module>;

  explicit mgp_module(memgraph::utils::MemoryResource *memory)
      : procedures(memory), transformations(memory), functions(memory) {}

  mgp_module(const mgp_module &other, memgraph::utils::MemoryResource *memory)
      : procedures(other.procedures, memory),
        transformations(other.transformations, memory),
        functions(other.functions, memory) {}

  mgp_module(mgp_module &&other, memgraph::utils::MemoryResource *memory)
      : procedures(std::move(other.procedures), memory),
        transformations(std::move(other.transformations), memory),
        functions(std::move(other.functions), memory) {}

  mgp_module(const mgp_module &) = default;
  mgp_module(mgp_module &&) = default;

  mgp_module &operator=(const mgp_module &) = delete;
  mgp_module &operator=(mgp_module &&) = delete;

  ~mgp_module() = default;

  memgraph::utils::pmr::map<memgraph::utils::pmr::string, mgp_proc> procedures;
  memgraph::utils::pmr::map<memgraph::utils::pmr::string, mgp_trans> transformations;
  memgraph::utils::pmr::map<memgraph::utils::pmr::string, mgp_func> functions;
};

namespace memgraph::query::procedure {

/// @throw std::bad_alloc
/// @throw std::length_error
/// @throw anything std::ostream::operator<< may throw.
void PrintProcSignature(const mgp_proc &, std::ostream *);

/// @throw std::bad_alloc
/// @throw std::length_error
/// @throw anything std::ostream::operator<< may throw.
void PrintFuncSignature(const mgp_func &, std::ostream &);

bool IsValidIdentifierName(const char *name);

}  // namespace memgraph::query::procedure

struct mgp_message {
  explicit mgp_message(const memgraph::integrations::kafka::Message &message) : msg{&message} {}
  explicit mgp_message(const memgraph::integrations::pulsar::Message &message) : msg{message} {}

  using KafkaMessage = const memgraph::integrations::kafka::Message *;
  using PulsarMessage = memgraph::integrations::pulsar::Message;
  std::variant<KafkaMessage, PulsarMessage> msg;
};

struct mgp_messages {
  using allocator_type = memgraph::utils::Allocator<mgp_messages>;
  using storage_type = memgraph::utils::pmr::vector<mgp_message>;
  explicit mgp_messages(storage_type &&storage) : messages(std::move(storage)) {}

  mgp_messages(const mgp_messages &) = delete;
  mgp_messages &operator=(const mgp_messages &) = delete;

  mgp_messages(mgp_messages &&) = delete;
  mgp_messages &operator=(mgp_messages &&) = delete;

  ~mgp_messages() = default;

  storage_type messages;
};

bool ContainsDeleted(const mgp_value *val);

memgraph::query::TypedValue ToTypedValue(const mgp_value &val, memgraph::utils::MemoryResource *memory);

struct mgp_query_execution {
  explicit mgp_query_execution(){};

  ~mgp_query_execution() = default;
};
