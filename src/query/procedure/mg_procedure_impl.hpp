/// @file
/// Contains private (implementation) declarations and definitions for
/// mg_procedure.h
#pragma once

#include "mg_procedure.h"

#include <optional>
#include <ostream>

#include "integrations/kafka/consumer.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/procedure/cypher_types.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/memory.hpp"
#include "utils/pmr/map.hpp"
#include "utils/pmr/string.hpp"
#include "utils/pmr/vector.hpp"
#include "utils/temporal.hpp"
/// Wraps memory resource used in custom procedures.
///
/// This should have been `using mgp_memory = utils::MemoryResource`, but that's
/// not valid C++ because we have a forward declare `struct mgp_memory` in
/// mg_procedure.h
/// TODO: Make this extendable in C API, so that custom procedure writer can add
/// their own memory management wrappers.
struct mgp_memory {
  utils::MemoryResource *impl;
};

/// Immutable container of various values that appear in openCypher.
struct mgp_value {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = utils::Allocator<mgp_value>;

  // Construct MGP_VALUE_TYPE_NULL.
  explicit mgp_value(utils::MemoryResource *) noexcept;

  mgp_value(bool, utils::MemoryResource *) noexcept;
  mgp_value(int64_t, utils::MemoryResource *) noexcept;
  mgp_value(double, utils::MemoryResource *) noexcept;
  /// @throw std::bad_alloc
  mgp_value(const char *, utils::MemoryResource *);
  /// Take ownership of the mgp_list, MemoryResource must match.
  mgp_value(mgp_list *, utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_map, MemoryResource must match.
  mgp_value(mgp_map *, utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_vertex, MemoryResource must match.
  mgp_value(mgp_vertex *, utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_edge, MemoryResource must match.
  mgp_value(mgp_edge *, utils::MemoryResource *) noexcept;
  /// Take ownership of the mgp_path, MemoryResource must match.
  mgp_value(mgp_path *, utils::MemoryResource *) noexcept;

  mgp_value(mgp_date *, utils::MemoryResource *) noexcept;
  mgp_value(mgp_local_time *, utils::MemoryResource *) noexcept;
  mgp_value(mgp_local_date_time *, utils::MemoryResource *) noexcept;
  mgp_value(mgp_duration *, utils::MemoryResource *) noexcept;

  /// Construct by copying query::TypedValue using utils::MemoryResource.
  /// mgp_graph is needed to construct mgp_vertex and mgp_edge.
  /// @throw std::bad_alloc
  mgp_value(const query::TypedValue &, const mgp_graph *, utils::MemoryResource *);

  /// Construct by copying storage::PropertyValue using utils::MemoryResource.
  /// @throw std::bad_alloc
  mgp_value(const storage::PropertyValue &, utils::MemoryResource *);

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_value(const mgp_value &) = delete;

  /// Copy construct using given utils::MemoryResource.
  /// @throw std::bad_alloc
  mgp_value(const mgp_value &, utils::MemoryResource *);

  /// Move construct using given utils::MemoryResource.
  /// @throw std::bad_alloc if MemoryResource is different, so we cannot move.
  mgp_value(mgp_value &&, utils::MemoryResource *);

  /// Move construct, utils::MemoryResource is inherited.
  mgp_value(mgp_value &&other) noexcept : mgp_value(other, other.memory) {}

  /// Copy-assignment is not allowed to preserve immutability.
  mgp_value &operator=(const mgp_value &) = delete;

  /// Move-assignment is not allowed to preserve immutability.
  mgp_value &operator=(mgp_value &&) = delete;

  ~mgp_value() noexcept;

  utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  mgp_value_type type;
  utils::MemoryResource *memory;

  union {
    bool bool_v;
    int64_t int_v;
    double double_v;
    utils::pmr::string string_v;
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

inline utils::DateParameters MapDateParameters(const struct mgp_date_parameters *parameters) {
  return {.years = parameters->year, .months = parameters->month, .days = parameters->day};
}

struct mgp_date {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = utils::Allocator<mgp_date>;

  // Hopefully VertexAccessor copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<utils::Date>);

  mgp_date(const utils::Date &date, utils::MemoryResource *memory) noexcept : memory(memory), date(date) {}

  mgp_date(const std::string_view string, utils::MemoryResource *memory) noexcept
      : memory(memory), date(utils::ParseDateParameters(string).first) {}

  mgp_date(const struct mgp_date_parameters *parameters, utils::MemoryResource *memory) noexcept
      : memory(memory), date(MapDateParameters(parameters)) {}

  mgp_date(const int64_t microseconds, utils::MemoryResource *memory) noexcept : memory(memory), date(microseconds) {}

  mgp_date(const mgp_date &other, utils::MemoryResource *memory) noexcept : memory(memory), date(other.date) {}

  mgp_date(mgp_date &&other, utils::MemoryResource *memory) noexcept : memory(memory), date(other.date) {}

  mgp_date(mgp_date &&other) noexcept : memory(other.memory), date(other.date) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_date(const mgp_date &) = delete;

  mgp_date &operator=(const mgp_date &) = delete;
  mgp_date &operator=(mgp_date &&) = delete;

  ~mgp_date() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  utils::MemoryResource *memory;
  utils::Date date;
};

inline utils::LocalTimeParameters MapLocalTimeParameters(const struct mgp_local_time_parameters *parameters) {
  return {.hours = parameters->hour,
          .minutes = parameters->minute,
          .seconds = parameters->second,
          .milliseconds = parameters->millisecond,
          .microseconds = parameters->microsecond};
}

struct mgp_local_time {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = utils::Allocator<mgp_local_time>;

  // Hopefully VertexAccessor copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<utils::Date>);

  mgp_local_time(const std::string_view string, utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(utils::ParseLocalTimeParameters(string).first) {}

  mgp_local_time(const struct mgp_local_time_parameters *parameters, utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(MapLocalTimeParameters(parameters)) {}

  mgp_local_time(const utils::LocalTime &local_time, utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(local_time) {}

  mgp_local_time(const int64_t microseconds, utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(microseconds) {}

  mgp_local_time(const mgp_local_time &other, utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(other.local_time) {}

  mgp_local_time(mgp_local_time &&other, utils::MemoryResource *memory) noexcept
      : memory(memory), local_time(other.local_time) {}

  mgp_local_time(mgp_local_time &&other) noexcept : memory(other.memory), local_time(other.local_time) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_local_time(const mgp_local_time &) = delete;

  mgp_local_time &operator=(const mgp_local_time &) = delete;
  mgp_local_time &operator=(mgp_local_time &&) = delete;

  ~mgp_local_time() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  utils::MemoryResource *memory;
  utils::LocalTime local_time;
};

inline utils::LocalDateTime CreateLocalDateTimeFromString(const std::string_view string) {
  const auto &[date_parameters, local_time_parameters] = utils::ParseLocalDateTimeParameters(string);
  return utils::LocalDateTime{date_parameters, local_time_parameters};
}

struct mgp_local_date_time {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = utils::Allocator<mgp_local_date_time>;

  // Hopefully VertexAccessor copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<utils::Date>);

  mgp_local_date_time(const utils::LocalDateTime &local_date_time, utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(local_date_time) {}

  mgp_local_date_time(const std::string_view string, utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(CreateLocalDateTimeFromString(string)) {}

  mgp_local_date_time(const struct mgp_local_date_time_parameters *parameters, utils::MemoryResource *memory) noexcept
      : memory(memory),
        local_date_time(MapDateParameters(parameters->date_parameters),
                        MapLocalTimeParameters(parameters->local_time_parameters)) {}

  mgp_local_date_time(const int64_t microseconds, utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(microseconds) {}

  mgp_local_date_time(const mgp_local_date_time &other, utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(other.local_date_time) {}

  mgp_local_date_time(mgp_local_date_time &&other, utils::MemoryResource *memory) noexcept
      : memory(memory), local_date_time(other.local_date_time) {}

  mgp_local_date_time(mgp_local_date_time &&other) noexcept
      : memory(other.memory), local_date_time(other.local_date_time) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_local_date_time(const mgp_local_date_time &) = delete;

  mgp_local_date_time &operator=(const mgp_local_date_time &) = delete;
  mgp_local_date_time &operator=(mgp_local_date_time &&) = delete;

  ~mgp_local_date_time() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  utils::MemoryResource *memory;
  utils::LocalDateTime local_date_time;
};

inline utils::DurationParameters MapDurationParameters(const struct mgp_duration_parameters *parameters) {
  return {.years = parameters->year,
          .months = parameters->month,
          .days = parameters->day,
          .hours = parameters->hour,
          .minutes = parameters->minute,
          .seconds = parameters->second,
          .milliseconds = parameters->millisecond,
          .microseconds = parameters->microsecond};
}
struct mgp_duration {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = utils::Allocator<mgp_duration>;

  // Hopefully VertexAccessor copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<utils::Date>);

  mgp_duration(const std::string_view string, utils::MemoryResource *memory) noexcept
      : memory(memory), duration(utils::ParseDurationParameters(string)) {}

  mgp_duration(const struct mgp_duration_parameters *parameters, utils::MemoryResource *memory) noexcept
      : memory(memory), duration(MapDurationParameters(parameters)) {}

  mgp_duration(const utils::Duration &duration, utils::MemoryResource *memory) noexcept
      : memory(memory), duration(duration) {}

  mgp_duration(const int64_t microseconds, utils::MemoryResource *memory) noexcept
      : memory(memory), duration(microseconds) {}

  mgp_duration(const mgp_duration &other, utils::MemoryResource *memory) noexcept
      : memory(memory), duration(other.duration) {}

  mgp_duration(mgp_duration &&other, utils::MemoryResource *memory) noexcept
      : memory(memory), duration(other.duration) {}

  mgp_duration(mgp_duration &&other) noexcept : memory(other.memory), duration(other.duration) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_duration(const mgp_duration &) = delete;

  mgp_duration &operator=(const mgp_duration &) = delete;
  mgp_duration &operator=(mgp_duration &&) = delete;

  ~mgp_duration() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  utils::MemoryResource *memory;
  utils::Duration duration;
};

struct mgp_list {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = utils::Allocator<mgp_list>;

  explicit mgp_list(utils::MemoryResource *memory) : elems(memory) {}

  mgp_list(utils::pmr::vector<mgp_value> &&elems, utils::MemoryResource *memory) : elems(std::move(elems), memory) {}

  mgp_list(const mgp_list &other, utils::MemoryResource *memory) : elems(other.elems, memory) {}

  mgp_list(mgp_list &&other, utils::MemoryResource *memory) : elems(std::move(other.elems), memory) {}

  mgp_list(mgp_list &&other) noexcept : elems(std::move(other.elems)) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_list(const mgp_list &) = delete;

  mgp_list &operator=(const mgp_list &) = delete;
  mgp_list &operator=(mgp_list &&) = delete;

  ~mgp_list() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return elems.get_allocator().GetMemoryResource(); }

  // C++17 vector can work with incomplete type.
  utils::pmr::vector<mgp_value> elems;
};

struct mgp_map {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = utils::Allocator<mgp_map>;

  explicit mgp_map(utils::MemoryResource *memory) : items(memory) {}

  mgp_map(utils::pmr::map<utils::pmr::string, mgp_value> &&items, utils::MemoryResource *memory)
      : items(std::move(items), memory) {}

  mgp_map(const mgp_map &other, utils::MemoryResource *memory) : items(other.items, memory) {}

  mgp_map(mgp_map &&other, utils::MemoryResource *memory) : items(std::move(other.items), memory) {}

  mgp_map(mgp_map &&other) noexcept : items(std::move(other.items)) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_map(const mgp_map &) = delete;

  mgp_map &operator=(const mgp_map &) = delete;
  mgp_map &operator=(mgp_map &&) = delete;

  ~mgp_map() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return items.get_allocator().GetMemoryResource(); }

  // Unfortunately using incomplete type with map is undefined, so mgp_map
  // needs to be defined after mgp_value.
  utils::pmr::map<utils::pmr::string, mgp_value> items;
};

struct mgp_map_item {
  const char *key;
  const mgp_value *value;
};

struct mgp_map_items_iterator {
  using allocator_type = utils::Allocator<mgp_map_items_iterator>;

  mgp_map_items_iterator(const mgp_map *map, utils::MemoryResource *memory)
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

  utils::MemoryResource *GetMemoryResource() const { return memory; }

  utils::MemoryResource *memory;
  const mgp_map *map;
  decltype(map->items.begin()) current_it;
  mgp_map_item current;
};

struct mgp_vertex {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = utils::Allocator<mgp_vertex>;

  // Hopefully VertexAccessor copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<query::VertexAccessor>);

  mgp_vertex(query::VertexAccessor v, const mgp_graph *graph, utils::MemoryResource *memory) noexcept
      : memory(memory), impl(v), graph(graph) {}

  mgp_vertex(const mgp_vertex &other, utils::MemoryResource *memory) noexcept
      : memory(memory), impl(other.impl), graph(other.graph) {}

  mgp_vertex(mgp_vertex &&other, utils::MemoryResource *memory) noexcept
      : memory(memory), impl(other.impl), graph(other.graph) {}

  mgp_vertex(mgp_vertex &&other) noexcept : memory(other.memory), impl(other.impl), graph(other.graph) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_vertex(const mgp_vertex &) = delete;

  mgp_vertex &operator=(const mgp_vertex &) = delete;
  mgp_vertex &operator=(mgp_vertex &&) = delete;

  bool operator==(const mgp_vertex &other) const noexcept { return this->impl == other.impl; }
  bool operator!=(const mgp_vertex &other) const noexcept { return !(*this == other); };

  ~mgp_vertex() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  utils::MemoryResource *memory;
  query::VertexAccessor impl;
  const mgp_graph *graph;
};

struct mgp_edge {
  /// Allocator type so that STL containers are aware that we need one.
  /// We don't actually need this, but it simplifies the C API, because we store
  /// the allocator which was used to allocate `this`.
  using allocator_type = utils::Allocator<mgp_edge>;

  // Hopefully EdgeAccessor copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_copy_constructible_v<query::EdgeAccessor>);

  mgp_edge(const query::EdgeAccessor &impl, const mgp_graph *graph, utils::MemoryResource *memory) noexcept
      : memory(memory), impl(impl), from(impl.From(), graph, memory), to(impl.To(), graph, memory) {}

  mgp_edge(const mgp_edge &other, utils::MemoryResource *memory) noexcept
      : memory(memory), impl(other.impl), from(other.from, memory), to(other.to, memory) {}

  mgp_edge(mgp_edge &&other, utils::MemoryResource *memory) noexcept
      : memory(other.memory), impl(other.impl), from(std::move(other.from), memory), to(std::move(other.to), memory) {}

  mgp_edge(mgp_edge &&other) noexcept
      : memory(other.memory), impl(other.impl), from(std::move(other.from)), to(std::move(other.to)) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_edge(const mgp_edge &) = delete;

  mgp_edge &operator=(const mgp_edge &) = delete;
  mgp_edge &operator=(mgp_edge &&) = delete;
  ~mgp_edge() = default;

  bool operator==(const mgp_edge &other) const noexcept { return this->impl == other.impl; }
  bool operator!=(const mgp_edge &other) const noexcept { return !(*this == other); };

  utils::MemoryResource *GetMemoryResource() const noexcept { return memory; }

  utils::MemoryResource *memory;
  query::EdgeAccessor impl;
  mgp_vertex from;
  mgp_vertex to;
};

struct mgp_path {
  /// Allocator type so that STL containers are aware that we need one.
  using allocator_type = utils::Allocator<mgp_path>;

  explicit mgp_path(utils::MemoryResource *memory) : vertices(memory), edges(memory) {}

  mgp_path(const mgp_path &other, utils::MemoryResource *memory)
      : vertices(other.vertices, memory), edges(other.edges, memory) {}

  mgp_path(mgp_path &&other, utils::MemoryResource *memory)
      : vertices(std::move(other.vertices), memory), edges(std::move(other.edges), memory) {}

  mgp_path(mgp_path &&other) noexcept : vertices(std::move(other.vertices)), edges(std::move(other.edges)) {}

  /// Copy construction without utils::MemoryResource is not allowed.
  mgp_path(const mgp_path &) = delete;

  mgp_path &operator=(const mgp_path &) = delete;
  mgp_path &operator=(mgp_path &&) = delete;

  ~mgp_path() = default;

  utils::MemoryResource *GetMemoryResource() const noexcept { return vertices.get_allocator().GetMemoryResource(); }

  utils::pmr::vector<mgp_vertex> vertices;
  utils::pmr::vector<mgp_edge> edges;
};

struct mgp_result_record {
  /// Result record signature as defined for mgp_proc.
  const utils::pmr::map<utils::pmr::string, std::pair<const query::procedure::CypherType *, bool>> *signature;
  utils::pmr::map<utils::pmr::string, query::TypedValue> values;
};

struct mgp_result {
  explicit mgp_result(
      const utils::pmr::map<utils::pmr::string, std::pair<const query::procedure::CypherType *, bool>> *signature,
      utils::MemoryResource *mem)
      : signature(signature), rows(mem) {}

  /// Result record signature as defined for mgp_proc.
  const utils::pmr::map<utils::pmr::string, std::pair<const query::procedure::CypherType *, bool>> *signature;
  utils::pmr::vector<mgp_result_record> rows;
  std::optional<utils::pmr::string> error_msg;
};

struct mgp_graph {
  query::DbAccessor *impl;
  storage::View view;
  // TODO: Merge `mgp_graph` and `mgp_memory` into a single `mgp_context`. The
  // `ctx` field is out of place here.
  query::ExecutionContext *ctx;
};

struct mgp_properties_iterator {
  using allocator_type = utils::Allocator<mgp_properties_iterator>;

  // Define members at the start because we use decltype a lot here, so members
  // need to be visible in method definitions.

  utils::MemoryResource *memory;
  const mgp_graph *graph;
  std::remove_reference_t<decltype(*std::declval<query::VertexAccessor>().Properties(graph->view))> pvs;
  decltype(pvs.begin()) current_it;
  std::optional<std::pair<utils::pmr::string, mgp_value>> current;
  mgp_property property{nullptr, nullptr};

  // Construct with no properties.
  explicit mgp_properties_iterator(const mgp_graph *graph, utils::MemoryResource *memory)
      : memory(memory), graph(graph), current_it(pvs.begin()) {}

  // May throw who the #$@! knows what because PropertyValueStore doesn't
  // document what it throws, and it may surely throw some piece of !@#$
  // exception because it's built on top of STL and other libraries.
  mgp_properties_iterator(const mgp_graph *graph, decltype(pvs) pvs, utils::MemoryResource *memory)
      : memory(memory), graph(graph), pvs(std::move(pvs)), current_it(this->pvs.begin()) {
    if (current_it != this->pvs.end()) {
      current.emplace(utils::pmr::string(graph->impl->PropertyToName(current_it->first), memory),
                      mgp_value(current_it->second, memory));
      property.name = current->first.c_str();
      property.value = &current->second;
    }
  }

  mgp_properties_iterator(const mgp_properties_iterator &) = delete;
  mgp_properties_iterator(mgp_properties_iterator &&) = delete;

  mgp_properties_iterator &operator=(const mgp_properties_iterator &) = delete;
  mgp_properties_iterator &operator=(mgp_properties_iterator &&) = delete;

  ~mgp_properties_iterator() = default;

  utils::MemoryResource *GetMemoryResource() const { return memory; }
};

struct mgp_edges_iterator {
  using allocator_type = utils::Allocator<mgp_edges_iterator>;

  // Hopefully mgp_vertex copy constructor remains noexcept, so that we can
  // have everything noexcept here.
  static_assert(std::is_nothrow_constructible_v<mgp_vertex, const mgp_vertex &, utils::MemoryResource *>);

  mgp_edges_iterator(const mgp_vertex &v, utils::MemoryResource *memory) noexcept
      : memory(memory), source_vertex(v, memory) {}

  mgp_edges_iterator(mgp_edges_iterator &&other) noexcept
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

  utils::MemoryResource *GetMemoryResource() const { return memory; }

  utils::MemoryResource *memory;
  mgp_vertex source_vertex;
  std::optional<std::remove_reference_t<decltype(*source_vertex.impl.InEdges(source_vertex.graph->view))>> in;
  std::optional<decltype(in->begin())> in_it;
  std::optional<std::remove_reference_t<decltype(*source_vertex.impl.OutEdges(source_vertex.graph->view))>> out;
  std::optional<decltype(out->begin())> out_it;
  std::optional<mgp_edge> current_e;
};

struct mgp_vertices_iterator {
  using allocator_type = utils::Allocator<mgp_vertices_iterator>;

  /// @throw anything VerticesIterable may throw
  mgp_vertices_iterator(const mgp_graph *graph, utils::MemoryResource *memory)
      : memory(memory), graph(graph), vertices(graph->impl->Vertices(graph->view)), current_it(vertices.begin()) {
    if (current_it != vertices.end()) {
      current_v.emplace(*current_it, graph, memory);
    }
  }

  utils::MemoryResource *GetMemoryResource() const { return memory; }

  utils::MemoryResource *memory;
  const mgp_graph *graph;
  decltype(graph->impl->Vertices(graph->view)) vertices;
  decltype(vertices.begin()) current_it;
  std::optional<mgp_vertex> current_v;
};

struct mgp_type {
  query::procedure::CypherTypePtr impl;
};

struct mgp_proc {
  using allocator_type = utils::Allocator<mgp_proc>;

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const char *name, mgp_proc_cb cb, utils::MemoryResource *memory)
      : name(name, memory), cb(cb), args(memory), opt_args(memory), results(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const char *name, std::function<void(const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *)> cb,
           utils::MemoryResource *memory)
      : name(name, memory), cb(cb), args(memory), opt_args(memory), results(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_proc(const mgp_proc &other, utils::MemoryResource *memory)
      : name(other.name, memory),
        cb(other.cb),
        args(other.args, memory),
        opt_args(other.opt_args, memory),
        results(other.results, memory) {}

  mgp_proc(mgp_proc &&other, utils::MemoryResource *memory)
      : name(std::move(other.name), memory),
        cb(std::move(other.cb)),
        args(std::move(other.args), memory),
        opt_args(std::move(other.opt_args), memory),
        results(std::move(other.results), memory) {}

  mgp_proc(const mgp_proc &other) = default;
  mgp_proc(mgp_proc &&other) = default;

  mgp_proc &operator=(const mgp_proc &) = delete;
  mgp_proc &operator=(mgp_proc &&) = delete;

  ~mgp_proc() = default;

  /// Name of the procedure.
  utils::pmr::string name;
  /// Entry-point for the procedure.
  std::function<void(const mgp_list *, const mgp_graph *, mgp_result *, mgp_memory *)> cb;
  /// Required, positional arguments as a (name, type) pair.
  utils::pmr::vector<std::pair<utils::pmr::string, const query::procedure::CypherType *>> args;
  /// Optional positional arguments as a (name, type, default_value) tuple.
  utils::pmr::vector<std::tuple<utils::pmr::string, const query::procedure::CypherType *, query::TypedValue>> opt_args;
  /// Fields this procedure returns, as a (name -> (type, is_deprecated)) map.
  utils::pmr::map<utils::pmr::string, std::pair<const query::procedure::CypherType *, bool>> results;
};

struct mgp_trans {
  using allocator_type = utils::Allocator<mgp_trans>;

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_trans(const char *name, mgp_trans_cb cb, utils::MemoryResource *memory)
      : name(name, memory), cb(cb), results(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_trans(const char *name,
            std::function<void(const mgp_messages *, const mgp_graph *, mgp_result *, mgp_memory *)> cb,
            utils::MemoryResource *memory)
      : name(name, memory), cb(cb), results(memory) {}

  /// @throw std::bad_alloc
  /// @throw std::length_error
  mgp_trans(const mgp_trans &other, utils::MemoryResource *memory)
      : name(other.name, memory), cb(other.cb), results(other.results) {}

  mgp_trans(mgp_trans &&other, utils::MemoryResource *memory)
      : name(std::move(other.name), memory), cb(std::move(other.cb)), results(std::move(other.results)) {}

  mgp_trans(const mgp_trans &other) = default;
  mgp_trans(mgp_trans &&other) = default;

  mgp_trans &operator=(const mgp_trans &) = delete;
  mgp_trans &operator=(mgp_trans &&) = delete;

  ~mgp_trans() = default;

  /// Name of the transformation.
  utils::pmr::string name;
  /// Entry-point for the transformation.
  std::function<void(const mgp_messages *, const mgp_graph *, mgp_result *, mgp_memory *)> cb;
  /// Fields this transformation returns.
  utils::pmr::map<utils::pmr::string, std::pair<const query::procedure::CypherType *, bool>> results;
};

mgp_error MgpTransAddFixedResult(mgp_trans *trans) noexcept;

struct mgp_module {
  using allocator_type = utils::Allocator<mgp_module>;

  explicit mgp_module(utils::MemoryResource *memory) : procedures(memory), transformations(memory) {}

  mgp_module(const mgp_module &other, utils::MemoryResource *memory)
      : procedures(other.procedures, memory), transformations(other.transformations, memory) {}

  mgp_module(mgp_module &&other, utils::MemoryResource *memory)
      : procedures(std::move(other.procedures), memory), transformations(std::move(other.transformations), memory) {}

  mgp_module(const mgp_module &) = default;
  mgp_module(mgp_module &&) = default;

  mgp_module &operator=(const mgp_module &) = delete;
  mgp_module &operator=(mgp_module &&) = delete;

  ~mgp_module() = default;

  utils::pmr::map<utils::pmr::string, mgp_proc> procedures;
  utils::pmr::map<utils::pmr::string, mgp_trans> transformations;
};

namespace query::procedure {

/// @throw std::bad_alloc
/// @throw std::length_error
/// @throw anything std::ostream::operator<< may throw.
void PrintProcSignature(const mgp_proc &, std::ostream *);

bool IsValidIdentifierName(const char *name);

}  // namespace query::procedure

struct mgp_message {
  const integrations::kafka::Message *msg;
};

struct mgp_messages {
  using allocator_type = utils::Allocator<mgp_messages>;
  using storage_type = utils::pmr::vector<mgp_message>;
  explicit mgp_messages(storage_type &&storage) : messages(std::move(storage)) {}

  mgp_messages(const mgp_messages &) = delete;
  mgp_messages &operator=(const mgp_messages &) = delete;

  mgp_messages(mgp_messages &&) = delete;
  mgp_messages &operator=(mgp_messages &&) = delete;

  ~mgp_messages() = default;

  storage_type messages;
};
