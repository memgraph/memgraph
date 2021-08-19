#include "query/procedure/mg_procedure_impl.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <optional>
#include <regex>
#include <type_traits>

#include "mg_procedure.h"
#include "module.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/view.hpp"
#include "utils/algorithm.hpp"
#include "utils/concepts.hpp"
#include "utils/logging.hpp"
#include "utils/math.hpp"
#include "utils/memory.hpp"
#include "utils/string.hpp"

// This file contains implementation of top level C API functions, but this is
// all actually part of query::procedure. So use that namespace for simplicity.
// NOLINTNEXTLINE(google-build-using-namespace)
using namespace query::procedure;

namespace {

void *MgpAlignedAllocImpl(utils::MemoryResource &memory, const size_t size_in_bytes, const size_t alignment) {
  if (size_in_bytes == 0U || !utils::IsPow2(alignment)) return nullptr;
  // Simplify alignment by always using values greater or equal to max_align.
  const size_t alloc_align = std::max(alignment, alignof(std::max_align_t));
  // Allocate space for header containing size & alignment info.
  const size_t header_size = sizeof(size_in_bytes) + sizeof(alloc_align);
  // We need to return the `data` pointer aligned to the requested alignment.
  // Since we request the initial memory to be aligned to `alloc_align`, we can
  // just allocate an additional multiple of `alloc_align` of bytes such that
  // the header fits. `data` will then be aligned after this multiple of bytes.
  static_assert(std::is_same_v<size_t, uint64_t>);
  const auto maybe_bytes_for_header = utils::RoundUint64ToMultiple(header_size, alloc_align);
  if (!maybe_bytes_for_header) return nullptr;
  const size_t bytes_for_header = *maybe_bytes_for_header;
  const size_t alloc_size = bytes_for_header + size_in_bytes;
  if (alloc_size < size_in_bytes) return nullptr;
  try {
    void *ptr = memory.Allocate(alloc_size, alloc_align);
    char *data = reinterpret_cast<char *>(ptr) + bytes_for_header;
    std::memcpy(data - sizeof(size_in_bytes), &size_in_bytes, sizeof(size_in_bytes));
    std::memcpy(data - sizeof(size_in_bytes) - sizeof(alloc_align), &alloc_align, sizeof(alloc_align));
    return data;
  } catch (...) {
    return nullptr;
  }
}

void MgpFreeImpl(utils::MemoryResource &memory, void *const p) {
  if (!p) return;
  char *const data = reinterpret_cast<char *>(p);
  // Read the header containing size & alignment info.
  size_t size_in_bytes;
  std::memcpy(&size_in_bytes, data - sizeof(size_in_bytes), sizeof(size_in_bytes));
  size_t alloc_align;
  std::memcpy(&alloc_align, data - sizeof(size_in_bytes) - sizeof(alloc_align), sizeof(alloc_align));
  // Reconstruct how many bytes we allocated on top of the original request.
  // We need not check allocation request overflow, since we did so already in
  // mgp_aligned_alloc.
  const size_t header_size = sizeof(size_in_bytes) + sizeof(alloc_align);
  const size_t bytes_for_header = *utils::RoundUint64ToMultiple(header_size, alloc_align);
  const size_t alloc_size = bytes_for_header + size_in_bytes;
  // Get the original ptr we allocated.
  void *const original_ptr = data - bytes_for_header;
  memory.Deallocate(original_ptr, alloc_size, alloc_align);
}

template <typename TResult>
int ResultToReturnCode(const TResult &result) {
  return result.HasValue() ? 1 : 0;
}

bool MgpGraphIsMutable(const mgp_graph &graph) { return graph.view == storage::View::NEW; }

bool MgpVertexIsMutable(const mgp_vertex &vertex) { return MgpGraphIsMutable(*vertex.graph); }

bool MgpEdgeIsMutable(const mgp_edge &edge) { return MgpVertexIsMutable(edge.from); }
}  // namespace

void *mgp_alloc(mgp_memory *memory, size_t size_in_bytes) {
  return mgp_aligned_alloc(memory, size_in_bytes, alignof(std::max_align_t));
}

void *mgp_aligned_alloc(mgp_memory *memory, const size_t size_in_bytes, const size_t alignment) {
  return MgpAlignedAllocImpl(*memory->impl, size_in_bytes, alignment);
}

void mgp_free(mgp_memory *memory, void *const p) { MgpFreeImpl(*memory->impl, p); }

void *mgp_global_alloc(size_t size_in_bytes) {
  return mgp_global_aligned_alloc(size_in_bytes, alignof(std::max_align_t));
}

void *mgp_global_aligned_alloc(size_t size_in_bytes, size_t alignment) {
  return MgpAlignedAllocImpl(gModuleRegistry.GetSharedMemoryResource(), size_in_bytes, alignment);
}

void mgp_global_free(void *const p) { MgpFreeImpl(gModuleRegistry.GetSharedMemoryResource(), p); }

namespace {

// May throw whatever the constructor of U throws. `std::bad_alloc` is handled
// by returning nullptr.
template <class U, class... TArgs>
U *new_mgp_object(utils::MemoryResource *memory, TArgs &&...args) {
  utils::Allocator<U> allocator(memory);
  try {
    return allocator.template new_object<U>(std::forward<TArgs>(args)...);
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}

template <class U, class... TArgs>
U *new_mgp_object(mgp_memory *memory, TArgs &&...args) {
  return new_mgp_object<U, TArgs...>(memory->impl, std::forward<TArgs>(args)...);
}

// Assume that deallocation and object destruction never throws. If it does,
// we are in big trouble.
template <class T>
void delete_mgp_object(T *ptr) noexcept {
  if (!ptr) return;
  utils::Allocator<T> allocator(ptr->GetMemoryResource());
  allocator.delete_object(ptr);
}

mgp_value_type FromTypedValueType(query::TypedValue::Type type) {
  switch (type) {
    case query::TypedValue::Type::Null:
      return MGP_VALUE_TYPE_NULL;
    case query::TypedValue::Type::Bool:
      return MGP_VALUE_TYPE_BOOL;
    case query::TypedValue::Type::Int:
      return MGP_VALUE_TYPE_INT;
    case query::TypedValue::Type::Double:
      return MGP_VALUE_TYPE_DOUBLE;
    case query::TypedValue::Type::String:
      return MGP_VALUE_TYPE_STRING;
    case query::TypedValue::Type::List:
      return MGP_VALUE_TYPE_LIST;
    case query::TypedValue::Type::Map:
      return MGP_VALUE_TYPE_MAP;
    case query::TypedValue::Type::Vertex:
      return MGP_VALUE_TYPE_VERTEX;
    case query::TypedValue::Type::Edge:
      return MGP_VALUE_TYPE_EDGE;
    case query::TypedValue::Type::Path:
      return MGP_VALUE_TYPE_PATH;
  }
}

query::TypedValue ToTypedValue(const mgp_value &val, utils::MemoryResource *memory) {
  switch (mgp_value_get_type(&val)) {
    case MGP_VALUE_TYPE_NULL:
      return query::TypedValue(memory);
    case MGP_VALUE_TYPE_BOOL:
      return query::TypedValue(static_cast<bool>(mgp_value_get_bool(&val)), memory);
    case MGP_VALUE_TYPE_INT:
      return query::TypedValue(mgp_value_get_int(&val), memory);
    case MGP_VALUE_TYPE_DOUBLE:
      return query::TypedValue(mgp_value_get_double(&val), memory);
    case MGP_VALUE_TYPE_STRING:
      return query::TypedValue(mgp_value_get_string(&val), memory);
    case MGP_VALUE_TYPE_LIST: {
      const auto *list = mgp_value_get_list(&val);
      query::TypedValue::TVector tv_list(memory);
      tv_list.reserve(list->elems.size());
      for (const auto &elem : list->elems) {
        tv_list.emplace_back(ToTypedValue(elem, memory));
      }
      return query::TypedValue(std::move(tv_list));
    }
    case MGP_VALUE_TYPE_MAP: {
      const auto *map = mgp_value_get_map(&val);
      query::TypedValue::TMap tv_map(memory);
      for (const auto &item : map->items) {
        tv_map.emplace(item.first, ToTypedValue(item.second, memory));
      }
      return query::TypedValue(std::move(tv_map));
    }
    case MGP_VALUE_TYPE_VERTEX:
      return query::TypedValue(mgp_value_get_vertex(&val)->impl, memory);
    case MGP_VALUE_TYPE_EDGE:
      return query::TypedValue(mgp_value_get_edge(&val)->impl, memory);
    case MGP_VALUE_TYPE_PATH: {
      const auto *path = mgp_value_get_path(&val);
      MG_ASSERT(!path->vertices.empty());
      MG_ASSERT(path->vertices.size() == path->edges.size() + 1);
      query::Path tv_path(path->vertices[0].impl, memory);
      for (size_t i = 0; i < path->edges.size(); ++i) {
        tv_path.Expand(path->edges[i].impl);
        tv_path.Expand(path->vertices[i + 1].impl);
      }
      return query::TypedValue(std::move(tv_path));
    }
  }
}

}  // namespace

mgp_value::mgp_value(utils::MemoryResource *m) noexcept : type(MGP_VALUE_TYPE_NULL), memory(m) {}

mgp_value::mgp_value(bool val, utils::MemoryResource *m) noexcept : type(MGP_VALUE_TYPE_BOOL), memory(m), bool_v(val) {}

mgp_value::mgp_value(int64_t val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_INT), memory(m), int_v(val) {}

mgp_value::mgp_value(double val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_DOUBLE), memory(m), double_v(val) {}

mgp_value::mgp_value(const char *val, utils::MemoryResource *m)
    : type(MGP_VALUE_TYPE_STRING), memory(m), string_v(val, m) {}

mgp_value::mgp_value(mgp_list *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_LIST), memory(m), list_v(val) {
  MG_ASSERT(val->GetMemoryResource() == m, "Unable to take ownership of a pointer with different allocator.");
}

mgp_value::mgp_value(mgp_map *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_MAP), memory(m), map_v(val) {
  MG_ASSERT(val->GetMemoryResource() == m, "Unable to take ownership of a pointer with different allocator.");
}

mgp_value::mgp_value(mgp_vertex *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_VERTEX), memory(m), vertex_v(val) {
  MG_ASSERT(val->GetMemoryResource() == m, "Unable to take ownership of a pointer with different allocator.");
}

mgp_value::mgp_value(mgp_edge *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_EDGE), memory(m), edge_v(val) {
  MG_ASSERT(val->GetMemoryResource() == m, "Unable to take ownership of a pointer with different allocator.");
}

mgp_value::mgp_value(mgp_path *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_PATH), memory(m), path_v(val) {
  MG_ASSERT(val->GetMemoryResource() == m, "Unable to take ownership of a pointer with different allocator.");
}

mgp_value::mgp_value(const query::TypedValue &tv, const mgp_graph *graph, utils::MemoryResource *m)
    : type(FromTypedValueType(tv.type())), memory(m) {
  switch (type) {
    case MGP_VALUE_TYPE_NULL:
      break;
    case MGP_VALUE_TYPE_BOOL:
      bool_v = tv.ValueBool();
      break;
    case MGP_VALUE_TYPE_INT:
      int_v = tv.ValueInt();
      break;
    case MGP_VALUE_TYPE_DOUBLE:
      double_v = tv.ValueDouble();
      break;
    case MGP_VALUE_TYPE_STRING:
      new (&string_v) utils::pmr::string(tv.ValueString(), m);
      break;
    case MGP_VALUE_TYPE_LIST: {
      // Fill the stack allocated container and then construct the actual member
      // value. This handles the case when filling the container throws
      // something and our destructor doesn't get called so member value isn't
      // released.
      utils::pmr::vector<mgp_value> elems(m);
      elems.reserve(tv.ValueList().size());
      for (const auto &elem : tv.ValueList()) {
        elems.emplace_back(elem, graph);
      }
      utils::Allocator<mgp_list> allocator(m);
      list_v = allocator.new_object<mgp_list>(std::move(elems));
      break;
    }
    case MGP_VALUE_TYPE_MAP: {
      // Fill the stack allocated container and then construct the actual member
      // value. This handles the case when filling the container throws
      // something and our destructor doesn't get called so member value isn't
      // released.
      utils::pmr::map<utils::pmr::string, mgp_value> items(m);
      for (const auto &item : tv.ValueMap()) {
        items.emplace(item.first, mgp_value(item.second, graph, m));
      }
      utils::Allocator<mgp_map> allocator(m);
      map_v = allocator.new_object<mgp_map>(std::move(items));
      break;
    }
    case MGP_VALUE_TYPE_VERTEX: {
      utils::Allocator<mgp_vertex> allocator(m);
      vertex_v = allocator.new_object<mgp_vertex>(tv.ValueVertex(), graph);
      break;
    }
    case MGP_VALUE_TYPE_EDGE: {
      utils::Allocator<mgp_edge> allocator(m);
      edge_v = allocator.new_object<mgp_edge>(tv.ValueEdge(), graph);
      break;
    }
    case MGP_VALUE_TYPE_PATH: {
      // Fill the stack allocated container and then construct the actual member
      // value. This handles the case when filling the container throws
      // something and our destructor doesn't get called so member value isn't
      // released.
      mgp_path tmp_path(m);
      tmp_path.vertices.reserve(tv.ValuePath().vertices().size());
      for (const auto &v : tv.ValuePath().vertices()) {
        tmp_path.vertices.emplace_back(v, graph);
      }
      tmp_path.edges.reserve(tv.ValuePath().edges().size());
      for (const auto &e : tv.ValuePath().edges()) {
        tmp_path.edges.emplace_back(e, graph);
      }
      utils::Allocator<mgp_path> allocator(m);
      path_v = allocator.new_object<mgp_path>(std::move(tmp_path));
      break;
    }
  }
}

mgp_value::mgp_value(const storage::PropertyValue &pv, utils::MemoryResource *m) : memory(m) {
  switch (pv.type()) {
    case storage::PropertyValue::Type::Null:
      type = MGP_VALUE_TYPE_NULL;
      break;
    case storage::PropertyValue::Type::Bool:
      type = MGP_VALUE_TYPE_BOOL;
      bool_v = pv.ValueBool();
      break;
    case storage::PropertyValue::Type::Int:
      type = MGP_VALUE_TYPE_INT;
      int_v = pv.ValueInt();
      break;
    case storage::PropertyValue::Type::Double:
      type = MGP_VALUE_TYPE_DOUBLE;
      double_v = pv.ValueDouble();
      break;
    case storage::PropertyValue::Type::String:
      type = MGP_VALUE_TYPE_STRING;
      new (&string_v) utils::pmr::string(pv.ValueString(), m);
      break;
    case storage::PropertyValue::Type::List: {
      // Fill the stack allocated container and then construct the actual member
      // value. This handles the case when filling the container throws
      // something and our destructor doesn't get called so member value isn't
      // released.
      type = MGP_VALUE_TYPE_LIST;
      utils::pmr::vector<mgp_value> elems(m);
      elems.reserve(pv.ValueList().size());
      for (const auto &elem : pv.ValueList()) {
        elems.emplace_back(elem);
      }
      utils::Allocator<mgp_list> allocator(m);
      list_v = allocator.new_object<mgp_list>(std::move(elems));
      break;
    }
    case storage::PropertyValue::Type::Map: {
      // Fill the stack allocated container and then construct the actual member
      // value. This handles the case when filling the container throws
      // something and our destructor doesn't get called so member value isn't
      // released.
      type = MGP_VALUE_TYPE_MAP;
      utils::pmr::map<utils::pmr::string, mgp_value> items(m);
      for (const auto &item : pv.ValueMap()) {
        items.emplace(item.first, item.second);
      }
      utils::Allocator<mgp_map> allocator(m);
      map_v = allocator.new_object<mgp_map>(std::move(items));
      break;
    }
  }
}

mgp_value::mgp_value(const mgp_value &other, utils::MemoryResource *m) : type(other.type), memory(m) {
  switch (other.type) {
    case MGP_VALUE_TYPE_NULL:
      break;
    case MGP_VALUE_TYPE_BOOL:
      bool_v = other.bool_v;
      break;
    case MGP_VALUE_TYPE_INT:
      int_v = other.int_v;
      break;
    case MGP_VALUE_TYPE_DOUBLE:
      double_v = other.double_v;
      break;
    case MGP_VALUE_TYPE_STRING:
      new (&string_v) utils::pmr::string(other.string_v, m);
      break;
    case MGP_VALUE_TYPE_LIST: {
      utils::Allocator<mgp_list> allocator(m);
      list_v = allocator.new_object<mgp_list>(*other.list_v);
      break;
    }
    case MGP_VALUE_TYPE_MAP: {
      utils::Allocator<mgp_map> allocator(m);
      map_v = allocator.new_object<mgp_map>(*other.map_v);
      break;
    }
    case MGP_VALUE_TYPE_VERTEX: {
      utils::Allocator<mgp_vertex> allocator(m);
      vertex_v = allocator.new_object<mgp_vertex>(*other.vertex_v);
      break;
    }
    case MGP_VALUE_TYPE_EDGE: {
      utils::Allocator<mgp_edge> allocator(m);
      edge_v = allocator.new_object<mgp_edge>(*other.edge_v);
      break;
    }
    case MGP_VALUE_TYPE_PATH: {
      utils::Allocator<mgp_path> allocator(m);
      path_v = allocator.new_object<mgp_path>(*other.path_v);
      break;
    }
  }
}

namespace {

void DeleteValueMember(mgp_value *value) noexcept {
  MG_ASSERT(value);
  utils::Allocator<mgp_value> allocator(value->GetMemoryResource());
  switch (mgp_value_get_type(value)) {
    case MGP_VALUE_TYPE_NULL:
    case MGP_VALUE_TYPE_BOOL:
    case MGP_VALUE_TYPE_INT:
    case MGP_VALUE_TYPE_DOUBLE:
      return;
    case MGP_VALUE_TYPE_STRING:
      using TString = utils::pmr::string;
      value->string_v.~TString();
      return;
    case MGP_VALUE_TYPE_LIST:
      allocator.delete_object(value->list_v);
      return;
    case MGP_VALUE_TYPE_MAP:
      allocator.delete_object(value->map_v);
      return;
    case MGP_VALUE_TYPE_VERTEX:
      allocator.delete_object(value->vertex_v);
      return;
    case MGP_VALUE_TYPE_EDGE:
      allocator.delete_object(value->edge_v);
      return;
    case MGP_VALUE_TYPE_PATH:
      allocator.delete_object(value->path_v);
      return;
  }
}

}  // namespace

mgp_value::mgp_value(mgp_value &&other, utils::MemoryResource *m) : type(other.type), memory(m) {
  switch (other.type) {
    case MGP_VALUE_TYPE_NULL:
      break;
    case MGP_VALUE_TYPE_BOOL:
      bool_v = other.bool_v;
      break;
    case MGP_VALUE_TYPE_INT:
      int_v = other.int_v;
      break;
    case MGP_VALUE_TYPE_DOUBLE:
      double_v = other.double_v;
      break;
    case MGP_VALUE_TYPE_STRING:
      new (&string_v) utils::pmr::string(std::move(other.string_v), m);
      break;
    case MGP_VALUE_TYPE_LIST:
      static_assert(std::is_pointer_v<decltype(list_v)>, "Expected to move list_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        list_v = other.list_v;
        other.type = MGP_VALUE_TYPE_NULL;
      } else {
        utils::Allocator<mgp_list> allocator(m);
        list_v = allocator.new_object<mgp_list>(std::move(*other.list_v));
      }
      break;
    case MGP_VALUE_TYPE_MAP:
      static_assert(std::is_pointer_v<decltype(map_v)>, "Expected to move map_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        map_v = other.map_v;
        other.type = MGP_VALUE_TYPE_NULL;
      } else {
        utils::Allocator<mgp_map> allocator(m);
        map_v = allocator.new_object<mgp_map>(std::move(*other.map_v));
      }
      break;
    case MGP_VALUE_TYPE_VERTEX:
      static_assert(std::is_pointer_v<decltype(vertex_v)>, "Expected to move vertex_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        vertex_v = other.vertex_v;
        other.type = MGP_VALUE_TYPE_NULL;
      } else {
        utils::Allocator<mgp_vertex> allocator(m);
        vertex_v = allocator.new_object<mgp_vertex>(std::move(*other.vertex_v));
      }
      break;
    case MGP_VALUE_TYPE_EDGE:
      static_assert(std::is_pointer_v<decltype(edge_v)>, "Expected to move edge_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        edge_v = other.edge_v;
        other.type = MGP_VALUE_TYPE_NULL;
      } else {
        utils::Allocator<mgp_edge> allocator(m);
        edge_v = allocator.new_object<mgp_edge>(std::move(*other.edge_v));
      }
      break;
    case MGP_VALUE_TYPE_PATH:
      static_assert(std::is_pointer_v<decltype(path_v)>, "Expected to move path_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        path_v = other.path_v;
        other.type = MGP_VALUE_TYPE_NULL;
      } else {
        utils::Allocator<mgp_path> allocator(m);
        path_v = allocator.new_object<mgp_path>(std::move(*other.path_v));
      }
      break;
  }
  DeleteValueMember(&other);
  other.type = MGP_VALUE_TYPE_NULL;
}

mgp_value::~mgp_value() noexcept { DeleteValueMember(this); }

void mgp_value_destroy(mgp_value *val) { delete_mgp_object(val); }

mgp_value *mgp_value_make_null(mgp_memory *memory) { return new_mgp_object<mgp_value>(memory); }

mgp_value *mgp_value_make_bool(int val, mgp_memory *memory) { return new_mgp_object<mgp_value>(memory, val != 0); }

mgp_value *mgp_value_make_int(int64_t val, mgp_memory *memory) { return new_mgp_object<mgp_value>(memory, val); }

mgp_value *mgp_value_make_double(double val, mgp_memory *memory) { return new_mgp_object<mgp_value>(memory, val); }

mgp_value *mgp_value_make_string(const char *val, mgp_memory *memory) {
  try {
    // This may throw something from std::string constructor, it could be
    // std::length_error, but it's not really well defined, so catch all.
    return new_mgp_object<mgp_value>(memory, val);
  } catch (...) {
    return nullptr;
  }
}

mgp_value *mgp_value_make_list(mgp_list *val) { return new_mgp_object<mgp_value>(val->GetMemoryResource(), val); }

mgp_value *mgp_value_make_map(mgp_map *val) { return new_mgp_object<mgp_value>(val->GetMemoryResource(), val); }

mgp_value *mgp_value_make_vertex(mgp_vertex *val) { return new_mgp_object<mgp_value>(val->GetMemoryResource(), val); }

mgp_value *mgp_value_make_edge(mgp_edge *val) { return new_mgp_object<mgp_value>(val->GetMemoryResource(), val); }

mgp_value *mgp_value_make_path(mgp_path *val) { return new_mgp_object<mgp_value>(val->GetMemoryResource(), val); }

mgp_value_type mgp_value_get_type(const mgp_value *val) { return val->type; }

int mgp_value_is_null(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_NULL; }

int mgp_value_is_bool(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_BOOL; }

int mgp_value_is_int(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_INT; }

int mgp_value_is_double(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_DOUBLE; }

int mgp_value_is_string(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_STRING; }

int mgp_value_is_list(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_LIST; }

int mgp_value_is_map(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_MAP; }

int mgp_value_is_vertex(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_VERTEX; }

int mgp_value_is_edge(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_EDGE; }

int mgp_value_is_path(const mgp_value *val) { return mgp_value_get_type(val) == MGP_VALUE_TYPE_PATH; }

int mgp_value_get_bool(const mgp_value *val) { return val->bool_v ? 1 : 0; }

int64_t mgp_value_get_int(const mgp_value *val) { return val->int_v; }

double mgp_value_get_double(const mgp_value *val) { return val->double_v; }

const char *mgp_value_get_string(const mgp_value *val) { return val->string_v.c_str(); }

const mgp_list *mgp_value_get_list(const mgp_value *val) { return val->list_v; }

const mgp_map *mgp_value_get_map(const mgp_value *val) { return val->map_v; }

const mgp_vertex *mgp_value_get_vertex(const mgp_value *val) { return val->vertex_v; }

const mgp_edge *mgp_value_get_edge(const mgp_value *val) { return val->edge_v; }

const mgp_path *mgp_value_get_path(const mgp_value *val) { return val->path_v; }

mgp_list *mgp_list_make_empty(size_t capacity, mgp_memory *memory) {
  auto *list = new_mgp_object<mgp_list>(memory);
  if (!list) return nullptr;
  try {
    // May throw std::bad_alloc or std::length_error.
    list->elems.reserve(capacity);
  } catch (...) {
    mgp_list_destroy(list);
    return nullptr;
  }
  return list;
}

void mgp_list_destroy(mgp_list *list) { delete_mgp_object(list); }

int mgp_list_append(mgp_list *list, const mgp_value *val) {
  if (mgp_list_size(list) >= mgp_list_capacity(list)) return 0;
  return mgp_list_append_extend(list, val);
}

int mgp_list_append_extend(mgp_list *list, const mgp_value *val) {
  try {
    // May throw std::bad_alloc or std::length_error.
    list->elems.push_back(*val);
  } catch (...) {
    return 0;
  }
  return 1;
}

size_t mgp_list_size(const mgp_list *list) { return list->elems.size(); }

size_t mgp_list_capacity(const mgp_list *list) { return list->elems.capacity(); }

const mgp_value *mgp_list_at(const mgp_list *list, size_t i) {
  if (i >= mgp_list_size(list)) return nullptr;
  return &list->elems[i];
}

mgp_map *mgp_map_make_empty(mgp_memory *memory) { return new_mgp_object<mgp_map>(memory); }

void mgp_map_destroy(mgp_map *map) { delete_mgp_object(map); }

int mgp_map_insert(mgp_map *map, const char *key, const mgp_value *value) {
  try {
    // Unfortunately, cppreference.com does not say what exceptions are thrown,
    // so catch all of them. It's probably `std::bad_alloc` and
    // `std::length_error`.
    map->items.emplace(key, *value);
    return 1;
  } catch (...) {
    return 0;
  }
}

size_t mgp_map_size(const mgp_map *map) { return map->items.size(); }

const mgp_value *mgp_map_at(const mgp_map *map, const char *key) {
  auto found_it = map->items.find(key);
  if (found_it == map->items.end()) return nullptr;
  return &found_it->second;
}

const char *mgp_map_item_key(const mgp_map_item *item) { return item->key; }

const mgp_value *mgp_map_item_value(const mgp_map_item *item) { return item->value; }

mgp_map_items_iterator *mgp_map_iter_items(const mgp_map *map, mgp_memory *memory) {
  return new_mgp_object<mgp_map_items_iterator>(memory, map);
}

void mgp_map_items_iterator_destroy(mgp_map_items_iterator *it) { delete_mgp_object(it); }

const mgp_map_item *mgp_map_items_iterator_get(const mgp_map_items_iterator *it) {
  if (it->current_it == it->map->items.end()) return nullptr;
  return &it->current;
}

const mgp_map_item *mgp_map_items_iterator_next(mgp_map_items_iterator *it) {
  if (it->current_it == it->map->items.end()) return nullptr;
  if (++it->current_it == it->map->items.end()) return nullptr;
  it->current.key = it->current_it->first.c_str();
  it->current.value = &it->current_it->second;
  return &it->current;
}

mgp_path *mgp_path_make_with_start(const mgp_vertex *vertex, mgp_memory *memory) {
  auto *path = new_mgp_object<mgp_path>(memory);
  if (!path) return nullptr;
  try {
    path->vertices.push_back(*vertex);
  } catch (...) {
    delete_mgp_object(path);
    return nullptr;
  }
  return path;
}

mgp_path *mgp_path_copy(const mgp_path *path, mgp_memory *memory) {
  MG_ASSERT(mgp_path_size(path) == path->vertices.size() - 1, "Invalid mgp_path");
  return new_mgp_object<mgp_path>(memory, *path);
}

void mgp_path_destroy(mgp_path *path) { delete_mgp_object(path); }

int mgp_path_expand(mgp_path *path, const mgp_edge *edge) {
  MG_ASSERT(mgp_path_size(path) == path->vertices.size() - 1, "Invalid mgp_path");
  // Check that the both the last vertex on path and dst_vertex are endpoints of
  // the given edge.
  const auto *src_vertex = &path->vertices.back();
  const mgp_vertex *dst_vertex = nullptr;
  if (mgp_vertex_equal(mgp_edge_get_to(edge), src_vertex)) {
    dst_vertex = mgp_edge_get_from(edge);
  } else if (mgp_vertex_equal(mgp_edge_get_from(edge), src_vertex)) {
    dst_vertex = mgp_edge_get_to(edge);
  } else {
    // edge is not a continuation on src_vertex
    return 0;
  }
  // Try appending edge and dst_vertex to path, preserving the original mgp_path
  // instance if anything fails.
  try {
    path->edges.push_back(*edge);
  } catch (...) {
    MG_ASSERT(mgp_path_size(path) == path->vertices.size() - 1);
    return 0;
  }
  try {
    path->vertices.push_back(*dst_vertex);
  } catch (...) {
    path->edges.pop_back();
    MG_ASSERT(mgp_path_size(path) == path->vertices.size() - 1);
    return 0;
  }
  MG_ASSERT(mgp_path_size(path) == path->vertices.size() - 1);
  return 1;
}

size_t mgp_path_size(const mgp_path *path) { return path->edges.size(); }

const mgp_vertex *mgp_path_vertex_at(const mgp_path *path, size_t i) {
  MG_ASSERT(mgp_path_size(path) == path->vertices.size() - 1);
  if (i > mgp_path_size(path)) return nullptr;
  return &path->vertices[i];
}

const mgp_edge *mgp_path_edge_at(const mgp_path *path, size_t i) {
  MG_ASSERT(mgp_path_size(path) == path->vertices.size() - 1);
  if (i >= mgp_path_size(path)) return nullptr;
  return &path->edges[i];
}

int mgp_path_equal(const struct mgp_path *p1, const struct mgp_path *p2) {
  MG_ASSERT(mgp_path_size(p1) == p1->vertices.size() - 1);
  MG_ASSERT(mgp_path_size(p2) == p2->vertices.size() - 1);
  if (mgp_path_size(p1) != mgp_path_size(p2)) return 0;
  const auto *start1 = mgp_path_vertex_at(p1, 0);
  const auto *start2 = mgp_path_vertex_at(p2, 0);
  if (!mgp_vertex_equal(start1, start2)) return 0;
  for (size_t i = 0; i < mgp_path_size(p1); ++i) {
    const auto *e1 = mgp_path_edge_at(p1, i);
    const auto *e2 = mgp_path_edge_at(p2, i);
    if (!mgp_edge_equal(e1, e2)) return 0;
  }
  return 1;
}

/// Plugin Result

int mgp_result_set_error_msg(mgp_result *res, const char *msg) {
  auto *memory = res->rows.get_allocator().GetMemoryResource();
  try {
    res->error_msg.emplace(msg, memory);
  } catch (...) {
    return 0;
  }
  return 1;
}

mgp_result_record *mgp_result_new_record(mgp_result *res) {
  auto *memory = res->rows.get_allocator().GetMemoryResource();
  MG_ASSERT(res->signature, "Expected to have a valid signature");
  try {
    res->rows.push_back(
        mgp_result_record{res->signature, utils::pmr::map<utils::pmr::string, query::TypedValue>(memory)});
  } catch (...) {
    return nullptr;
  }
  return &res->rows.back();
}

int mgp_result_record_insert(mgp_result_record *record, const char *field_name, const mgp_value *val) {
  auto *memory = record->values.get_allocator().GetMemoryResource();
  // Validate field_name & val satisfy the procedure's result signature.
  MG_ASSERT(record->signature, "Expected to have a valid signature");
  auto find_it = record->signature->find(field_name);
  if (find_it == record->signature->end()) return 0;
  const auto *type = find_it->second.first;
  if (!type->SatisfiesType(*val)) return 0;
  try {
    record->values.emplace(field_name, ToTypedValue(*val, memory));
  } catch (...) {
    return 0;
  }
  return 1;
}

/// Graph Constructs

void mgp_properties_iterator_destroy(mgp_properties_iterator *it) { delete_mgp_object(it); }

const mgp_property *mgp_properties_iterator_get(const mgp_properties_iterator *it) {
  if (it->current) return &it->property;
  return nullptr;
}

const mgp_property *mgp_properties_iterator_next(mgp_properties_iterator *it) {
  // Incrementing the iterator either for on-disk or in-memory
  // storage, so perhaps the underlying thing can throw.
  // Both copying TypedValue and/or string from PropertyName may fail to
  // allocate. Also, dereferencing `it->current_it` could also throw, so
  // either way return nullptr and leave `it` in undefined state.
  // Hopefully iterator comparison doesn't throw, but wrap the whole thing in
  // try ... catch just to be sure.
  try {
    if (it->current_it == it->pvs.end()) {
      MG_ASSERT(!it->current,
                "Iteration is already done, so it->current should "
                "have been set to std::nullopt");
      return nullptr;
    }
    if (++it->current_it == it->pvs.end()) {
      it->current = std::nullopt;
      return nullptr;
    }
    it->current.emplace(
        utils::pmr::string(it->graph->impl->PropertyToName(it->current_it->first), it->GetMemoryResource()),
        mgp_value(it->current_it->second, it->GetMemoryResource()));
    it->property.name = it->current->first.c_str();
    it->property.value = &it->current->second;
    return &it->property;
  } catch (...) {
    it->current = std::nullopt;
    return nullptr;
  }
}

mgp_vertex_id mgp_vertex_get_id(const mgp_vertex *v) { return mgp_vertex_id{.as_int = v->impl.Gid().AsInt()}; }

int mgp_vertex_underlying_graph_is_mutable(const mgp_vertex *v) { return mgp_graph_is_mutable(v->graph); }

namespace {
std::optional<storage::PropertyValue> ToPropertyValue(const mgp_value &value);

std::optional<storage::PropertyValue> ToPropertyValue(const mgp_list &list) {
  storage::PropertyValue result{std::vector<storage::PropertyValue>{}};
  auto &result_list = result.ValueList();
  for (const auto &value : list.elems) {
    auto maybe_property_value = ToPropertyValue(value);
    if (!maybe_property_value.has_value()) {
      return std::nullopt;
    }
    result_list.push_back(std::move(maybe_property_value).value());
  }
  return {std::move(result)};
}

std::optional<storage::PropertyValue> ToPropertyValue(const mgp_map &map) {
  storage::PropertyValue result{std::map<std::string, storage::PropertyValue>{}};
  auto &result_map = result.ValueMap();
  for (const auto &[key, value] : map.items) {
    auto maybe_property_value = ToPropertyValue(value);
    if (!maybe_property_value.has_value()) {
      return std::nullopt;
    }
    result_map.insert_or_assign(std::string{key}, std::move(maybe_property_value).value());
  }
  return {std::move(result)};
}

std::optional<storage::PropertyValue> ToPropertyValue(const mgp_value &value) {
  switch (value.type) {
    case MGP_VALUE_TYPE_NULL:
      return storage::PropertyValue{};
    case MGP_VALUE_TYPE_BOOL:
      return storage::PropertyValue{value.bool_v};
    case MGP_VALUE_TYPE_INT:
      return storage::PropertyValue{value.int_v};
    case MGP_VALUE_TYPE_DOUBLE:
      return storage::PropertyValue{value.double_v};
    case MGP_VALUE_TYPE_STRING:
      return storage::PropertyValue{std::string{value.string_v}};
    case MGP_VALUE_TYPE_LIST:
      return ToPropertyValue(*value.list_v);
    case MGP_VALUE_TYPE_MAP:
      return ToPropertyValue(*value.map_v);
    case MGP_VALUE_TYPE_VERTEX:
    case MGP_VALUE_TYPE_EDGE:
    case MGP_VALUE_TYPE_PATH:
      return std::nullopt;
  }
}
}  // namespace

// TODO(antaljanosbenjamin) Wrap the function bodies to protect against OOM exceptions
int mgp_vertex_set_property(struct mgp_vertex *v, const char *property_name, const struct mgp_value *property_value) {
  if (!MgpVertexIsMutable(*v)) {
    return 0;
  }
  if (auto maybe_prop_value = ToPropertyValue(*property_value); maybe_prop_value.has_value()) {
    return ResultToReturnCode(
        v->impl.SetProperty(v->graph->impl->NameToProperty(property_name), std::move(maybe_prop_value).value()));
  }
  return 0;
}

int mgp_vertex_add_label(struct mgp_vertex *v, struct mgp_label label) {
  if (!MgpVertexIsMutable(*v)) {
    return 0;
  }
  return ResultToReturnCode(v->impl.AddLabel(v->graph->impl->NameToLabel(label.name)));
}

int mgp_vertex_remove_label(struct mgp_vertex *v, struct mgp_label label) {
  if (!MgpVertexIsMutable(*v)) {
    return 0;
  }
  return ResultToReturnCode(v->impl.RemoveLabel(v->graph->impl->NameToLabel(label.name)));
}

mgp_vertex *mgp_vertex_copy(const mgp_vertex *v, mgp_memory *memory) { return new_mgp_object<mgp_vertex>(memory, *v); }

void mgp_vertex_destroy(mgp_vertex *v) { delete_mgp_object(v); }

int mgp_vertex_equal(const mgp_vertex *a, const mgp_vertex *b) { return a->impl == b->impl ? 1 : 0; }

size_t mgp_vertex_labels_count(const mgp_vertex *v) {
  auto maybe_labels = v->impl.Labels(v->graph->view);
  if (maybe_labels.HasError()) {
    switch (maybe_labels.GetError()) {
      case storage::Error::DELETED_OBJECT:
      case storage::Error::NONEXISTENT_OBJECT:
        // Treat deleted/nonexistent vertex as having no labels.
        return 0;
      case storage::Error::PROPERTIES_DISABLED:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::SERIALIZATION_ERROR:
        spdlog::error("Unexpected error when getting vertex labels.");
        return 0;
    }
  }
  return maybe_labels->size();
}

mgp_label mgp_vertex_label_at(const mgp_vertex *v, size_t i) {
  // TODO: Maybe it's worth caching this in mgp_vertex.
  auto maybe_labels = v->impl.Labels(v->graph->view);
  if (maybe_labels.HasError()) {
    switch (maybe_labels.GetError()) {
      case storage::Error::DELETED_OBJECT:
      case storage::Error::NONEXISTENT_OBJECT:
        return mgp_label{nullptr};
      case storage::Error::PROPERTIES_DISABLED:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::SERIALIZATION_ERROR:
        spdlog::error("Unexpected error when getting vertex labels.");
        return mgp_label{nullptr};
    }
  }
  if (i >= maybe_labels->size()) return mgp_label{nullptr};
  const auto &label = (*maybe_labels)[i];
  static_assert(std::is_lvalue_reference_v<decltype(v->graph->impl->LabelToName(label))>,
                "Expected LabelToName to return a pointer or reference, so we "
                "don't have to take a copy and manage memory.");
  const auto &name = v->graph->impl->LabelToName(label);
  return mgp_label{name.c_str()};
}

int mgp_vertex_has_label_named(const mgp_vertex *v, const char *name) {
  storage::LabelId label;
  try {
    // This will allocate a std::string from `name`, which may throw
    // std::bad_alloc or std::length_error. This could be avoided with a
    // std::string_view. Although storage API may be updated with
    // std::string_view, NameToLabel itself may still throw std::bad_alloc when
    // creating a new LabelId mapping and we need to handle that.
    label = v->graph->impl->NameToLabel(name);
  } catch (...) {
    spdlog::error("Unable to allocate a LabelId mapping");
    // If we need to allocate a new mapping, then the vertex does not have such
    // a label, so return 0.
    return 0;
  }
  auto maybe_has_label = v->impl.HasLabel(v->graph->view, label);
  if (maybe_has_label.HasError()) {
    switch (maybe_has_label.GetError()) {
      case storage::Error::DELETED_OBJECT:
      case storage::Error::NONEXISTENT_OBJECT:
        return 0;
      case storage::Error::PROPERTIES_DISABLED:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::SERIALIZATION_ERROR:
        spdlog::error("Unexpected error when checking vertex has label.");
        return 0;
    }
  }
  return *maybe_has_label;
}

int mgp_vertex_has_label(const mgp_vertex *v, mgp_label label) { return mgp_vertex_has_label_named(v, label.name); }

mgp_value *mgp_vertex_get_property(const mgp_vertex *v, const char *name, mgp_memory *memory) {
  try {
    const auto key = v->graph->impl->NameToProperty(name);
    auto maybe_prop = v->impl.GetProperty(v->graph->view, key);
    if (maybe_prop.HasError()) {
      switch (maybe_prop.GetError()) {
        case storage::Error::DELETED_OBJECT:
        case storage::Error::NONEXISTENT_OBJECT:
          // Treat deleted/nonexistent vertex as having no properties.
          return new_mgp_object<mgp_value>(memory);
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
          spdlog::error("Unexpected error when getting vertex property");
          return nullptr;
      }
    }
    return new_mgp_object<mgp_value>(memory, std::move(*maybe_prop));
  } catch (...) {
    // In case NameToProperty or GetProperty throw an exception, most likely
    // std::bad_alloc.
    return nullptr;
  }
}

mgp_properties_iterator *mgp_vertex_iter_properties(const mgp_vertex *v, mgp_memory *memory) {
  // NOTE: This copies the whole properties into the iterator.
  // TODO: Think of a good way to avoid the copy which doesn't just rely on some
  // assumption that storage may return a pointer to the property store. This
  // will probably require a different API in storage.
  try {
    auto maybe_props = v->impl.Properties(v->graph->view);
    if (maybe_props.HasError()) {
      switch (maybe_props.GetError()) {
        case storage::Error::DELETED_OBJECT:
        case storage::Error::NONEXISTENT_OBJECT:
          // Treat deleted/nonexistent vertex as having no properties.
          return new_mgp_object<mgp_properties_iterator>(memory, v->graph);
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
          spdlog::error("Unexpected error when getting vertex properties");
          return nullptr;
      }
    }
    return new_mgp_object<mgp_properties_iterator>(memory, v->graph, std::move(*maybe_props));
  } catch (...) {
    // Since we are copying stuff, we may get std::bad_alloc. Hopefully, no
    // other exceptions are possible, but catch them all just in case.
    return nullptr;
  }
}

void mgp_edges_iterator_destroy(mgp_edges_iterator *it) { delete_mgp_object(it); }

mgp_edges_iterator *mgp_vertex_iter_in_edges(const mgp_vertex *v, mgp_memory *memory) {
  auto *it = new_mgp_object<mgp_edges_iterator>(memory, *v);
  if (!it) return nullptr;
  try {
    auto maybe_edges = v->impl.InEdges(v->graph->view);
    if (maybe_edges.HasError()) {
      switch (maybe_edges.GetError()) {
        case storage::Error::DELETED_OBJECT:
        case storage::Error::NONEXISTENT_OBJECT:
          // Treat deleted/nonexistent vertex as having no edges.
          return it;
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
          spdlog::error("Unexpected error when getting in edges");
          mgp_edges_iterator_destroy(it);
          return nullptr;
      }
    }
    it->in.emplace(std::move(*maybe_edges));
    it->in_it.emplace(it->in->begin());
    if (*it->in_it != it->in->end()) {
      it->current_e.emplace(**it->in_it, v->graph, it->GetMemoryResource());
    }
  } catch (...) {
    // We are probably copying edges, and that may throw std::bad_alloc.
    mgp_edges_iterator_destroy(it);
    return nullptr;
  }
  return it;
}

mgp_edges_iterator *mgp_vertex_iter_out_edges(const mgp_vertex *v, mgp_memory *memory) {
  auto *it = new_mgp_object<mgp_edges_iterator>(memory, *v);
  if (!it) return nullptr;
  try {
    auto maybe_edges = v->impl.OutEdges(v->graph->view);
    if (maybe_edges.HasError()) {
      switch (maybe_edges.GetError()) {
        case storage::Error::DELETED_OBJECT:
        case storage::Error::NONEXISTENT_OBJECT:
          // Treat deleted/nonexistent vertex as having no edges.
          return it;
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
          spdlog::error("Unexpected error when getting out edges");
          mgp_edges_iterator_destroy(it);
          return nullptr;
      }
    }
    it->out.emplace(std::move(*maybe_edges));
    it->out_it.emplace(it->out->begin());
    if (*it->out_it != it->out->end()) {
      it->current_e.emplace(**it->out_it, v->graph, it->GetMemoryResource());
    }
  } catch (...) {
    // We are probably copying edges, and that may throw std::bad_alloc.
    mgp_edges_iterator_destroy(it);
    return nullptr;
  }
  return it;
}

int mgp_edges_iterator_underlying_graph_is_mutable(const struct mgp_edges_iterator *it) {
  return mgp_vertex_underlying_graph_is_mutable(&it->source_vertex);
}

const mgp_edge *mgp_edges_iterator_get(const mgp_edges_iterator *it) {
  if (it->current_e) return &*it->current_e;
  return nullptr;
}

struct mgp_edge *mgp_edges_iterator_get_mutable(struct mgp_edges_iterator *it) {
  if (mgp_edges_iterator_underlying_graph_is_mutable(it) == 0 || !it->current_e.has_value()) {
    return nullptr;
  }
  return &*it->current_e;
}

const mgp_edge *mgp_edges_iterator_next(mgp_edges_iterator *it) {
  if (!it->in && !it->out) return nullptr;
  auto next = [&](auto *impl_it, const auto &end) -> const mgp_edge * {
    if (*impl_it == end) {
      MG_ASSERT(!it->current_e,
                "Iteration is already done, so it->current_e "
                "should have been set to std::nullopt");
      return nullptr;
    }
    if (++(*impl_it) == end) {
      it->current_e = std::nullopt;
      return nullptr;
    }
    it->current_e.emplace(**impl_it, it->source_vertex.graph, it->GetMemoryResource());
    return &*it->current_e;
  };
  try {
    if (it->in_it) {
      return next(&*it->in_it, it->in->end());
    } else {
      return next(&*it->out_it, it->out->end());
    }
  } catch (...) {
    // Just to be sure that operator++ or anything else has thrown something.
    it->current_e = std::nullopt;
    return nullptr;
  }
}

mgp_edge_id mgp_edge_get_id(const mgp_edge *e) { return mgp_edge_id{.as_int = e->impl.Gid().AsInt()}; }

int mgp_edge_underlying_graph_is_mutable(const struct mgp_edge *e) {
  return mgp_vertex_underlying_graph_is_mutable(&e->from);
}

mgp_edge *mgp_edge_copy(const mgp_edge *e, mgp_memory *memory) {
  return new_mgp_object<mgp_edge>(memory, e->impl, e->from.graph);
}

void mgp_edge_destroy(mgp_edge *e) { delete_mgp_object(e); }

int mgp_edge_equal(const struct mgp_edge *e1, const struct mgp_edge *e2) { return e1->impl == e2->impl ? 1 : 0; }

mgp_edge_type mgp_edge_get_type(const mgp_edge *e) {
  const auto &name = e->from.graph->impl->EdgeTypeToName(e->impl.EdgeType());
  static_assert(std::is_lvalue_reference_v<decltype(e->from.graph->impl->EdgeTypeToName(e->impl.EdgeType()))>,
                "Expected EdgeTypeToName to return a pointer or reference, so we "
                "don't have to take a copy and manage memory.");
  return mgp_edge_type{name.c_str()};
}

const mgp_vertex *mgp_edge_get_from(const mgp_edge *e) { return &e->from; }

const mgp_vertex *mgp_edge_get_to(const mgp_edge *e) { return &e->to; }

struct mgp_vertex *mgp_edge_get_mutable_from(struct mgp_edge *e) {
  if (!MgpEdgeIsMutable(*e)) {
    return nullptr;
  }
  return &e->from;
}

struct mgp_vertex *mgp_edge_get_mutable_to(struct mgp_edge *e) {
  if (!MgpEdgeIsMutable(*e)) {
    return nullptr;
  }
  return &e->to;
}

mgp_value *mgp_edge_get_property(const mgp_edge *e, const char *name, mgp_memory *memory) {
  try {
    const auto &key = e->from.graph->impl->NameToProperty(name);
    auto view = e->from.graph->view;
    auto maybe_prop = e->impl.GetProperty(view, key);
    if (maybe_prop.HasError()) {
      switch (maybe_prop.GetError()) {
        case storage::Error::DELETED_OBJECT:
        case storage::Error::NONEXISTENT_OBJECT:
          // Treat deleted/nonexistent edge as having no properties.
          return new_mgp_object<mgp_value>(memory);
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
          spdlog::error("Unexpected error when getting edge property");
          return nullptr;
      }
    }
    return new_mgp_object<mgp_value>(memory, std::move(*maybe_prop));
  } catch (...) {
    // In case NameToProperty or GetProperty throw an exception, most likely
    // std::bad_alloc.
    return nullptr;
  }
}

int mgp_edge_set_property(struct mgp_edge *e, const char *property_name, const struct mgp_value *property_value) {
  if (!MgpEdgeIsMutable(*e)) {
    return 0;
  }

  if (auto maybe_prop_value = ToPropertyValue(*property_value); maybe_prop_value.has_value()) {
    return ResultToReturnCode(
        e->impl.SetProperty(e->from.graph->impl->NameToProperty(property_name), std::move(maybe_prop_value).value()));
  }
  return 0;
}

mgp_properties_iterator *mgp_edge_iter_properties(const mgp_edge *e, mgp_memory *memory) {
  // NOTE: This copies the whole properties into iterator.
  // TODO: Think of a good way to avoid the copy which doesn't just rely on some
  // assumption that storage may return a pointer to the property store. This
  // will probably require a different API in storage.
  try {
    auto view = e->from.graph->view;
    auto maybe_props = e->impl.Properties(view);
    if (maybe_props.HasError()) {
      switch (maybe_props.GetError()) {
        case storage::Error::DELETED_OBJECT:
        case storage::Error::NONEXISTENT_OBJECT:
          // Treat deleted/nonexistent edge as having no properties.
          return new_mgp_object<mgp_properties_iterator>(memory, e->from.graph);
        case storage::Error::PROPERTIES_DISABLED:
        case storage::Error::VERTEX_HAS_EDGES:
        case storage::Error::SERIALIZATION_ERROR:
          spdlog::error("Unexpected error when getting edge properties");
          return nullptr;
      }
    }
    return new_mgp_object<mgp_properties_iterator>(memory, e->from.graph, std::move(*maybe_props));
  } catch (...) {
    // Since we are copying stuff, we may get std::bad_alloc. Hopefully, no
    // other exceptions are possible, but catch them all just in case.
    return nullptr;
  }
}

mgp_vertex *mgp_graph_get_vertex_by_id(const mgp_graph *graph, mgp_vertex_id id, mgp_memory *memory) {
  auto maybe_vertex = graph->impl->FindVertex(storage::Gid::FromInt(id.as_int), graph->view);
  if (maybe_vertex) return new_mgp_object<mgp_vertex>(memory, *maybe_vertex, graph);
  return nullptr;
}

int mgp_graph_is_mutable(const struct mgp_graph *graph) { return MgpGraphIsMutable(*graph) ? 1 : 0; };

mgp_vertex *mgp_graph_create_vertex(struct mgp_graph *graph, struct mgp_memory *memory) {
  if (!MgpGraphIsMutable(*graph)) {
    return nullptr;
  }
  auto vertex = graph->impl->InsertVertex();
  return new_mgp_object<mgp_vertex>(memory, vertex, graph);
}

int mgp_graph_remove_vertex(struct mgp_graph *graph, struct mgp_vertex *vertex) {
  if (!MgpGraphIsMutable(*graph)) {
    return 0;
  }
  return ResultToReturnCode(graph->impl->RemoveVertex(&vertex->impl));
}

struct mgp_edge *mgp_graph_create_edge(struct mgp_graph *graph, struct mgp_vertex *from, struct mgp_vertex *to,
                                       struct mgp_edge_type type, struct mgp_memory *memory) {
  if (!MgpGraphIsMutable(*graph)) {
    return nullptr;
  }
  auto edge = graph->impl->InsertEdge(&from->impl, &to->impl, from->graph->impl->NameToEdgeType(type.name));
  if (edge.HasError()) {
    return nullptr;
  }

  return new_mgp_object<mgp_edge>(memory, edge.GetValue(), from->graph);
}

int mgp_graph_remove_edge(struct mgp_graph *graph, struct mgp_edge *edge) {
  if (!MgpGraphIsMutable(*graph)) {
    return 0;
  }
  return ResultToReturnCode(graph->impl->RemoveEdge(&edge->impl));
}

void mgp_vertices_iterator_destroy(mgp_vertices_iterator *it) { delete_mgp_object(it); }

mgp_vertices_iterator *mgp_graph_iter_vertices(const mgp_graph *graph, mgp_memory *memory) {
  try {
    return new_mgp_object<mgp_vertices_iterator>(memory, graph);
  } catch (...) {
    return nullptr;
  }
}

int mgp_vertices_iterator_underlying_graph_is_mutable(const struct mgp_vertices_iterator *it) {
  return mgp_graph_is_mutable(it->graph);
}

const mgp_vertex *mgp_vertices_iterator_get(const mgp_vertices_iterator *it) {
  if (it->current_v) {
    return &*it->current_v;
  }
  return nullptr;
}

mgp_vertex *mgp_vertices_iterator_get_mutable(mgp_vertices_iterator *it) {
  if (mgp_vertices_iterator_underlying_graph_is_mutable(it) == 0 || !it->current_v.has_value()) {
    return nullptr;
  }
  return &*it->current_v;
}

const mgp_vertex *mgp_vertices_iterator_next(mgp_vertices_iterator *it) {
  try {
    if (it->current_it == it->vertices.end()) {
      MG_ASSERT(!it->current_v,
                "Iteration is already done, so it->current_v "
                "should have been set to std::nullopt");
      return nullptr;
    }
    if (++it->current_it == it->vertices.end()) {
      it->current_v = std::nullopt;
      return nullptr;
    }
    it->current_v.emplace(*it->current_it, it->graph, it->GetMemoryResource());
    return &*it->current_v;
  } catch (...) {
    // VerticesIterable::Iterator::operator++ may throw
    it->current_v = std::nullopt;
    return nullptr;
  }
}

/// Type System
///
/// All types are allocated globally, so that we simplify the API and minimize
/// allocations done for types.

namespace {
void NoOpCypherTypeDeleter(CypherType *) {}
}  // namespace

const mgp_type *mgp_type_any() {
  static AnyType impl;
  static mgp_type any_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &any_type;
}

const mgp_type *mgp_type_bool() {
  static BoolType impl;
  static mgp_type bool_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &bool_type;
}

const mgp_type *mgp_type_string() {
  static StringType impl;
  static mgp_type string_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &string_type;
}

const mgp_type *mgp_type_int() {
  static IntType impl;
  static mgp_type int_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &int_type;
}

const mgp_type *mgp_type_float() {
  static FloatType impl;
  static mgp_type float_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &float_type;
}

const mgp_type *mgp_type_number() {
  static NumberType impl;
  static mgp_type number_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &number_type;
}

const mgp_type *mgp_type_map() {
  static MapType impl;
  static mgp_type map_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &map_type;
}

const mgp_type *mgp_type_node() {
  static NodeType impl;
  static mgp_type node_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &node_type;
}

const mgp_type *mgp_type_relationship() {
  static RelationshipType impl;
  static mgp_type relationship_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &relationship_type;
}

const mgp_type *mgp_type_path() {
  static PathType impl;
  static mgp_type path_type{CypherTypePtr(&impl, NoOpCypherTypeDeleter)};
  return &path_type;
}

const mgp_type *mgp_type_list(const mgp_type *type) {
  if (!type) return nullptr;
  // Maps `type` to corresponding instance of ListType.
  static utils::pmr::map<const mgp_type *, mgp_type> list_types(utils::NewDeleteResource());
  static utils::SpinLock lock;
  std::lock_guard<utils::SpinLock> guard(lock);
  auto found_it = list_types.find(type);
  if (found_it != list_types.end()) return &found_it->second;
  try {
    auto alloc = list_types.get_allocator();
    CypherTypePtr impl(
        alloc.new_object<ListType>(
            // Just obtain the pointer to original impl, don't own it.
            CypherTypePtr(type->impl.get(), NoOpCypherTypeDeleter), alloc.GetMemoryResource()),
        [alloc](CypherType *base_ptr) mutable { alloc.delete_object(static_cast<ListType *>(base_ptr)); });
    return &list_types.emplace(type, mgp_type{std::move(impl)}).first->second;
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}

const mgp_type *mgp_type_nullable(const mgp_type *type) {
  if (!type) return nullptr;
  // Maps `type` to corresponding instance of NullableType.
  static utils::pmr::map<const mgp_type *, mgp_type> gNullableTypes(utils::NewDeleteResource());
  static utils::SpinLock lock;
  std::lock_guard<utils::SpinLock> guard(lock);
  auto found_it = gNullableTypes.find(type);
  if (found_it != gNullableTypes.end()) return &found_it->second;
  try {
    auto alloc = gNullableTypes.get_allocator();
    auto impl = NullableType::Create(CypherTypePtr(type->impl.get(), NoOpCypherTypeDeleter), alloc.GetMemoryResource());
    return &gNullableTypes.emplace(type, mgp_type{std::move(impl)}).first->second;
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}

namespace {
template <typename TProc>
struct mgp_proc *mgp_module_add_procedure(mgp_module *module, const char *name, TProc cb) noexcept {
  if (!module || !cb) return nullptr;
  if (!IsValidIdentifierName(name)) return nullptr;
  if (module->procedures.find(name) != module->procedures.end()) return nullptr;
  try {
    auto *memory = module->procedures.get_allocator().GetMemoryResource();
    // May throw std::bad_alloc, std::length_error
    return &module->procedures.emplace(name, mgp_proc(name, cb, memory)).first->second;
  } catch (...) {
    return nullptr;
  }
}
}  // namespace

mgp_proc *mgp_module_add_read_procedure(mgp_module *module, const char *name, mgp_read_proc_cb cb) {
  return mgp_module_add_procedure(module, name, cb);
}

mgp_proc *mgp_module_add_write_procedure(mgp_module *module, const char *name, mgp_write_proc_cb cb) {
  return mgp_module_add_procedure(module, name, cb);
}

int mgp_proc_add_arg(mgp_proc *proc, const char *name, const mgp_type *type) {
  if (!proc || !type) return 0;
  if (!proc->opt_args.empty()) return 0;
  if (!IsValidIdentifierName(name)) return 0;
  try {
    proc->args.emplace_back(name, type->impl.get());
    return 1;
  } catch (...) {
    return 0;
  }
}

int mgp_proc_add_opt_arg(mgp_proc *proc, const char *name, const mgp_type *type, const mgp_value *default_value) {
  if (!proc || !type || !default_value) return 0;
  if (!IsValidIdentifierName(name)) return 0;
  switch (mgp_value_get_type(default_value)) {
    case MGP_VALUE_TYPE_VERTEX:
    case MGP_VALUE_TYPE_EDGE:
    case MGP_VALUE_TYPE_PATH:
      // default_value must not be a graph element.
      return 0;
    case MGP_VALUE_TYPE_NULL:
    case MGP_VALUE_TYPE_BOOL:
    case MGP_VALUE_TYPE_INT:
    case MGP_VALUE_TYPE_DOUBLE:
    case MGP_VALUE_TYPE_STRING:
    case MGP_VALUE_TYPE_LIST:
    case MGP_VALUE_TYPE_MAP:
      break;
  }
  // Default value must be of required `type`.
  if (!type->impl->SatisfiesType(*default_value)) return 0;
  auto *memory = proc->opt_args.get_allocator().GetMemoryResource();
  try {
    proc->opt_args.emplace_back(utils::pmr::string(name, memory), type->impl.get(),
                                ToTypedValue(*default_value, memory));
    return 1;
  } catch (...) {
    return 0;
  }
}

namespace {

template <typename T>
concept ModuleProperties = utils::SameAsAnyOf<T, mgp_proc, mgp_trans>;

template <ModuleProperties T>
bool AddResultToProp(T *prop, const char *name, const mgp_type *type, bool is_deprecated) {
  if (!prop || !type) return false;
  if (!IsValidIdentifierName(name)) return false;
  if (prop->results.find(name) != prop->results.end()) return false;
  try {
    auto *memory = prop->results.get_allocator().GetMemoryResource();
    prop->results.emplace(utils::pmr::string(name, memory), std::make_pair(type->impl.get(), is_deprecated));
    return true;
  } catch (...) {
    return false;
  }
}

}  // namespace

int mgp_proc_add_result(mgp_proc *proc, const char *name, const mgp_type *type) {
  return AddResultToProp(proc, name, type, false);
}

bool MgpTransAddFixedResult(mgp_trans *trans) {
  if (int err = AddResultToProp(trans, "query", mgp_type_string(), false); err != 1) {
    return err;
  }
  return AddResultToProp(trans, "parameters", mgp_type_nullable(mgp_type_map()), false);
}

int mgp_proc_add_deprecated_result(mgp_proc *proc, const char *name, const mgp_type *type) {
  return AddResultToProp(proc, name, type, true);
}

int mgp_must_abort(const mgp_graph *graph) {
  MG_ASSERT(graph->ctx);
  return query::MustAbort(*graph->ctx);
}

namespace query::procedure {

namespace {

// Print the value in user presentable fashion.
// @throw std::bad_alloc
// @throw std::length_error
std::ostream &PrintValue(const TypedValue &value, std::ostream *stream) {
  switch (value.type()) {
    case TypedValue::Type::Null:
      return (*stream) << "Null";
    case TypedValue::Type::Bool:
      return (*stream) << (value.ValueBool() ? "true" : "false");
    case TypedValue::Type::Int:
      return (*stream) << value.ValueInt();
    case TypedValue::Type::Double:
      return (*stream) << value.ValueDouble();
    case TypedValue::Type::String:
      // String value should be escaped, this allocates a new string.
      return (*stream) << utils::Escape(value.ValueString());
    case TypedValue::Type::List:
      (*stream) << "[";
      utils::PrintIterable(*stream, value.ValueList(), ", ",
                           [](auto &stream, const auto &elem) { PrintValue(elem, &stream); });
      return (*stream) << "]";
    case TypedValue::Type::Map:
      (*stream) << "{";
      utils::PrintIterable(*stream, value.ValueMap(), ", ", [](auto &stream, const auto &item) {
        // Map keys are not escaped strings.
        stream << item.first << ": ";
        PrintValue(item.second, &stream);
      });
      return (*stream) << "}";
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
    case TypedValue::Type::Path:
      LOG_FATAL("value must not be a graph element");
  }
}

}  // namespace

void PrintProcSignature(const mgp_proc &proc, std::ostream *stream) {
  (*stream) << proc.name << "(";
  utils::PrintIterable(*stream, proc.args, ", ", [](auto &stream, const auto &arg) {
    stream << arg.first << " :: " << arg.second->GetPresentableName();
  });
  if (!proc.args.empty() && !proc.opt_args.empty()) (*stream) << ", ";
  utils::PrintIterable(*stream, proc.opt_args, ", ", [](auto &stream, const auto &arg) {
    stream << std::get<0>(arg) << " = ";
    PrintValue(std::get<2>(arg), &stream) << " :: " << std::get<1>(arg)->GetPresentableName();
  });
  (*stream) << ") :: (";
  utils::PrintIterable(*stream, proc.results, ", ", [](auto &stream, const auto &name_result) {
    const auto &[type, is_deprecated] = name_result.second;
    if (is_deprecated) stream << "DEPRECATED ";
    stream << name_result.first << " :: " << type->GetPresentableName();
  });
  (*stream) << ")";
}

bool IsValidIdentifierName(const char *name) {
  if (!name) return false;
  std::regex regex("[_[:alpha:]][_[:alnum:]]*");
  return std::regex_match(name, regex);
}

}  // namespace query::procedure

const char *mgp_message_payload(const mgp_message *message) { return message->msg->Payload().data(); }

size_t mgp_message_payload_size(const mgp_message *message) { return message->msg->Payload().size(); }

const char *mgp_message_topic_name(const mgp_message *message) { return message->msg->TopicName().data(); }

const char *mgp_message_key(const mgp_message *message) { return message->msg->Key().data(); }

size_t mgp_message_key_size(const struct mgp_message *message) { return message->msg->Key().size(); }

int64_t mgp_message_timestamp(const mgp_message *message) { return message->msg->Timestamp(); }

size_t mgp_messages_size(const mgp_messages *messages) { return messages->messages.size(); }

const mgp_message *mgp_messages_at(const mgp_messages *messages, size_t index) {
  return index >= mgp_messages_size(messages) ? nullptr : &messages->messages[index];
}

int mgp_module_add_transformation(mgp_module *module, const char *name, mgp_trans_cb cb) {
  if (!module || !cb) return 0;
  if (!IsValidIdentifierName(name)) return 0;
  if (module->transformations.find(name) != module->transformations.end()) return 0;
  try {
    auto *memory = module->transformations.get_allocator().GetMemoryResource();
    // May throw std::bad_alloc, std::length_error
    module->transformations.emplace(name, mgp_trans(name, cb, memory));
    return 1;
  } catch (...) {
    return 0;
  }
}
