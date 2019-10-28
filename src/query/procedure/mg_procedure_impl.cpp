#include "query/procedure/mg_procedure_impl.hpp"

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <type_traits>

#include <glog/logging.h>

#include "utils/math.hpp"

void *mgp_alloc(mgp_memory *memory, size_t size_in_bytes) {
  return mgp_aligned_alloc(memory, size_in_bytes, alignof(std::max_align_t));
}

void *mgp_aligned_alloc(mgp_memory *memory, const size_t size_in_bytes,
                        const size_t alignment) {
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
  const auto maybe_bytes_for_header =
      utils::RoundUint64ToMultiple(header_size, alloc_align);
  if (!maybe_bytes_for_header) return nullptr;
  const size_t bytes_for_header = *maybe_bytes_for_header;
  const size_t alloc_size = bytes_for_header + size_in_bytes;
  if (alloc_size < size_in_bytes) return nullptr;
  try {
    void *ptr = memory->impl->Allocate(alloc_size, alloc_align);
    char *data = reinterpret_cast<char *>(ptr) + bytes_for_header;
    std::memcpy(data - sizeof(size_in_bytes), &size_in_bytes,
                sizeof(size_in_bytes));
    std::memcpy(data - sizeof(size_in_bytes) - sizeof(alloc_align),
                &alloc_align, sizeof(alloc_align));
    return data;
  } catch (...) {
    return nullptr;
  }
}

void mgp_free(mgp_memory *memory, void *const p) {
  if (!p) return;
  char *const data = reinterpret_cast<char *>(p);
  // Read the header containing size & alignment info.
  size_t size_in_bytes;
  std::memcpy(&size_in_bytes, data - sizeof(size_in_bytes),
              sizeof(size_in_bytes));
  size_t alloc_align;
  std::memcpy(&alloc_align, data - sizeof(size_in_bytes) - sizeof(alloc_align),
              sizeof(alloc_align));
  // Reconstruct how many bytes we allocated on top of the original request.
  // We need not check allocation request overflow, since we did so already in
  // mgp_aligned_alloc.
  const size_t header_size = sizeof(size_in_bytes) + sizeof(alloc_align);
  const size_t bytes_for_header =
      *utils::RoundUint64ToMultiple(header_size, alloc_align);
  const size_t alloc_size = bytes_for_header + size_in_bytes;
  // Get the original ptr we allocated.
  void *const original_ptr = data - bytes_for_header;
  memory->impl->Deallocate(original_ptr, alloc_size, alloc_align);
}

namespace {

// May throw whatever the constructor of U throws. `std::bad_alloc` is handled
// by returning nullptr.
template <class U, class... TArgs>
U *new_mgp_object(utils::MemoryResource *memory, TArgs &&... args) {
  utils::Allocator<U> allocator(memory);
  try {
    return allocator.template new_object<U>(std::forward<TArgs>(args)...);
  } catch (const std::bad_alloc &) {
    return nullptr;
  }
}

template <class U, class... TArgs>
U *new_mgp_object(mgp_memory *memory, TArgs &&... args) {
  return new_mgp_object<U, TArgs...>(memory->impl,
                                     std::forward<TArgs>(args)...);
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

query::TypedValue ToTypedValue(const mgp_value &val,
                               utils::MemoryResource *memory) {
  switch (mgp_value_get_type(&val)) {
    case MGP_VALUE_TYPE_NULL:
      return query::TypedValue(memory);
    case MGP_VALUE_TYPE_BOOL:
      return query::TypedValue(static_cast<bool>(mgp_value_get_bool(&val)),
                               memory);
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
      CHECK(!path->vertices.empty());
      CHECK(path->vertices.size() == path->edges.size() + 1);
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

mgp_value::mgp_value(utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_NULL), memory(m) {}

mgp_value::mgp_value(bool val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_BOOL), memory(m), bool_v(val) {}

mgp_value::mgp_value(int64_t val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_INT), memory(m), int_v(val) {}

mgp_value::mgp_value(double val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_DOUBLE), memory(m), double_v(val) {}

mgp_value::mgp_value(const char *val, utils::MemoryResource *m)
    : type(MGP_VALUE_TYPE_STRING), memory(m), string_v(val, m) {}

mgp_value::mgp_value(mgp_list *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_LIST), memory(m), list_v(val) {
  CHECK(val->GetMemoryResource() == m)
      << "Unable to take ownership of a pointer with different allocator.";
}

mgp_value::mgp_value(mgp_map *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_MAP), memory(m), map_v(val) {
  CHECK(val->GetMemoryResource() == m)
      << "Unable to take ownership of a pointer with different allocator.";
}

mgp_value::mgp_value(mgp_vertex *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_VERTEX), memory(m), vertex_v(val) {
  CHECK(val->GetMemoryResource() == m)
      << "Unable to take ownership of a pointer with different allocator.";
}

mgp_value::mgp_value(mgp_edge *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_EDGE), memory(m), edge_v(val) {
  CHECK(val->GetMemoryResource() == m)
      << "Unable to take ownership of a pointer with different allocator.";
}

mgp_value::mgp_value(mgp_path *val, utils::MemoryResource *m) noexcept
    : type(MGP_VALUE_TYPE_PATH), memory(m), path_v(val) {
  CHECK(val->GetMemoryResource() == m)
      << "Unable to take ownership of a pointer with different allocator.";
}

mgp_value::mgp_value(const query::TypedValue &tv, const mgp_graph *graph,
                     utils::MemoryResource *m)
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

mgp_value::mgp_value(const PropertyValue &pv, utils::MemoryResource *m)
    : memory(m) {
  switch (pv.type()) {
    case PropertyValue::Type::Null:
      type = MGP_VALUE_TYPE_NULL;
      break;
    case PropertyValue::Type::Bool:
      type = MGP_VALUE_TYPE_BOOL;
      bool_v = pv.ValueBool();
      break;
    case PropertyValue::Type::Int:
      type = MGP_VALUE_TYPE_INT;
      int_v = pv.ValueInt();
      break;
    case PropertyValue::Type::Double:
      type = MGP_VALUE_TYPE_DOUBLE;
      double_v = pv.ValueDouble();
      break;
    case PropertyValue::Type::String:
      type = MGP_VALUE_TYPE_STRING;
      new (&string_v) utils::pmr::string(pv.ValueString(), m);
      break;
    case PropertyValue::Type::List: {
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
    case PropertyValue::Type::Map: {
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

mgp_value::mgp_value(const mgp_value &other, utils::MemoryResource *m)
    : type(other.type), memory(m) {
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
  CHECK(value);
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

mgp_value::mgp_value(mgp_value &&other, utils::MemoryResource *m)
    : type(other.type), memory(m) {
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
      static_assert(std::is_pointer_v<decltype(list_v)>,
                    "Expected to move list_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        list_v = other.list_v;
      } else {
        utils::Allocator<mgp_list> allocator(m);
        list_v = allocator.new_object<mgp_list>(std::move(*other.list_v));
      }
      break;
    case MGP_VALUE_TYPE_MAP:
      static_assert(std::is_pointer_v<decltype(map_v)>,
                    "Expected to move map_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        map_v = other.map_v;
      } else {
        utils::Allocator<mgp_map> allocator(m);
        map_v = allocator.new_object<mgp_map>(std::move(*other.map_v));
      }
      break;
    case MGP_VALUE_TYPE_VERTEX:
      static_assert(std::is_pointer_v<decltype(vertex_v)>,
                    "Expected to move vertex_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        vertex_v = other.vertex_v;
      } else {
        utils::Allocator<mgp_vertex> allocator(m);
        vertex_v = allocator.new_object<mgp_vertex>(std::move(*other.vertex_v));
      }
      break;
    case MGP_VALUE_TYPE_EDGE:
      static_assert(std::is_pointer_v<decltype(edge_v)>,
                    "Expected to move edge_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        edge_v = other.edge_v;
      } else {
        utils::Allocator<mgp_edge> allocator(m);
        edge_v = allocator.new_object<mgp_edge>(std::move(*other.edge_v));
      }
      break;
    case MGP_VALUE_TYPE_PATH:
      static_assert(std::is_pointer_v<decltype(path_v)>,
                    "Expected to move path_v by copying pointers.");
      if (*other.GetMemoryResource() == *m) {
        path_v = other.path_v;
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

mgp_value *mgp_value_make_null(mgp_memory *memory) {
  return new_mgp_object<mgp_value>(memory);
}

mgp_value *mgp_value_make_bool(int val, mgp_memory *memory) {
  return new_mgp_object<mgp_value>(memory, val != 0);
}

mgp_value *mgp_value_make_int(int64_t val, mgp_memory *memory) {
  return new_mgp_object<mgp_value>(memory, val);
}

mgp_value *mgp_value_make_double(double val, mgp_memory *memory) {
  return new_mgp_object<mgp_value>(memory, val);
}

mgp_value *mgp_value_make_string(const char *val, mgp_memory *memory) {
  try {
    // This may throw something from std::string constructor, it could be
    // std::length_error, but it's not really well defined, so catch all.
    return new_mgp_object<mgp_value>(memory, val);
  } catch (...) {
    return nullptr;
  }
}

mgp_value *mgp_value_make_list(mgp_list *val) {
  return new_mgp_object<mgp_value>(val->GetMemoryResource(), val);
}

mgp_value *mgp_value_make_map(mgp_map *val) {
  return new_mgp_object<mgp_value>(val->GetMemoryResource(), val);
}

mgp_value *mgp_value_make_vertex(mgp_vertex *val) {
  return new_mgp_object<mgp_value>(val->GetMemoryResource(), val);
}

mgp_value *mgp_value_make_edge(mgp_edge *val) {
  return new_mgp_object<mgp_value>(val->GetMemoryResource(), val);
}

mgp_value *mgp_value_make_path(mgp_path *val) {
  return new_mgp_object<mgp_value>(val->GetMemoryResource(), val);
}

mgp_value_type mgp_value_get_type(const mgp_value *val) { return val->type; }

int mgp_value_is_null(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_NULL;
}

int mgp_value_is_bool(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_BOOL;
}

int mgp_value_is_int(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_INT;
}

int mgp_value_is_double(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_DOUBLE;
}

int mgp_value_is_string(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_STRING;
}

int mgp_value_is_list(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_LIST;
}

int mgp_value_is_map(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_MAP;
}

int mgp_value_is_vertex(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_VERTEX;
}

int mgp_value_is_edge(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_EDGE;
}

int mgp_value_is_path(const mgp_value *val) {
  return mgp_value_get_type(val) == MGP_VALUE_TYPE_PATH;
}

int mgp_value_get_bool(const mgp_value *val) { return val->bool_v ? 1 : 0; }

int64_t mgp_value_get_int(const mgp_value *val) { return val->int_v; }

double mgp_value_get_double(const mgp_value *val) { return val->double_v; }

const char *mgp_value_get_string(const mgp_value *val) {
  return val->string_v.c_str();
}

const mgp_list *mgp_value_get_list(const mgp_value *val) { return val->list_v; }

const mgp_map *mgp_value_get_map(const mgp_value *val) { return val->map_v; }

const mgp_vertex *mgp_value_get_vertex(const mgp_value *val) {
  return val->vertex_v;
}

const mgp_edge *mgp_value_get_edge(const mgp_value *val) { return val->edge_v; }

const mgp_path *mgp_value_get_path(const mgp_value *val) { return val->path_v; }

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
  try {
    res->rows.push_back(mgp_result_record{
        utils::pmr::map<utils::pmr::string, query::TypedValue>(memory)});
  } catch (...) {
    return nullptr;
  }
  return &res->rows.back();
}

int mgp_result_record_insert(mgp_result_record *record, const char *field_name,
                             const mgp_value *val) {
  auto *memory = record->values.get_allocator().GetMemoryResource();
  // TODO: Result validation when we add registering procedures with result
  // signature description.
  try {
    record->values.emplace(field_name, ToTypedValue(*val, memory));
  } catch (...) {
    return 0;
  }
  return 1;
}
