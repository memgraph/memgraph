#include "query/backend/cpp/runtime_functions.hpp"

#include "storage/record_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "storage/edge_accessor.hpp"

namespace backend {
namespace cpp {

TypedValue GetProperty(const TypedValue &t, const std::string &key) {
  if (t.type() == TypedValue::Type::Vertex) {
    return t.Value<VertexAccessor>().PropsAt(key);
  } else if (t.type() == TypedValue::Type::Edge) {
    return t.Value<EdgeAccessor>().PropsAt(key);
  } else {
    throw TypedValueException("Unsupported type.");
  }
}
}
}
