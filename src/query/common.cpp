#include "query/common.hpp"

namespace query {

void ReconstructTypedValue(TypedValue &value) {
  using Type = TypedValue::Type;
  switch (value.type()) {
    case Type::Vertex:
      if (!value.ValueVertex().Reconstruct()) throw ReconstructionException();
      break;
    case Type::Edge:
      if (!value.ValueEdge().Reconstruct()) throw ReconstructionException();
      break;
    case Type::List:
      for (TypedValue &inner_value : value.ValueList())
        ReconstructTypedValue(inner_value);
      break;
    case Type::Map:
      for (auto &kv : value.ValueMap())
        ReconstructTypedValue(kv.second);
      break;
    case Type::Path:
      for (auto &vertex : value.ValuePath().vertices()) {
        if (!vertex.Reconstruct()) throw ReconstructionException();
      }
      for (auto &edge : value.ValuePath().edges()) {
        if (!edge.Reconstruct()) throw ReconstructionException();
      }
    case Type::Null:
    case Type::Bool:
    case Type::Int:
    case Type::Double:
    case Type::String:
      break;
  }
}

namespace impl {

bool TypedValueCompare(const TypedValue &a, const TypedValue &b) {
  // in ordering null comes after everything else
  // at the same time Null is not less that null
  // first deal with Null < Whatever case
  if (a.IsNull()) return false;
  // now deal with NotNull < Null case
  if (b.IsNull()) return true;

  // comparisons are from this point legal only between values of
  // the  same type, or int+float combinations
  if ((a.type() != b.type() && !(a.IsNumeric() && b.IsNumeric())))
    throw QueryRuntimeException(
        "Can't compare value of type {} to value of type {}.", a.type(),
        b.type());

  switch (a.type()) {
    case TypedValue::Type::Bool:
      return !a.Value<bool>() && b.Value<bool>();
    case TypedValue::Type::Int:
      if (b.type() == TypedValue::Type::Double)
        return a.Value<int64_t>() < b.Value<double>();
      else
        return a.Value<int64_t>() < b.Value<int64_t>();
    case TypedValue::Type::Double:
      if (b.type() == TypedValue::Type::Int)
        return a.Value<double>() < b.Value<int64_t>();
      else
        return a.Value<double>() < b.Value<double>();
    case TypedValue::Type::String:
      return a.ValueString() < b.ValueString();
    case TypedValue::Type::List:
    case TypedValue::Type::Map:
    case TypedValue::Type::Vertex:
    case TypedValue::Type::Edge:
    case TypedValue::Type::Path:
      throw QueryRuntimeException(
          "Comparison is not defined for values of type {}.", a.type());
    default:
      LOG(FATAL) << "Unhandled comparison for types";
  }
}

}  // namespace impl

template <typename TAccessor>
void SwitchAccessor(TAccessor &accessor, GraphView graph_view) {
  switch (graph_view) {
    case GraphView::NEW:
      accessor.SwitchNew();
      break;
    case GraphView::OLD:
      accessor.SwitchOld();
      break;
  }
}

template void SwitchAccessor<>(VertexAccessor &accessor, GraphView graph_view);
template void SwitchAccessor<>(EdgeAccessor &accessor, GraphView graph_view);

}  // namespace query
