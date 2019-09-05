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
      return !a.ValueBool() && b.ValueBool();
    case TypedValue::Type::Int:
      if (b.type() == TypedValue::Type::Double)
        return a.ValueInt() < b.ValueDouble();
      else
        return a.ValueInt() < b.ValueInt();
    case TypedValue::Type::Double:
      if (b.type() == TypedValue::Type::Int)
        return a.ValueDouble() < b.ValueInt();
      else
        return a.ValueDouble() < b.ValueDouble();
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
void SwitchAccessor(TAccessor &accessor, storage::View view) {
  switch (view) {
    case storage::View::NEW:
      accessor.SwitchNew();
      break;
    case storage::View::OLD:
      accessor.SwitchOld();
      break;
  }
}

template void SwitchAccessor<>(VertexAccessor &accessor, storage::View view);
template void SwitchAccessor<>(EdgeAccessor &accessor, storage::View view);

}  // namespace query
