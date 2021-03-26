#include "query/interpret/eval.hpp"

namespace query {

int64_t EvaluateInt(ExpressionEvaluator *evaluator, Expression *expr, const std::string &what) {
  TypedValue value = expr->Accept(*evaluator);
  try {
    return value.ValueInt();
  } catch (TypedValueException &e) {
    throw QueryRuntimeException(what + " must be an int");
  }
}

std::optional<size_t> EvaluateMemoryLimit(ExpressionEvaluator *eval, Expression *memory_limit, size_t memory_scale) {
  if (!memory_limit) return std::nullopt;
  auto limit_value = memory_limit->Accept(*eval);
  if (!limit_value.IsInt() || limit_value.ValueInt() <= 0)
    throw QueryRuntimeException("Memory limit must be a non-negative integer.");
  size_t limit = limit_value.ValueInt();
  if (std::numeric_limits<size_t>::max() / memory_scale < limit) throw QueryRuntimeException("Memory limit overflow.");
  return limit * memory_scale;
}

}  // namespace query
