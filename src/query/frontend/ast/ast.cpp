#include "query/frontend/ast/ast.hpp"

#include <algorithm>

namespace query {

AstStorage::AstStorage() {
  std::unique_ptr<Query> root(new Query(next_uid_++));
  root_idx_ = 0;
  storage_.emplace_back(std::move(root));
}

Query *AstStorage::query() const {
  return dynamic_cast<Query *>(storage_[root_idx_].get());
}

ReturnBody CloneReturnBody(AstStorage &storage, const ReturnBody &body) {
  ReturnBody new_body;
  new_body.distinct = body.distinct;
  new_body.all_identifiers = body.all_identifiers;
  for (auto *named_expr : body.named_expressions) {
    new_body.named_expressions.push_back(named_expr->Clone(storage));
  }
  for (auto order : body.order_by) {
    new_body.order_by.push_back(
        SortItem{order.ordering, order.expression->Clone(storage)});
  }
  new_body.skip = body.skip ? body.skip->Clone(storage) : nullptr;
  new_body.limit = body.limit ? body.limit->Clone(storage) : nullptr;
  return new_body;
}

}  // namespace query
