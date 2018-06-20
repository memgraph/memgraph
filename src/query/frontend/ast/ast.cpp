#include "query/frontend/ast/ast.hpp"

#include <algorithm>

namespace query {

// Id for boost's archive get_helper needs to be unique among all ids. If it
// isn't unique, then different instances (even across different types!) will
// replace the previous helper. The documentation recommends to take an address
// of a function, which should be unique in the produced binary.
// It might seem logical to take an address of a AstStorage constructor, but
// according to c++ standard, the constructor function need not have an address.
// Additionally, pointers to member functions are not required to contain the
// address of the function
// (https://isocpp.org/wiki/faq/pointers-to-members#addr-of-memfn). So, to be
// safe, use a regular top-level function.
void *const AstStorage::kHelperId = (void *)CloneReturnBody;

AstStorage::AstStorage() {
  storage_.emplace_back(new Query(next_uid_++));
}

Query *AstStorage::query() const {
  return dynamic_cast<Query *>(storage_[0].get());
}

ReturnBody CloneReturnBody(AstStorage &storage, const ReturnBody &body) {
  ReturnBody new_body;
  new_body.distinct = body.distinct;
  new_body.all_identifiers = body.all_identifiers;
  for (auto *named_expr : body.named_expressions) {
    new_body.named_expressions.push_back(named_expr->Clone(storage));
  }
  for (auto order : body.order_by) {
    new_body.order_by.emplace_back(order.first, order.second->Clone(storage));
  }
  new_body.skip = body.skip ? body.skip->Clone(storage) : nullptr;
  new_body.limit = body.limit ? body.limit->Clone(storage) : nullptr;
  return new_body;
}

bool Tree::IsSaved(const std::vector<int> &saved_uids) {
  return utils::Contains(saved_uids, uid_);
}

void Tree::AddToSaved(std::vector<int> *saved_uids) {
  saved_uids->emplace_back(uid_);
}
}
