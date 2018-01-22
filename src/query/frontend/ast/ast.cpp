#include "query/frontend/ast/ast.hpp"

// Include archives before registering most derived types.
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"

namespace query {

// Id for boost's archive get_helper needs to be unique among all ids. If it
// isn't unique, then different instances (even across different types!) will
// replace the previous helper. The documentation recommends to take an address
// of a function, which should be unique in the produced binary.
// It might seem logical to take an address of a AstTreeStorage constructor, but
// according to c++ standard, the constructor function need not have an address.
// Additionally, pointers to member functions are not required to contain the
// address of the function
// (https://isocpp.org/wiki/faq/pointers-to-members#addr-of-memfn). So, to be
// safe, use a regular top-level function.
void *const AstTreeStorage::kHelperId = (void *)CloneReturnBody;

AstTreeStorage::AstTreeStorage() {
  storage_.emplace_back(new Query(next_uid_++));
}

Query *AstTreeStorage::query() const {
  return dynamic_cast<Query *>(storage_[0].get());
}

ReturnBody CloneReturnBody(AstTreeStorage &storage, const ReturnBody &body) {
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

}  // namespace query

BOOST_CLASS_EXPORT(query::Query);
BOOST_CLASS_EXPORT(query::SingleQuery);
BOOST_CLASS_EXPORT(query::CypherUnion);
BOOST_CLASS_EXPORT(query::NamedExpression);
BOOST_CLASS_EXPORT(query::OrOperator);
BOOST_CLASS_EXPORT(query::XorOperator);
BOOST_CLASS_EXPORT(query::AndOperator);
BOOST_CLASS_EXPORT(query::NotOperator);
BOOST_CLASS_EXPORT(query::AdditionOperator);
BOOST_CLASS_EXPORT(query::SubtractionOperator);
BOOST_CLASS_EXPORT(query::MultiplicationOperator);
BOOST_CLASS_EXPORT(query::DivisionOperator);
BOOST_CLASS_EXPORT(query::ModOperator);
BOOST_CLASS_EXPORT(query::NotEqualOperator);
BOOST_CLASS_EXPORT(query::EqualOperator);
BOOST_CLASS_EXPORT(query::LessOperator);
BOOST_CLASS_EXPORT(query::GreaterOperator);
BOOST_CLASS_EXPORT(query::LessEqualOperator);
BOOST_CLASS_EXPORT(query::GreaterEqualOperator);
BOOST_CLASS_EXPORT(query::InListOperator);
BOOST_CLASS_EXPORT(query::ListMapIndexingOperator);
BOOST_CLASS_EXPORT(query::ListSlicingOperator);
BOOST_CLASS_EXPORT(query::IfOperator);
BOOST_CLASS_EXPORT(query::UnaryPlusOperator);
BOOST_CLASS_EXPORT(query::UnaryMinusOperator);
BOOST_CLASS_EXPORT(query::IsNullOperator);
BOOST_CLASS_EXPORT(query::ListLiteral);
BOOST_CLASS_EXPORT(query::MapLiteral);
BOOST_CLASS_EXPORT(query::PropertyLookup);
BOOST_CLASS_EXPORT(query::LabelsTest);
BOOST_CLASS_EXPORT(query::Aggregation);
BOOST_CLASS_EXPORT(query::Function);
BOOST_CLASS_EXPORT(query::All);
BOOST_CLASS_EXPORT(query::ParameterLookup);
BOOST_CLASS_EXPORT(query::Create);
BOOST_CLASS_EXPORT(query::Match);
BOOST_CLASS_EXPORT(query::Return);
BOOST_CLASS_EXPORT(query::With);
BOOST_CLASS_EXPORT(query::Pattern);
BOOST_CLASS_EXPORT(query::NodeAtom);
BOOST_CLASS_EXPORT(query::EdgeAtom);
BOOST_CLASS_EXPORT(query::Delete);
BOOST_CLASS_EXPORT(query::Where);
BOOST_CLASS_EXPORT(query::SetProperty);
BOOST_CLASS_EXPORT(query::SetProperties);
BOOST_CLASS_EXPORT(query::SetLabels);
BOOST_CLASS_EXPORT(query::RemoveProperty);
BOOST_CLASS_EXPORT(query::RemoveLabels);
BOOST_CLASS_EXPORT(query::Merge);
BOOST_CLASS_EXPORT(query::Unwind);
BOOST_CLASS_EXPORT(query::Identifier);
BOOST_CLASS_EXPORT(query::PrimitiveLiteral);
BOOST_CLASS_EXPORT(query::CreateIndex);
