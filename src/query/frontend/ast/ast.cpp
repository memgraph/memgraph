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

BOOST_CLASS_EXPORT_IMPLEMENT(query::Query);
BOOST_CLASS_EXPORT_IMPLEMENT(query::SingleQuery);
BOOST_CLASS_EXPORT_IMPLEMENT(query::CypherUnion);
BOOST_CLASS_EXPORT_IMPLEMENT(query::NamedExpression);
BOOST_CLASS_EXPORT_IMPLEMENT(query::OrOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::XorOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::AndOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::NotOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::AdditionOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::SubtractionOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::MultiplicationOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::DivisionOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::ModOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::NotEqualOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::EqualOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::LessOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::GreaterOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::LessEqualOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::GreaterEqualOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::InListOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::ListMapIndexingOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::ListSlicingOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::IfOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::UnaryPlusOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::UnaryMinusOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::IsNullOperator);
BOOST_CLASS_EXPORT_IMPLEMENT(query::ListLiteral);
BOOST_CLASS_EXPORT_IMPLEMENT(query::MapLiteral);
BOOST_CLASS_EXPORT_IMPLEMENT(query::PropertyLookup);
BOOST_CLASS_EXPORT_IMPLEMENT(query::LabelsTest);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Aggregation);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Function);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Reduce);
BOOST_CLASS_EXPORT_IMPLEMENT(query::All);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Single);
BOOST_CLASS_EXPORT_IMPLEMENT(query::ParameterLookup);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Create);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Match);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Return);
BOOST_CLASS_EXPORT_IMPLEMENT(query::With);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Pattern);
BOOST_CLASS_EXPORT_IMPLEMENT(query::NodeAtom);
BOOST_CLASS_EXPORT_IMPLEMENT(query::EdgeAtom);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Delete);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Where);
BOOST_CLASS_EXPORT_IMPLEMENT(query::SetProperty);
BOOST_CLASS_EXPORT_IMPLEMENT(query::SetProperties);
BOOST_CLASS_EXPORT_IMPLEMENT(query::SetLabels);
BOOST_CLASS_EXPORT_IMPLEMENT(query::RemoveProperty);
BOOST_CLASS_EXPORT_IMPLEMENT(query::RemoveLabels);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Merge);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Unwind);
BOOST_CLASS_EXPORT_IMPLEMENT(query::Identifier);
BOOST_CLASS_EXPORT_IMPLEMENT(query::PrimitiveLiteral);
BOOST_CLASS_EXPORT_IMPLEMENT(query::CreateIndex);
