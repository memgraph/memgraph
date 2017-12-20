#include "query/frontend/ast/ast.hpp"

namespace query {

AstTreeStorage::AstTreeStorage() {
  storage_.emplace_back(new Query(next_uid_++));
}

Query *AstTreeStorage::query() const {
  return dynamic_cast<Query *>(storage_[0].get());
}

int AstTreeStorage::MaximumStorageUid() const {
  int max_uid = -1;
  for (const auto &tree : storage_) {
    max_uid = std::max(max_uid, tree->uid());
  }
  return max_uid;
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

#define LOAD_AND_CONSTRUCT(DerivedClass, ...)               \
  template <class TArchive>                                 \
  void load_construct_data(TArchive &ar, DerivedClass *cls, \
                           const unsigned int) {            \
    ::new (cls) DerivedClass(__VA_ARGS__);                  \
  }

// All of the serialization cruft follows

namespace boost::serialization {
LOAD_AND_CONSTRUCT(query::Where, 0);
LOAD_AND_CONSTRUCT(query::OrOperator, 0);
LOAD_AND_CONSTRUCT(query::XorOperator, 0);
LOAD_AND_CONSTRUCT(query::AndOperator, 0);
LOAD_AND_CONSTRUCT(query::AdditionOperator, 0);
LOAD_AND_CONSTRUCT(query::SubtractionOperator, 0);
LOAD_AND_CONSTRUCT(query::MultiplicationOperator, 0);
LOAD_AND_CONSTRUCT(query::DivisionOperator, 0);
LOAD_AND_CONSTRUCT(query::ModOperator, 0);
LOAD_AND_CONSTRUCT(query::NotEqualOperator, 0);
LOAD_AND_CONSTRUCT(query::EqualOperator, 0);
LOAD_AND_CONSTRUCT(query::LessOperator, 0);
LOAD_AND_CONSTRUCT(query::GreaterOperator, 0);
LOAD_AND_CONSTRUCT(query::LessEqualOperator, 0);
LOAD_AND_CONSTRUCT(query::GreaterEqualOperator, 0);
LOAD_AND_CONSTRUCT(query::InListOperator, 0);
LOAD_AND_CONSTRUCT(query::ListMapIndexingOperator, 0);
LOAD_AND_CONSTRUCT(query::ListSlicingOperator, 0, nullptr, nullptr, nullptr);
LOAD_AND_CONSTRUCT(query::IfOperator, 0, nullptr, nullptr, nullptr);
LOAD_AND_CONSTRUCT(query::NotOperator, 0);
LOAD_AND_CONSTRUCT(query::UnaryPlusOperator, 0);
LOAD_AND_CONSTRUCT(query::UnaryMinusOperator, 0);
LOAD_AND_CONSTRUCT(query::IsNullOperator, 0);
LOAD_AND_CONSTRUCT(query::PrimitiveLiteral, 0);
LOAD_AND_CONSTRUCT(query::ListLiteral, 0);
LOAD_AND_CONSTRUCT(query::MapLiteral, 0);
LOAD_AND_CONSTRUCT(query::Identifier, 0, "");
LOAD_AND_CONSTRUCT(query::PropertyLookup, 0, nullptr, "",
                   GraphDbTypes::Property());
LOAD_AND_CONSTRUCT(query::LabelsTest, 0, nullptr,
                   std::vector<GraphDbTypes::Label>());
LOAD_AND_CONSTRUCT(query::Function, 0);
LOAD_AND_CONSTRUCT(query::Aggregation, 0, nullptr, nullptr,
                   query::Aggregation::Op::COUNT);
LOAD_AND_CONSTRUCT(query::All, 0, nullptr, nullptr, nullptr);
LOAD_AND_CONSTRUCT(query::ParameterLookup, 0);
LOAD_AND_CONSTRUCT(query::NamedExpression, 0);
LOAD_AND_CONSTRUCT(query::NodeAtom, 0);
LOAD_AND_CONSTRUCT(query::EdgeAtom, 0);
LOAD_AND_CONSTRUCT(query::Pattern, 0);
LOAD_AND_CONSTRUCT(query::SingleQuery, 0);
LOAD_AND_CONSTRUCT(query::CypherUnion, 0);
LOAD_AND_CONSTRUCT(query::Query, 0);
LOAD_AND_CONSTRUCT(query::Create, 0);
LOAD_AND_CONSTRUCT(query::Match, 0);
LOAD_AND_CONSTRUCT(query::Return, 0);
LOAD_AND_CONSTRUCT(query::With, 0);
LOAD_AND_CONSTRUCT(query::Delete, 0);
LOAD_AND_CONSTRUCT(query::SetProperty, 0);
LOAD_AND_CONSTRUCT(query::SetProperties, 0);
LOAD_AND_CONSTRUCT(query::SetLabels, 0);
LOAD_AND_CONSTRUCT(query::RemoveProperty, 0);
LOAD_AND_CONSTRUCT(query::RemoveLabels, 0);
LOAD_AND_CONSTRUCT(query::Merge, 0);
LOAD_AND_CONSTRUCT(query::Unwind, 0);
LOAD_AND_CONSTRUCT(query::CreateIndex, 0);
}  // namespace boost::serialization

#undef LOAD_AND_CONSTRUCT

// Include archives before registering most derived types.
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/archive/text_iarchive.hpp"
#include "boost/archive/text_oarchive.hpp"

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
BOOST_CLASS_EXPORT_IMPLEMENT(query::All);
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
