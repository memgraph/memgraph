#include "query/frontend/ast/ast.hpp"

// Include archives before registering most derived types.
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"

#include "utils/typed_value.capnp.h"

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

// capnproto serialization

Tree *AstTreeStorage::Load(capnp::Tree::Reader &tree) {
  switch (tree.which()) {
    case capnp::Tree::EXPRESSION: {
      auto expr_reader = tree.getExpression();
      auto *ret = Expression::Construct(expr_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::WHERE: {
      auto where_reader = tree.getWhere();
      auto *ret = Where::Construct(where_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::CLAUSE: {
      auto clause_reader = tree.getClause();
      auto *ret = Clause::Construct(clause_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::CYPHER_UNION: {
      auto cu_reader = tree.getCypherUnion();
      auto *ret = CypherUnion::Construct(cu_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::NAMED_EXPRESSION: {
      auto ne_reader = tree.getNamedExpression();
      auto *ret = NamedExpression::Construct(ne_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::PATTERN: {
      auto pattern_reader = tree.getPattern();
      auto *ret = Pattern::Construct(pattern_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::PATTERN_ATOM: {
      auto pa_reader = tree.getPatternAtom();
      auto *ret = PatternAtom::Construct(pa_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::QUERY: {
      auto query_reader = tree.getQuery();
      auto *ret = Query::Construct(query_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
    case capnp::Tree::SINGLE_QUERY: {
      auto single_reader = tree.getSingleQuery();
      auto *ret = SingleQuery::Construct(single_reader, *this);
      ret->Load(tree, *this);
      return ret;
    }
  }
}

// Tree.
void Tree::Save(capnp::Tree::Builder &tree_builder) {
  tree_builder.setUid(uid_);
}

void Tree::Load(capnp::Tree::Reader &reader, AstTreeStorage &storage) {
  uid_ = reader.getUid();
}

// Expression.
void Expression::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto expr_builder = tree_builder.initExpression();
  Save(expr_builder);
}

Expression *Expression::Construct(capnp::Expression::Reader &reader,
                                  AstTreeStorage &storage) {
  switch (reader.which()) {
    case capnp::Expression::BINARY_OPERATOR: {
      auto bop_reader = reader.getBinaryOperator();
      return BinaryOperator::Construct(bop_reader, storage);
    }
    case capnp::Expression::UNARY_OPERATOR: {
      auto uop_reader = reader.getUnaryOperator();
      return UnaryOperator::Construct(uop_reader, storage);
    }
    case capnp::Expression::BASE_LITERAL: {
      auto bl_reader = reader.getBaseLiteral();
      return BaseLiteral::Construct(bl_reader, storage);
    }
    case capnp::Expression::LIST_SLICING_OPERATOR: {
      auto lso_reader = reader.getListSlicingOperator();
      return ListSlicingOperator::Construct(lso_reader, storage);
    }
    case capnp::Expression::IF_OPERATOR: {
      auto if_reader = reader.getIfOperator();
      return IfOperator::Construct(if_reader, storage);
    }
    case capnp::Expression::ALL: {
      auto all_reader = reader.getAll();
      return All::Construct(all_reader, storage);
    }
    case capnp::Expression::FUNCTION: {
      auto func_reader = reader.getFunction();
      return Function::Construct(func_reader, storage);
    }
    case capnp::Expression::IDENTIFIER: {
      auto id_reader = reader.getIdentifier();
      return Identifier::Construct(id_reader, storage);
    }
    case capnp::Expression::LABELS_TEST: {
      auto labels_reader = reader.getLabelsTest();
      return LabelsTest::Construct(labels_reader, storage);
    }
    case capnp::Expression::PARAMETER_LOOKUP: {
      auto pl_reader = reader.getParameterLookup();
      return ParameterLookup::Construct(pl_reader, storage);
    }
    case capnp::Expression::PROPERTY_LOOKUP: {
      auto pl_reader = reader.getPropertyLookup();
      return PropertyLookup::Construct(pl_reader, storage);
    }
    case capnp::Expression::REDUCE: {
      auto reduce_reader = reader.getReduce();
      return Reduce::Construct(reduce_reader, storage);
    }
    case capnp::Expression::SINGLE: {
      auto single_reader = reader.getSingle();
      return Single::Construct(single_reader, storage);
    }
  }
}

// Base Literal.
void BaseLiteral::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto base_literal_builder = expr_builder.initBaseLiteral();
  Save(base_literal_builder);
}

BaseLiteral *BaseLiteral::Construct(capnp::BaseLiteral::Reader &reader,
                                    AstTreeStorage &storage) {
  switch (reader.which()) {
    case capnp::BaseLiteral::PRIMITIVE_LITERAL: {
      auto literal = reader.getPrimitiveLiteral();
      return PrimitiveLiteral::Construct(literal, storage);
    }
    case capnp::BaseLiteral::LIST_LITERAL: {
      auto literal = reader.getListLiteral();
      return ListLiteral::Construct(literal, storage);
    }
    case capnp::BaseLiteral::MAP_LITERAL: {
      auto literal = reader.getMapLiteral();
      return MapLiteral::Construct(literal, storage);
    }
  }
}

// Primitive Literal.
void PrimitiveLiteral::Save(capnp::BaseLiteral::Builder &base_literal_builder) {
  BaseLiteral::Save(base_literal_builder);
  auto primitive_literal_builder = base_literal_builder.initPrimitiveLiteral();
  primitive_literal_builder.setTokenPosition(token_position_);
  auto typed_value_builder = primitive_literal_builder.getValue();
  utils::SaveCapnpTypedValue(value_, typed_value_builder);
}

PrimitiveLiteral *PrimitiveLiteral::Construct(
    capnp::PrimitiveLiteral::Reader &reader, AstTreeStorage &storage) {
  auto typed_value_reader = reader.getValue();
  TypedValue value;
  utils::LoadCapnpTypedValue(value, typed_value_reader);
  return storage.Create<PrimitiveLiteral>(value, reader.getTokenPosition());
}

// List Literal.
void ListLiteral::Save(capnp::BaseLiteral::Builder &base_literal_builder) {
  BaseLiteral::Save(base_literal_builder);
  auto list_literal_builder = base_literal_builder.initListLiteral();
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      list_literal_builder.initElements(elements_.size());
  for (size_t i = 0; i < elements_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    elements_[i]->Save(tree_builder);
  }
}

ListLiteral *ListLiteral::Construct(capnp::ListLiteral::Reader &reader,
                                    AstTreeStorage &storage) {
  auto *list = storage.Create<ListLiteral>();
  for (auto tree_reader : reader.getElements()) {
    auto tree = storage.Load(tree_reader);
    list->elements_.push_back(dynamic_cast<Expression *>(tree));
  }
  return list;
}

// Map Literal.
void MapLiteral::Save(capnp::BaseLiteral::Builder &base_literal_builder) {
  BaseLiteral::Save(base_literal_builder);
  auto map_literal_builder = base_literal_builder.initMapLiteral();
  ::capnp::List<capnp::MapLiteral::Entry>::Builder map_builder =
      map_literal_builder.initElements(elements_.size());
  size_t i = 0;
  for (auto &entry : elements_) {
    auto entry_builder = map_builder[i];
    auto key_builder = entry_builder.getKey();
    key_builder.setFirst(entry.first.first);
    auto storage_property_builder = key_builder.getSecond();
    entry.first.second.Save(storage_property_builder);
    auto value_builder = entry_builder.getValue();
    if (entry.second) entry.second->Save(value_builder);
    ++i;
  }
}

MapLiteral *MapLiteral::Construct(capnp::MapLiteral::Reader &reader,
                                  AstTreeStorage &storage) {
  auto *map = storage.Create<MapLiteral>();
  for (auto entry_reader : reader.getElements()) {
    auto key_pair_reader = entry_reader.getKey();
    auto key_first = key_pair_reader.getFirst().cStr();
    auto storage_property_reader = key_pair_reader.getSecond();
    storage::Property key_second;
    key_second.Load(storage_property_reader);
    auto value_reader = entry_reader.getValue();
    auto value = storage.Load(value_reader);
    auto key = std::make_pair(key_first, key_second);
    // TODO Maybe check for nullptr expression?
    map->elements_.emplace(key, dynamic_cast<Expression *>(value));
  }
  return map;
}

// Binary Operator.
void BinaryOperator::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initBinaryOperator();
  Save(builder);
}

void BinaryOperator::Save(capnp::BinaryOperator::Builder &builder) {
  if (expression1_) {
    auto expr1_builder = builder.getExpression1();
    expression1_->Save(expr1_builder);
  }
  if (expression2_) {
    auto expr2_builder = builder.getExpression2();
    expression2_->Save(expr2_builder);
  }
}

void BinaryOperator::Load(capnp::Tree::Reader &reader,
                          AstTreeStorage &storage) {
  Expression::Load(reader, storage);
  auto bop_reader = reader.getExpression().getBinaryOperator();
  if (bop_reader.hasExpression1()) {
    auto expr1_reader = bop_reader.getExpression1();
    expression1_ = dynamic_cast<Expression *>(storage.Load(expr1_reader));
  }
  if (bop_reader.hasExpression2()) {
    auto expr2_reader = bop_reader.getExpression2();
    expression2_ = dynamic_cast<Expression *>(storage.Load(expr2_reader));
  }
}

BinaryOperator *BinaryOperator::Construct(capnp::BinaryOperator::Reader &reader,
                                          AstTreeStorage &storage) {
  switch (reader.which()) {
    case capnp::BinaryOperator::ADDITION_OPERATOR: {
      auto literal = reader.getAdditionOperator();
      return AdditionOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::AGGREGATION: {
      auto literal = reader.getAggregation();
      return Aggregation::Construct(literal, storage);
    }
    case capnp::BinaryOperator::AND_OPERATOR: {
      auto literal = reader.getAndOperator();
      return AndOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::DIVISION_OPERATOR: {
      auto literal = reader.getDivisionOperator();
      return DivisionOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::EQUAL_OPERATOR: {
      auto literal = reader.getEqualOperator();
      return EqualOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::GREATER_EQUAL_OPERATOR: {
      auto literal = reader.getGreaterEqualOperator();
      return GreaterEqualOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::GREATER_OPERATOR: {
      auto literal = reader.getGreaterOperator();
      return GreaterOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::IN_LIST_OPERATOR: {
      auto literal = reader.getInListOperator();
      return InListOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::LESS_EQUAL_OPERATOR: {
      auto literal = reader.getLessEqualOperator();
      return LessEqualOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::LESS_OPERATOR: {
      auto literal = reader.getLessOperator();
      return LessOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::LIST_MAP_INDEXING_OPERATOR: {
      auto literal = reader.getListMapIndexingOperator();
      return ListMapIndexingOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::MOD_OPERATOR: {
      auto literal = reader.getModOperator();
      return ModOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::MULTIPLICATION_OPERATOR: {
      auto literal = reader.getMultiplicationOperator();
      return MultiplicationOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::NOT_EQUAL_OPERATOR: {
      auto literal = reader.getNotEqualOperator();
      return NotEqualOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::OR_OPERATOR: {
      auto literal = reader.getOrOperator();
      return OrOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::SUBTRACTION_OPERATOR: {
      auto literal = reader.getSubtractionOperator();
      return SubtractionOperator::Construct(literal, storage);
    }
    case capnp::BinaryOperator::XOR_OPERATOR: {
      auto literal = reader.getXorOperator();
      return XorOperator::Construct(literal, storage);
    }
  }
}

void OrOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initOrOperator();
}

OrOperator *OrOperator::Construct(capnp::OrOperator::Reader &,
                                  AstTreeStorage &storage) {
  return storage.Create<OrOperator>();
}

void XorOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initXorOperator();
}

XorOperator *XorOperator::Construct(capnp::XorOperator::Reader &,
                                    AstTreeStorage &storage) {
  return storage.Create<XorOperator>();
}

void AndOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initAndOperator();
}

AndOperator *AndOperator::Construct(capnp::AndOperator::Reader &,
                                    AstTreeStorage &storage) {
  return storage.Create<AndOperator>();
}

void AdditionOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initAdditionOperator();
}

AdditionOperator *AdditionOperator::Construct(capnp::AdditionOperator::Reader &,
                                              AstTreeStorage &storage) {
  return storage.Create<AdditionOperator>();
}

void SubtractionOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initSubtractionOperator();
}

SubtractionOperator *SubtractionOperator::Construct(
    capnp::SubtractionOperator::Reader &, AstTreeStorage &storage) {
  return storage.Create<SubtractionOperator>();
}

void MultiplicationOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initMultiplicationOperator();
}

MultiplicationOperator *MultiplicationOperator::Construct(
    capnp::MultiplicationOperator::Reader &, AstTreeStorage &storage) {
  return storage.Create<MultiplicationOperator>();
}

void DivisionOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initDivisionOperator();
}

DivisionOperator *DivisionOperator::Construct(capnp::DivisionOperator::Reader &,
                                              AstTreeStorage &storage) {
  return storage.Create<DivisionOperator>();
}

void ModOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initModOperator();
}

ModOperator *ModOperator::Construct(capnp::ModOperator::Reader &,
                                    AstTreeStorage &storage) {
  return storage.Create<ModOperator>();
}

void NotEqualOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initNotEqualOperator();
}

NotEqualOperator *NotEqualOperator::Construct(capnp::NotEqualOperator::Reader &,
                                              AstTreeStorage &storage) {
  return storage.Create<NotEqualOperator>();
}

void EqualOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initEqualOperator();
}

EqualOperator *EqualOperator::Construct(capnp::EqualOperator::Reader &,
                                        AstTreeStorage &storage) {
  return storage.Create<EqualOperator>();
}

void LessOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initLessOperator();
}

LessOperator *LessOperator::Construct(capnp::LessOperator::Reader &,
                                      AstTreeStorage &storage) {
  return storage.Create<LessOperator>();
}

void GreaterOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initGreaterOperator();
}

GreaterOperator *GreaterOperator::Construct(capnp::GreaterOperator::Reader &,
                                            AstTreeStorage &storage) {
  return storage.Create<GreaterOperator>();
}

void LessEqualOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initLessEqualOperator();
}

LessEqualOperator *LessEqualOperator::Construct(
    capnp::LessEqualOperator::Reader &, AstTreeStorage &storage) {
  return storage.Create<LessEqualOperator>();
}

void GreaterEqualOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initGreaterEqualOperator();
}

GreaterEqualOperator *GreaterEqualOperator::Construct(
    capnp::GreaterEqualOperator::Reader &, AstTreeStorage &storage) {
  return storage.Create<GreaterEqualOperator>();
}

void InListOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initInListOperator();
}

InListOperator *InListOperator::Construct(capnp::InListOperator::Reader &,
                                          AstTreeStorage &storage) {
  return storage.Create<InListOperator>();
}

void ListMapIndexingOperator::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  builder.initListMapIndexingOperator();
}

ListMapIndexingOperator *ListMapIndexingOperator::Construct(
    capnp::ListMapIndexingOperator::Reader &, AstTreeStorage &storage) {
  return storage.Create<ListMapIndexingOperator>();
}

void Aggregation::Save(capnp::BinaryOperator::Builder &builder) {
  BinaryOperator::Save(builder);
  auto ag_builder = builder.initAggregation();
  switch (op_) {
    case Op::AVG:
      ag_builder.setOp(capnp::Aggregation::Op::AVG);
      break;
    case Op::COLLECT_LIST:
      ag_builder.setOp(capnp::Aggregation::Op::COLLECT_LIST);
      break;
    case Op::COLLECT_MAP:
      ag_builder.setOp(capnp::Aggregation::Op::COLLECT_MAP);
      break;
    case Op::COUNT:
      ag_builder.setOp(capnp::Aggregation::Op::COUNT);
      break;
    case Op::MAX:
      ag_builder.setOp(capnp::Aggregation::Op::MAX);
      break;
    case Op::MIN:
      ag_builder.setOp(capnp::Aggregation::Op::MIN);
      break;
    case Op::SUM:
      ag_builder.setOp(capnp::Aggregation::Op::SUM);
      break;
  }
}

Aggregation *Aggregation::Construct(capnp::Aggregation::Reader &reader,
                                    AstTreeStorage &storage) {
  Op op;
  switch (reader.getOp()) {
    case capnp::Aggregation::Op::AVG:
      op = Op::AVG;
      break;
    case capnp::Aggregation::Op::COLLECT_LIST:
      op = Op::COLLECT_LIST;
      break;
    case capnp::Aggregation::Op::COLLECT_MAP:
      op = Op::COLLECT_MAP;
      break;
    case capnp::Aggregation::Op::COUNT:
      op = Op::COUNT;
      break;
    case capnp::Aggregation::Op::MAX:
      op = Op::MAX;
      break;
    case capnp::Aggregation::Op::MIN:
      op = Op::MIN;
      break;
    case capnp::Aggregation::Op::SUM:
      op = Op::SUM;
      break;
  }
  return storage.Create<Aggregation>(op);
}

// Unary Operator.
void UnaryOperator::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initUnaryOperator();
  Save(builder);
}

void UnaryOperator::Save(capnp::UnaryOperator::Builder &builder) {
  if (expression_) {
    auto expr_builder = builder.getExpression();
    expression_->Save(expr_builder);
  }
}

void UnaryOperator::Load(capnp::Tree::Reader &reader, AstTreeStorage &storage) {
  Expression::Load(reader, storage);
  if (reader.hasExpression()) {
    auto expr_reader =
        reader.getExpression().getUnaryOperator().getExpression();
    expression_ = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
}

UnaryOperator *UnaryOperator::Construct(capnp::UnaryOperator::Reader &reader,
                                        AstTreeStorage &storage) {
  switch (reader.which()) {
    case capnp::UnaryOperator::IS_NULL_OPERATOR: {
      auto op = reader.getIsNullOperator();
      return IsNullOperator::Construct(op, storage);
    }
    case capnp::UnaryOperator::NOT_OPERATOR: {
      auto op = reader.getNotOperator();
      return NotOperator::Construct(op, storage);
    }
    case capnp::UnaryOperator::UNARY_MINUS_OPERATOR: {
      auto op = reader.getUnaryMinusOperator();
      return UnaryMinusOperator::Construct(op, storage);
    }
    case capnp::UnaryOperator::UNARY_PLUS_OPERATOR: {
      auto op = reader.getUnaryPlusOperator();
      return UnaryPlusOperator::Construct(op, storage);
    }
  }
}

// IsNull Operator.
void IsNullOperator::Save(capnp::UnaryOperator::Builder &builder) {
  UnaryOperator::Save(builder);
  builder.initIsNullOperator();
}

IsNullOperator *IsNullOperator::Construct(capnp::IsNullOperator::Reader &,
                                          AstTreeStorage &storage) {
  return storage.Create<IsNullOperator>();
}

// Not Operator.
void NotOperator::Save(capnp::UnaryOperator::Builder &builder) {
  UnaryOperator::Save(builder);
  builder.initNotOperator();
}

NotOperator *NotOperator::Construct(capnp::NotOperator::Reader &,
                                    AstTreeStorage &storage) {
  return storage.Create<NotOperator>();
}

// UnaryPlus Operator.
void UnaryPlusOperator::Save(capnp::UnaryOperator::Builder &builder) {
  UnaryOperator::Save(builder);
  builder.initUnaryPlusOperator();
}

UnaryPlusOperator *UnaryPlusOperator::Construct(
    capnp::UnaryPlusOperator::Reader &, AstTreeStorage &storage) {
  return storage.Create<UnaryPlusOperator>();
}

// UnaryMinus Operator.
void UnaryMinusOperator::Save(capnp::UnaryOperator::Builder &builder) {
  UnaryOperator::Save(builder);
  builder.initUnaryMinusOperator();
}

UnaryMinusOperator *UnaryMinusOperator::Construct(
    capnp::UnaryMinusOperator::Reader &, AstTreeStorage &storage) {
  return storage.Create<UnaryMinusOperator>();
}

// ListSlicing Operator.
void ListSlicingOperator::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initListSlicingOperator();
  Save(builder);
}

void ListSlicingOperator::Save(capnp::ListSlicingOperator::Builder &builder) {
  if (list_) {
    auto list_builder = builder.getList();
    list_->Save(list_builder);
  }
  if (lower_bound_) {
    auto lb_builder = builder.getLowerBound();
    lower_bound_->Save(lb_builder);
  }
  if (upper_bound_) {
    auto up_builder = builder.getUpperBound();
    upper_bound_->Save(up_builder);
  }
}

ListSlicingOperator *ListSlicingOperator::Construct(
    capnp::ListSlicingOperator::Reader &reader, AstTreeStorage &storage) {
  Expression *list = nullptr;
  Expression *upper_bound = nullptr;
  Expression *lower_bound = nullptr;
  if (reader.hasList()) {
    auto list_reader = reader.getList();
    list = dynamic_cast<Expression *>(storage.Load(list_reader));
  }
  if (reader.hasUpperBound()) {
    auto ub_reader = reader.getUpperBound();
    upper_bound = dynamic_cast<Expression *>(storage.Load(ub_reader));
  }
  if (reader.hasLowerBound()) {
    auto lb_reader = reader.getLowerBound();
    lower_bound = dynamic_cast<Expression *>(storage.Load(lb_reader));
  }
  return storage.Create<ListSlicingOperator>(list, lower_bound, upper_bound);
}

// If Operator.
void IfOperator::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initIfOperator();
  Save(builder);
}

void IfOperator::Save(capnp::IfOperator::Builder &builder) {
  auto condition_builder = builder.getCondition();
  condition_->Save(condition_builder);
  auto then_builder = builder.getThenExpression();
  then_expression_->Save(then_builder);
  auto else_builder = builder.getElseExpression();
  else_expression_->Save(else_builder);
}

IfOperator *IfOperator::Construct(capnp::IfOperator::Reader &reader,
                                  AstTreeStorage &storage) {
  auto condition_reader = reader.getCondition();
  auto condition = dynamic_cast<Expression *>(storage.Load(condition_reader));
  auto then_reader = reader.getThenExpression();
  auto then_expression = dynamic_cast<Expression *>(storage.Load(then_reader));
  auto else_reader = reader.getElseExpression();
  auto else_expression = dynamic_cast<Expression *>(storage.Load(else_reader));
  return storage.Create<IfOperator>(condition, then_expression,
                                    else_expression);
}

// All
void All::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initAll();
  Save(builder);
}

void All::Save(capnp::All::Builder &builder) {
  auto identifier_builder = builder.getIdentifier();
  identifier_->Save(identifier_builder);
  auto expr_builder = builder.getListExpression();
  list_expression_->Save(expr_builder);
  auto where_builder = builder.getWhere();
  where_->Save(where_builder);
}

All *All::Construct(capnp::All::Reader &reader, AstTreeStorage &storage) {
  auto id_reader = reader.getIdentifier();
  auto identifier = dynamic_cast<Identifier *>(storage.Load(id_reader));
  auto expr_reader = reader.getListExpression();
  auto list_expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  auto where_reader = reader.getWhere();
  auto where = dynamic_cast<Where *>(storage.Load(where_reader));
  return storage.Create<All>(identifier, list_expression, where);
}

// Function
void Function::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initFunction();
  Save(builder);
}

void Function::Save(capnp::Function::Builder &builder) {
  builder.setFunctionName(function_name_);
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder.initArguments(arguments_.size());
  for (size_t i = 0; i < arguments_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    arguments_[i]->Save(tree_builder);
  }
}

Function *Function::Construct(capnp::Function::Reader &reader,
                              AstTreeStorage &storage) {
  auto name = reader.getFunctionName().cStr();
  std::vector<Expression *> arguments;
  for (auto tree_reader : reader.getArguments()) {
    auto tree = storage.Load(tree_reader);
    arguments.push_back(dynamic_cast<Expression *>(tree));
  }
  return storage.Create<Function>(name, arguments);
}

// Identifier
void Identifier::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initIdentifier();
  Save(builder);
}

void Identifier::Save(capnp::Identifier::Builder &builder) {
  builder.setName(name_);
  builder.setUserDeclared(user_declared_);
}

Identifier *Identifier::Construct(capnp::Identifier::Reader &reader,
                                  AstTreeStorage &storage) {
  auto name = reader.getName().cStr();
  auto user_declared = reader.getUserDeclared();
  return storage.Create<Identifier>(name, user_declared);
}

// LabelsTest
void LabelsTest::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initLabelsTest();
  Save(builder);
}

void LabelsTest::Save(capnp::LabelsTest::Builder &builder) {
  if (expression_) {
    auto expr_builder = builder.initExpression();
    expression_->Save(expr_builder);
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder.initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}

LabelsTest *LabelsTest::Construct(capnp::LabelsTest::Reader &reader,
                                  AstTreeStorage &storage) {
  Expression *expression = nullptr;
  if (reader.hasExpression()) {
    auto expr_reader = reader.getExpression();
    expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
  std::vector<storage::Label> labels;
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    labels.push_back(label);
  }
  return storage.Create<LabelsTest>(expression, labels);
}

// ParameterLookup
void ParameterLookup::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initParameterLookup();
  Save(builder);
}

void ParameterLookup::Save(capnp::ParameterLookup::Builder &builder) {
  builder.setTokenPosition(token_position_);
}

ParameterLookup *ParameterLookup::Construct(
    capnp::ParameterLookup::Reader &reader, AstTreeStorage &storage) {
  auto token_position = reader.getTokenPosition();
  return storage.Create<ParameterLookup>(token_position);
}

// PropertyLookup
void PropertyLookup::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initPropertyLookup();
  Save(builder);
}

void PropertyLookup::Save(capnp::PropertyLookup::Builder &builder) {
  if (expression_) {
    auto expr_builder = builder.initExpression();
    expression_->Save(expr_builder);
  }
  builder.setPropertyName(property_name_);
  auto storage_property_builder = builder.initProperty();
  property_.Save(storage_property_builder);
}

PropertyLookup *PropertyLookup::Construct(capnp::PropertyLookup::Reader &reader,
                                          AstTreeStorage &storage) {
  Expression *expression = nullptr;
  if (reader.hasExpression()) {
    auto expr_reader = reader.getExpression();
    expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
  auto property_name = reader.getPropertyName().cStr();
  auto storage_property_reader = reader.getProperty();
  storage::Property property;
  property.Load(storage_property_reader);
  return storage.Create<PropertyLookup>(expression, property_name, property);
}

// Reduce
void Reduce::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initReduce();
  Save(builder);
}

void Reduce::Save(capnp::Reduce::Builder &builder) {
  auto acc_builder = builder.initAccumulator();
  accumulator_->Save(acc_builder);
  auto init_builder = builder.initInitializer();
  initializer_->Save(init_builder);
  auto id_builder = builder.initIdentifier();
  identifier_->Save(id_builder);
  auto list_builder = builder.initList();
  list_->Save(list_builder);
  auto expr_builder = builder.initExpression();
  expression_->Save(expr_builder);
}

Reduce *Reduce::Construct(capnp::Reduce::Reader &reader,
                          AstTreeStorage &storage) {
  auto acc_reader = reader.getAccumulator();
  auto accumulator = dynamic_cast<Identifier *>(storage.Load(acc_reader));
  auto init_reader = reader.getInitializer();
  auto initializer = dynamic_cast<Expression *>(storage.Load(init_reader));
  auto id_reader = reader.getIdentifier();
  auto identifier = dynamic_cast<Identifier *>(storage.Load(id_reader));
  auto list_reader = reader.getList();
  auto list = dynamic_cast<Expression *>(storage.Load(list_reader));
  auto expr_reader = reader.getExpression();
  auto expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  return storage.Create<Reduce>(accumulator, initializer, identifier, list,
                                expression);
}

// Single
void Single::Save(capnp::Expression::Builder &expr_builder) {
  Expression::Save(expr_builder);
  auto builder = expr_builder.initSingle();
  Save(builder);
}

void Single::Save(capnp::Single::Builder &builder) {
  auto where_builder = builder.initWhere();
  where_->Save(where_builder);
  auto id_builder = builder.initIdentifier();
  identifier_->Save(id_builder);
  auto expr_builder = builder.initListExpression();
  list_expression_->Save(expr_builder);
}

Single *Single::Construct(capnp::Single::Reader &reader,
                          AstTreeStorage &storage) {
  auto id_reader = reader.getIdentifier();
  auto identifier = dynamic_cast<Identifier *>(storage.Load(id_reader));
  auto list_reader = reader.getListExpression();
  auto list_expression = dynamic_cast<Expression *>(storage.Load(list_reader));
  auto where_reader = reader.getWhere();
  auto where = dynamic_cast<Where *>(storage.Load(where_reader));
  return storage.Create<Single>(identifier, list_expression, where);
}

// Where
void Where::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto builder = tree_builder.initWhere();
  Save(builder);
}

void Where::Save(capnp::Where::Builder &builder) {
  if (expression_) {
    auto expr_builder = builder.initExpression();
    expression_->Save(expr_builder);
  }
}

Where *Where::Construct(capnp::Where::Reader &reader, AstTreeStorage &storage) {
  Expression *expression = nullptr;
  if (reader.hasExpression()) {
    auto expr_reader = reader.getExpression();
    expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
  return storage.Create<Where>(expression);
}

// Clause.
void Clause::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto clause_builder = tree_builder.initClause();
  Save(clause_builder);
}

Clause *Clause::Construct(capnp::Clause::Reader &reader,
                          AstTreeStorage &storage) {
  switch (reader.which()) {
    case capnp::Clause::CREATE: {
      auto create_reader = reader.getCreate();
      return Create::Construct(create_reader, storage);
    }
    case capnp::Clause::CREATE_INDEX: {
      auto ci_reader = reader.getCreateIndex();
      return CreateIndex::Construct(ci_reader, storage);
    }
    case capnp::Clause::DELETE: {
      auto del_reader = reader.getDelete();
      return Delete::Construct(del_reader, storage);
    }
    case capnp::Clause::MATCH: {
      auto match_reader = reader.getMatch();
      return Match::Construct(match_reader, storage);
    }
    case capnp::Clause::MERGE: {
      auto merge_reader = reader.getMerge();
      return Merge::Construct(merge_reader, storage);
    }
    case capnp::Clause::REMOVE_LABELS: {
      auto rl_reader = reader.getRemoveLabels();
      return RemoveLabels::Construct(rl_reader, storage);
    }
    case capnp::Clause::REMOVE_PROPERTY: {
      auto rp_reader = reader.getRemoveProperty();
      return RemoveProperty::Construct(rp_reader, storage);
    }
    case capnp::Clause::RETURN: {
      auto ret_reader = reader.getReturn();
      return Return::Construct(ret_reader, storage);
    }
    case capnp::Clause::SET_LABELS: {
      auto sl_reader = reader.getSetLabels();
      return SetLabels::Construct(sl_reader, storage);
      break;
    }
    case capnp::Clause::SET_PROPERTY: {
      auto sp_reader = reader.getSetProperty();
      return SetProperty::Construct(sp_reader, storage);
    }
    case capnp::Clause::SET_PROPERTIES: {
      auto sp_reader = reader.getSetProperties();
      return SetProperties::Construct(sp_reader, storage);
    }
    case capnp::Clause::UNWIND: {
      auto unwind_reader = reader.getUnwind();
      return Unwind::Construct(unwind_reader, storage);
    }
    case capnp::Clause::WITH: {
      auto with_reader = reader.getWith();
      return With::Construct(with_reader, storage);
    }
  }
}

// Create.
void Create::Save(capnp::Clause::Builder &builder) {
  Clause::Save(builder);
  auto create_builder = builder.initCreate();
  Create::Save(create_builder);
}

void Create::Save(capnp::Create::Builder &builder) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder.initPatterns(patterns_.size());
  for (size_t i = 0; i < patterns_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    patterns_[i]->Save(tree_builder);
  }
}

Create *Create::Construct(capnp::Create::Reader &reader,
                          AstTreeStorage &storage) {
  std::vector<Pattern *> patterns;
  for (auto tree_reader : reader.getPatterns()) {
    auto tree = storage.Load(tree_reader);
    patterns.push_back(dynamic_cast<Pattern *>(tree));
  }
  return storage.Create<Create>(patterns);
}

// CreateIndex.
void CreateIndex::Save(capnp::Clause::Builder &builder) {
  Clause::Save(builder);
  auto create_builder = builder.initCreateIndex();
  CreateIndex::Save(create_builder);
}

void CreateIndex::Save(capnp::CreateIndex::Builder &builder) {
  auto label_builder = builder.getLabel();
  label_.Save(label_builder);
  auto property_builder = builder.getProperty();
  property_.Save(property_builder);
}

CreateIndex *CreateIndex::Construct(capnp::CreateIndex::Reader &reader,
                                    AstTreeStorage &storage) {
  auto label_reader = reader.getLabel();
  storage::Label label;
  label.Load(label_reader);
  auto property_reader = reader.getProperty();
  storage::Property property;
  property.Load(property_reader);
  return storage.Create<CreateIndex>(label, property);
}

// Delete.
void Delete::Save(capnp::Clause::Builder &builder) {
  Clause::Save(builder);
  auto del_builder = builder.initDelete();
  Delete::Save(del_builder);
}

void Delete::Save(capnp::Delete::Builder &builder) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder.initExpressions(expressions_.size());
  for (size_t i = 0; i < expressions_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    expressions_[i]->Save(tree_builder);
  }
  builder.setDetach(detach_);
}

Delete *Delete::Construct(capnp::Delete::Reader &reader,
                          AstTreeStorage &storage) {
  std::vector<Expression *> expressions;
  for (auto tree_reader : reader.getExpressions()) {
    auto tree = storage.Load(tree_reader);
    expressions.push_back(dynamic_cast<Expression *>(tree));
  }
  auto detach = reader.getDetach();
  return storage.Create<Delete>(detach, expressions);
}

// Match.
void Match::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initMatch();
  Match::Save(builder);
}

void Match::Save(capnp::Match::Builder &builder) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder.initPatterns(patterns_.size());
  for (size_t i = 0; i < patterns_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    patterns_[i]->Save(tree_builder);
  }

  if (where_) {
    auto where_builder = builder.initWhere();
    where_->Save(where_builder);
  }
  builder.setOptional(optional_);
}

Match *Match::Construct(capnp::Match::Reader &reader, AstTreeStorage &storage) {
  std::vector<Pattern *> patterns;
  for (auto tree_reader : reader.getPatterns()) {
    auto tree = storage.Load(tree_reader);
    patterns.push_back(dynamic_cast<Pattern *>(tree));
  }
  Where *where = nullptr;
  if (reader.hasWhere()) {
    auto where_reader = reader.getWhere();
    where = dynamic_cast<Where *>(storage.Load(where_reader));
  }
  auto optional = reader.getOptional();
  return storage.Create<Match>(optional, where, patterns);
}

// Merge.
void Merge::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initMerge();
  Merge::Save(builder);
}

void Merge::Save(capnp::Merge::Builder &builder) {
  ::capnp::List<capnp::Tree>::Builder match_builder =
      builder.initOnMatch(on_match_.size());
  for (size_t i = 0; i < on_match_.size(); ++i) {
    auto tree_builder = match_builder[i];
    on_match_[i]->Save(tree_builder);
  }

  ::capnp::List<capnp::Tree>::Builder create_builder =
      builder.initOnCreate(on_create_.size());
  for (size_t i = 0; i < on_create_.size(); ++i) {
    auto tree_builder = create_builder[i];
    on_create_[i]->Save(tree_builder);
  }

  if (pattern_) {
    auto pattern_builder = builder.getPattern();
    pattern_->Save(pattern_builder);
  }
}

Merge *Merge::Construct(capnp::Merge::Reader &reader, AstTreeStorage &storage) {
  std::vector<Clause *> on_match;
  for (auto tree_reader : reader.getOnMatch()) {
    auto tree = storage.Load(tree_reader);
    on_match.push_back(dynamic_cast<Clause *>(tree));
  }

  std::vector<Clause *> on_create;
  for (auto tree_reader : reader.getOnCreate()) {
    auto tree = storage.Load(tree_reader);
    on_create.push_back(dynamic_cast<Clause *>(tree));
  }
  Pattern *pattern = nullptr;
  if (reader.hasPattern()) {
    auto pattern_reader = reader.getPattern();
    pattern = dynamic_cast<Pattern *>(storage.Load(pattern_reader));
  }
  return storage.Create<Merge>(pattern, on_match, on_create);
}

// RemoveLabels.
void RemoveLabels::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initRemoveLabels();
  RemoveLabels::Save(builder);
}

void RemoveLabels::Save(capnp::RemoveLabels::Builder &builder) {
  if (identifier_) {
    auto id_builder = builder.getIdentifier();
    identifier_->Save(id_builder);
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder.initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}

RemoveLabels *RemoveLabels::Construct(capnp::RemoveLabels::Reader &reader,
                                      AstTreeStorage &storage) {
  Identifier *identifier = nullptr;
  if (reader.hasIdentifier()) {
    auto id_reader = reader.getIdentifier();
    identifier = dynamic_cast<Identifier *>(storage.Load(id_reader));
  }
  std::vector<storage::Label> labels;
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    labels.push_back(label);
  }
  return storage.Create<RemoveLabels>(identifier, labels);
}

// RemoveProperty.
void RemoveProperty::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initRemoveProperty();
  RemoveProperty::Save(builder);
}

void RemoveProperty::Save(capnp::RemoveProperty::Builder &builder) {
  if (property_lookup_) {
    auto pl_builder = builder.getPropertyLookup();
    property_lookup_->Save(pl_builder);
  }
}

RemoveProperty *RemoveProperty::Construct(capnp::RemoveProperty::Reader &reader,
                                          AstTreeStorage &storage) {
  PropertyLookup *property_lookup;
  if (reader.hasPropertyLookup()) {
    auto pl_reader = reader.getPropertyLookup();
    property_lookup = dynamic_cast<PropertyLookup *>(storage.Load(pl_reader));
  }
  return storage.Create<RemoveProperty>(property_lookup);
}

// Return.
void Return::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initReturn();
  Return::Save(builder);
}

void SaveReturnBody(capnp::ReturnBody::Builder &rb_builder, ReturnBody &body) {
  rb_builder.setDistinct(body.distinct);
  rb_builder.setAllIdentifiers(body.all_identifiers);

  ::capnp::List<capnp::Tree>::Builder named_expressions =
      rb_builder.initNamedExpressions(body.named_expressions.size());
  for (size_t i = 0; i < body.named_expressions.size(); ++i) {
    auto tree_builder = named_expressions[i];
    body.named_expressions[i]->Save(tree_builder);
  }

  ::capnp::List<capnp::ReturnBody::Pair>::Builder order_by =
      rb_builder.initOrderBy(body.order_by.size());
  for (size_t i = 0; i < body.order_by.size(); ++i) {
    auto pair_builder = order_by[i];
    auto ordering = body.order_by[i].first == Ordering::ASC
                        ? capnp::Ordering::ASC
                        : capnp::Ordering::DESC;
    pair_builder.setOrdering(ordering);
    auto tree_builder = pair_builder.getExpression();
    body.order_by[i].second->Save(tree_builder);
  }

  if (body.skip) {
    auto skip_builder = rb_builder.getSkip();
    body.skip->Save(skip_builder);
  }
  if (body.limit) {
    auto limit_builder = rb_builder.getLimit();
    body.limit->Save(limit_builder);
  }
}

void Return::Save(capnp::Return::Builder &builder) {
  auto rb_builder = builder.initReturnBody();
  SaveReturnBody(rb_builder, body_);
}

void LoadReturnBody(capnp::ReturnBody::Reader &rb_reader, ReturnBody &body,
                    AstTreeStorage &storage) {
  body.distinct = rb_reader.getDistinct();
  body.all_identifiers = rb_reader.getAllIdentifiers();

  for (auto tree_reader : rb_reader.getNamedExpressions()) {
    auto tree = storage.Load(tree_reader);
    body.named_expressions.push_back(dynamic_cast<NamedExpression *>(tree));
  }

  for (auto pair_reader : rb_reader.getOrderBy()) {
    auto ordering = pair_reader.getOrdering() == capnp::Ordering::ASC
                        ? Ordering::ASC
                        : Ordering::DESC;
    auto tree_reader = pair_reader.getExpression();
    // TODO Check if expression is null?
    auto tree = dynamic_cast<Expression *>(storage.Load(tree_reader));
    body.order_by.push_back(std::make_pair(ordering, tree));
  }

  if (rb_reader.hasSkip()) {
    auto skip_reader = rb_reader.getSkip();
    body.skip = dynamic_cast<Expression *>(storage.Load(skip_reader));
  }
  if (rb_reader.hasLimit()) {
    auto limit_reader = rb_reader.getLimit();
    body.limit = dynamic_cast<Expression *>(storage.Load(limit_reader));
  }
}

Return *Return::Construct(capnp::Return::Reader &reader,
                          AstTreeStorage &storage) {
  auto rb_reader = reader.getReturnBody();
  ReturnBody body;
  LoadReturnBody(rb_reader, body, storage);
  return storage.Create<Return>(body);
}

// SetLabels.
void SetLabels::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initSetLabels();
  SetLabels::Save(builder);
}

void SetLabels::Save(capnp::SetLabels::Builder &builder) {
  if (identifier_) {
    auto id_builder = builder.getIdentifier();
    identifier_->Save(id_builder);
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder.initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}

SetLabels *SetLabels::Construct(capnp::SetLabels::Reader &reader,
                                AstTreeStorage &storage) {
  Identifier *identifier = nullptr;
  if (reader.hasIdentifier()) {
    auto id_reader = reader.getIdentifier();
    identifier = dynamic_cast<Identifier *>(storage.Load(id_reader));
  }
  std::vector<storage::Label> labels;
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    labels.push_back(label);
  }
  return storage.Create<SetLabels>(identifier, labels);
}

// SetProperty.
void SetProperty::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initSetProperty();
  SetProperty::Save(builder);
}

void SetProperty::Save(capnp::SetProperty::Builder &builder) {
  if (property_lookup_) {
    auto pl_builder = builder.getPropertyLookup();
    property_lookup_->Save(pl_builder);
  }
  if (expression_) {
    auto expr_builder = builder.getExpression();
    expression_->Save(expr_builder);
  }
}

SetProperty *SetProperty::Construct(capnp::SetProperty::Reader &reader,
                                    AstTreeStorage &storage) {
  PropertyLookup *property_lookup = nullptr;
  if (reader.hasPropertyLookup()) {
    auto pl_reader = reader.getPropertyLookup();
    property_lookup = dynamic_cast<PropertyLookup *>(storage.Load(pl_reader));
  }
  Expression *expression;
  if (reader.hasExpression()) {
    auto expr_reader = reader.getExpression();
    expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
  return storage.Create<SetProperty>(property_lookup, expression);
}

// SetProperties.
void SetProperties::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initSetProperties();
  SetProperties::Save(builder);
}

void SetProperties::Save(capnp::SetProperties::Builder &builder) {
  if (identifier_) {
    auto id_builder = builder.getIdentifier();
    identifier_->Save(id_builder);
  }
  if (expression_) {
    auto expr_builder = builder.getExpression();
    expression_->Save(expr_builder);
  }
  builder.setUpdate(update_);
}

SetProperties *SetProperties::Construct(capnp::SetProperties::Reader &reader,
                                        AstTreeStorage &storage) {
  Identifier *identifier = nullptr;
  if (reader.hasIdentifier()) {
    auto id_reader = reader.getIdentifier();
    identifier = dynamic_cast<Identifier *>(storage.Load(id_reader));
  }
  Expression *expression = nullptr;
  if (reader.hasExpression()) {
    auto expr_reader = reader.getExpression();
    expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
  auto update = reader.getUpdate();
  return storage.Create<SetProperties>(identifier, expression, update);
}

// Unwind.
void Unwind::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initUnwind();
  Unwind::Save(builder);
}

void Unwind::Save(capnp::Unwind::Builder &builder) {
  if (named_expression_) {
    auto expr_builder = builder.getNamedExpression();
    named_expression_->Save(expr_builder);
  }
}

Unwind *Unwind::Construct(capnp::Unwind::Reader &reader,
                          AstTreeStorage &storage) {
  NamedExpression *expression = nullptr;
  if (reader.hasNamedExpression()) {
    auto expr_reader = reader.getNamedExpression();
    expression = dynamic_cast<NamedExpression *>(storage.Load(expr_reader));
  }
  return storage.Create<Unwind>(expression);
}

// With.
void With::Save(capnp::Clause::Builder &clause_builder) {
  Clause::Save(clause_builder);
  auto builder = clause_builder.initWith();
  With::Save(builder);
}

void With::Save(capnp::With::Builder &builder) {
  if (where_) {
    auto where_builder = builder.getWhere();
    where_->Save(where_builder);
  }
  auto rb_builder = builder.initReturnBody();
  SaveReturnBody(rb_builder, body_);
}

With *With::Construct(capnp::With::Reader &reader, AstTreeStorage &storage) {
  Where *where = nullptr;
  if (reader.hasWhere()) {
    auto where_reader = reader.getWhere();
    where = dynamic_cast<Where *>(storage.Load(where_reader));
  }
  auto rb_reader = reader.getReturnBody();
  ReturnBody body;
  LoadReturnBody(rb_reader, body, storage);
  return storage.Create<With>(body, where);
}

// CypherUnion
void CypherUnion::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto builder = tree_builder.initCypherUnion();
  Save(builder);
}

void CypherUnion::Save(capnp::CypherUnion::Builder &builder) {
  if (single_query_) {
    auto sq_builder = builder.initSingleQuery();
    single_query_->Save(sq_builder);
  }
  builder.setDistinct(distinct_);
  ::capnp::List<capnp::Symbol>::Builder symbol_builders =
      builder.initUnionSymbols(union_symbols_.size());
  for (size_t i = 0; i < union_symbols_.size(); ++i) {
    auto symbol_builder = symbol_builders[i];
    union_symbols_[i].Save(symbol_builder);
  }
}

CypherUnion *CypherUnion::Construct(capnp::CypherUnion::Reader &reader,
                                    AstTreeStorage &storage) {
  SingleQuery *single_query = nullptr;
  if (reader.hasSingleQuery()) {
    auto sq_reader = reader.getSingleQuery();
    single_query = dynamic_cast<SingleQuery *>(storage.Load(sq_reader));
  }
  auto distinct = reader.getDistinct();
  std::vector<Symbol> symbols;
  for (auto symbol_reader : reader.getUnionSymbols()) {
    Symbol symbol;
    symbol.Load(symbol_reader);
    symbols.push_back(symbol);
  }
  return storage.Create<CypherUnion>(distinct, single_query, symbols);
}

// NamedExpression
void NamedExpression::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto builder = tree_builder.initNamedExpression();
  Save(builder);
}

void NamedExpression::Save(capnp::NamedExpression::Builder &builder) {
  builder.setName(name_);
  builder.setTokenPosition(token_position_);
  if (expression_) {
    auto expr_builder = builder.getExpression();
    expression_->Save(expr_builder);
  }
}

NamedExpression *NamedExpression::Construct(
    capnp::NamedExpression::Reader &reader, AstTreeStorage &storage) {
  auto name = reader.getName().cStr();
  auto token_position = reader.getTokenPosition();
  Expression *expression = nullptr;
  if (reader.hasExpression()) {
    auto expr_reader = reader.getExpression();
    expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
  return storage.Create<NamedExpression>(name, expression, token_position);
}

// Pattern
void Pattern::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto builder = tree_builder.initPattern();
  Save(builder);
}

void Pattern::Save(capnp::Pattern::Builder &builder) {
  if (identifier_) {
    auto id_builder = builder.getIdentifier();
    identifier_->Save(id_builder);
  }
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder.initAtoms(atoms_.size());
  for (size_t i = 0; i < atoms_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    atoms_[i]->Save(tree_builder);
  }
}

Pattern *Pattern::Construct(capnp::Pattern::Reader &reader,
                            AstTreeStorage &storage) {
  auto pattern = storage.Create<Pattern>();
  if (reader.hasIdentifier()) {
    auto id_reader = reader.getIdentifier();
    pattern->identifier_ = dynamic_cast<Identifier *>(storage.Load(id_reader));
  }
  for (auto tree_reader : reader.getAtoms()) {
    auto tree = storage.Load(tree_reader);
    pattern->atoms_.push_back(dynamic_cast<PatternAtom *>(tree));
  }
  return pattern;
}

// PatternAtom.
void PatternAtom::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto pattern_builder = tree_builder.initPatternAtom();
  Save(pattern_builder);
}

void PatternAtom::Save(capnp::PatternAtom::Builder &builder) {
  if (identifier_) {
    auto id_builder = builder.getIdentifier();
    identifier_->Save(id_builder);
  }
}

PatternAtom *PatternAtom::Construct(capnp::PatternAtom::Reader &reader,
                                    AstTreeStorage &storage) {
  switch (reader.which()) {
    case capnp::PatternAtom::EDGE_ATOM: {
      auto edge_reader = reader.getEdgeAtom();
      return EdgeAtom::Construct(edge_reader, storage);
    }
    case capnp::PatternAtom::NODE_ATOM: {
      auto node_reader = reader.getNodeAtom();
      return NodeAtom::Construct(node_reader, storage);
    }
  }
}

void PatternAtom::Load(capnp::Tree::Reader &reader, AstTreeStorage &storage) {
  Tree::Load(reader, storage);
  auto pa_reader = reader.getPatternAtom();
  if (pa_reader.hasIdentifier()) {
    auto id_reader = pa_reader.getIdentifier();
    identifier_ = dynamic_cast<Identifier *>(storage.Load(id_reader));
  }
}

// NodeAtom
void NodeAtom::Save(capnp::PatternAtom::Builder &pattern_builder) {
  PatternAtom::Save(pattern_builder);
  auto builder = pattern_builder.initNodeAtom();
  Save(builder);
}

void NodeAtom::Save(capnp::NodeAtom::Builder &builder) {
  ::capnp::List<capnp::NodeAtom::Entry>::Builder map_builder =
      builder.initProperties(properties_.size());
  size_t i = 0;
  for (auto &entry : properties_) {
    auto entry_builder = map_builder[i];
    auto key_builder = entry_builder.getKey();
    key_builder.setFirst(entry.first.first);
    auto storage_property_builder = key_builder.getSecond();
    entry.first.second.Save(storage_property_builder);
    auto value_builder = entry_builder.getValue();
    if (entry.second) entry.second->Save(value_builder);
    ++i;
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder.initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}

NodeAtom *NodeAtom::Construct(capnp::NodeAtom::Reader &reader,
                              AstTreeStorage &storage) {
  auto *atom = storage.Create<NodeAtom>();
  for (auto entry_reader : reader.getProperties()) {
    auto key_pair_reader = entry_reader.getKey();
    auto key_first = key_pair_reader.getFirst().cStr();
    auto storage_property_reader = key_pair_reader.getSecond();
    storage::Property property;
    property.Load(storage_property_reader);
    auto value_reader = entry_reader.getValue();
    auto value = storage.Load(value_reader);
    auto key = std::make_pair(key_first, property);
    // TODO Maybe check if expression is nullptr?
    atom->properties_.emplace(key, dynamic_cast<Expression *>(value));
  }
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    atom->labels_.push_back(label);
  }
  return atom;
}

// EdgeAtom
void EdgeAtom::Save(capnp::PatternAtom::Builder &pattern_builder) {
  PatternAtom::Save(pattern_builder);
  auto builder = pattern_builder.initEdgeAtom();
  Save(builder);
}

void SaveLambda(query::EdgeAtom::Lambda &lambda,
                capnp::EdgeAtom::Lambda::Builder &builder) {
  if (lambda.inner_edge) {
    auto ie_builder = builder.getInnerEdge();
    lambda.inner_edge->Save(ie_builder);
  }
  if (lambda.inner_node) {
    auto in_builder = builder.getInnerNode();
    lambda.inner_node->Save(in_builder);
  }
  if (lambda.expression) {
    auto expr_builder = builder.getExpression();
    lambda.expression->Save(expr_builder);
  }
}

void EdgeAtom::Save(capnp::EdgeAtom::Builder &builder) {
  switch (type_) {
    case Type::BREADTH_FIRST:
      builder.setType(capnp::EdgeAtom::Type::BREADTH_FIRST);
      break;
    case Type::DEPTH_FIRST:
      builder.setType(capnp::EdgeAtom::Type::DEPTH_FIRST);
      break;
    case Type::SINGLE:
      builder.setType(capnp::EdgeAtom::Type::SINGLE);
      break;
    case Type::WEIGHTED_SHORTEST_PATH:
      builder.setType(capnp::EdgeAtom::Type::WEIGHTED_SHORTEST_PATH);
      break;
  }

  switch (direction_) {
    case Direction::BOTH:
      builder.setDirection(capnp::EdgeAtom::Direction::BOTH);
      break;
    case Direction::IN:
      builder.setDirection(capnp::EdgeAtom::Direction::IN);
      break;
    case Direction::OUT:
      builder.setDirection(capnp::EdgeAtom::Direction::OUT);
      break;
  }

  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder.initEdgeTypes(edge_types_.size());
  for (size_t i = 0; i < edge_types_.size(); ++i) {
    auto common_builder = common_builders[i];
    edge_types_[i].Save(common_builder);
  }

  ::capnp::List<capnp::EdgeAtom::Entry>::Builder map_builder =
      builder.initProperties(properties_.size());
  size_t i = 0;
  for (auto &entry : properties_) {
    auto entry_builder = map_builder[i];
    auto key_builder = entry_builder.getKey();
    key_builder.setFirst(entry.first.first);
    auto storage_property_builder = key_builder.getSecond();
    entry.first.second.Save(storage_property_builder);
    auto value_builder = entry_builder.getValue();
    if (entry.second) entry.second->Save(value_builder);
    ++i;
  }

  if (lower_bound_) {
    auto lb_builder = builder.getLowerBound();
    lower_bound_->Save(lb_builder);
  }
  if (upper_bound_) {
    auto ub_builder = builder.getUpperBound();
    upper_bound_->Save(ub_builder);
  }

  auto filter_builder = builder.initFilterLambda();
  SaveLambda(filter_lambda_, filter_builder);
  auto weight_builder = builder.initWeightLambda();
  SaveLambda(weight_lambda_, weight_builder);

  if (total_weight_) {
    auto total_weight_builder = builder.getTotalWeight();
    total_weight_->Save(total_weight_builder);
  }
}

void LoadLambda(capnp::EdgeAtom::Lambda::Reader &reader,
                query::EdgeAtom::Lambda &lambda, AstTreeStorage &storage) {
  if (reader.hasInnerEdge()) {
    auto ie_reader = reader.getInnerEdge();
    lambda.inner_edge = dynamic_cast<Identifier *>(storage.Load(ie_reader));
  }
  if (reader.hasInnerNode()) {
    auto in_reader = reader.getInnerNode();
    lambda.inner_node = dynamic_cast<Identifier *>(storage.Load(in_reader));
  }
  if (reader.hasExpression()) {
    auto expr_reader = reader.getExpression();
    lambda.expression = dynamic_cast<Expression *>(storage.Load(expr_reader));
  }
}

EdgeAtom *EdgeAtom::Construct(capnp::EdgeAtom::Reader &reader,
                              AstTreeStorage &storage) {
  auto *atom = storage.Create<EdgeAtom>();

  switch (reader.getType()) {
    case capnp::EdgeAtom::Type::BREADTH_FIRST:
      atom->type_ = Type::BREADTH_FIRST;
      break;
    case capnp::EdgeAtom::Type::DEPTH_FIRST:
      atom->type_ = Type::DEPTH_FIRST;
      break;
    case capnp::EdgeAtom::Type::SINGLE:
      atom->type_ = Type::SINGLE;
      break;
    case capnp::EdgeAtom::Type::WEIGHTED_SHORTEST_PATH:
      atom->type_ = Type::WEIGHTED_SHORTEST_PATH;
      break;
  }

  switch (reader.getDirection()) {
    case capnp::EdgeAtom::Direction::BOTH:
      atom->direction_ = Direction::BOTH;
      break;
    case capnp::EdgeAtom::Direction::IN:
      atom->direction_ = Direction::IN;
      break;
    case capnp::EdgeAtom::Direction::OUT:
      atom->direction_ = Direction::OUT;
      break;
  }

  if (reader.hasTotalWeight()) {
    auto id_reader = reader.getTotalWeight();
    atom->total_weight_ = dynamic_cast<Identifier *>(storage.Load(id_reader));
  }
  if (reader.hasLowerBound()) {
    auto lb_reader = reader.getLowerBound();
    atom->lower_bound_ = dynamic_cast<Expression *>(storage.Load(lb_reader));
  }
  if (reader.hasUpperBound()) {
    auto ub_reader = reader.getUpperBound();
    atom->upper_bound_ = dynamic_cast<Expression *>(storage.Load(ub_reader));
  }
  auto filter_reader = reader.getFilterLambda();
  LoadLambda(filter_reader, atom->filter_lambda_, storage);
  auto weight_reader = reader.getWeightLambda();
  LoadLambda(weight_reader, atom->weight_lambda_, storage);

  for (auto entry_reader : reader.getProperties()) {
    auto key_pair_reader = entry_reader.getKey();
    auto key_first = key_pair_reader.getFirst().cStr();
    auto storage_property_reader = key_pair_reader.getSecond();
    storage::Property property;
    property.Load(storage_property_reader);
    auto value_reader = entry_reader.getValue();
    auto value = storage.Load(value_reader);
    auto key = std::make_pair(key_first, property);
    // TODO Check if expression is null?
    atom->properties_.emplace(key, dynamic_cast<Expression *>(value));
  }

  for (auto edge_type_reader : reader.getEdgeTypes()) {
    storage::EdgeType edge_type;
    edge_type.Load(edge_type_reader);
    atom->edge_types_.push_back(edge_type);
  }
  return atom;
}

// Query
void Query::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto builder = tree_builder.initQuery();
  Save(builder);
}

void Query::Save(capnp::Query::Builder &builder) {
  if (single_query_) {
    auto sq_builder = builder.initSingleQuery();
    single_query_->Save(sq_builder);
  }
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder.initCypherUnions(cypher_unions_.size());
  for (size_t i = 0; i < cypher_unions_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    cypher_unions_[i]->Save(tree_builder);
  }
}

Query *Query::Construct(capnp::Query::Reader &reader, AstTreeStorage &storage) {
  auto query = storage.query();
  if (reader.hasSingleQuery()) {
    auto sq_reader = reader.getSingleQuery();
    query->single_query_ = dynamic_cast<SingleQuery *>(storage.Load(sq_reader));
  }
  for (auto tree_reader : reader.getCypherUnions()) {
    auto tree = storage.Load(tree_reader);
    query->cypher_unions_.push_back(dynamic_cast<CypherUnion *>(tree));
  }
  return query;
}

// SingleQuery
void SingleQuery::Save(capnp::Tree::Builder &tree_builder) {
  Tree::Save(tree_builder);
  auto builder = tree_builder.initSingleQuery();
  Save(builder);
}

void SingleQuery::Save(capnp::SingleQuery::Builder &builder) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder.initClauses(clauses_.size());
  for (size_t i = 0; i < clauses_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    clauses_[i]->Save(tree_builder);
  }
}

SingleQuery *SingleQuery::Construct(capnp::SingleQuery::Reader &reader,
                                    AstTreeStorage &storage) {
  auto query = storage.Create<SingleQuery>();
  for (auto tree_reader : reader.getClauses()) {
    auto tree = storage.Load(tree_reader);
    query->clauses_.push_back(dynamic_cast<Clause *>(tree));
  }
  return query;
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
