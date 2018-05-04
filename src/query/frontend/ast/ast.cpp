#include "query/frontend/ast/ast.hpp"

#include <algorithm>
// Include archives before registering most derived types.
#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"

#include "utils/serialization.capnp.h"

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

// Capnproto serialization.

Tree *AstTreeStorage::Load(const capnp::Tree::Reader &tree,
                           std::vector<int> *loaded_uids) {
  auto uid = tree.getUid();

  // Check if element already deserialized and if yes, return existing
  // element from storage.
  if (utils::Contains(*loaded_uids, uid)) {
    auto found = std::find_if(storage_.begin(), storage_.end(),
                              [&](const auto &n) { return n->uid() == uid; });
    DCHECK(found != storage_.end());
    return found->get();
  }

  Tree *ret = nullptr;
  switch (tree.which()) {
    case capnp::Tree::EXPRESSION: {
      auto expr_reader = tree.getExpression();
      ret = Expression::Construct(expr_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
    case capnp::Tree::WHERE: {
      auto where_reader = tree.getWhere();
      ret = Where::Construct(where_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
    case capnp::Tree::CLAUSE: {
      auto clause_reader = tree.getClause();
      ret = Clause::Construct(clause_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
    case capnp::Tree::CYPHER_UNION: {
      auto cu_reader = tree.getCypherUnion();
      ret = CypherUnion::Construct(cu_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
    case capnp::Tree::NAMED_EXPRESSION: {
      auto ne_reader = tree.getNamedExpression();
      ret = NamedExpression::Construct(ne_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
    case capnp::Tree::PATTERN: {
      auto pattern_reader = tree.getPattern();
      ret = Pattern::Construct(pattern_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
    case capnp::Tree::PATTERN_ATOM: {
      auto pa_reader = tree.getPatternAtom();
      ret = PatternAtom::Construct(pa_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
    case capnp::Tree::QUERY: {
      this->query()->Load(tree, this, loaded_uids);
      ret = this->query();
      break;
    }
    case capnp::Tree::SINGLE_QUERY: {
      auto single_reader = tree.getSingleQuery();
      ret = SingleQuery::Construct(single_reader, this);
      ret->Load(tree, this, loaded_uids);
      break;
    }
  }
  DCHECK(ret != nullptr);
  loaded_uids->emplace_back(ret->uid_);
  auto previous_max = std::max_element(
      storage_.begin(), storage_.end(),
      [](const std::unique_ptr<Tree> &a, const std::unique_ptr<Tree> &b) {
        return a->uid() < b->uid();
      });
  next_uid_ = (*previous_max)->uid() + 1;
  return ret;
}

// Tree.
void Tree::Save(capnp::Tree::Builder *tree_builder,
                std::vector<int> *saved_uids) {
  tree_builder->setUid(uid_);
}

bool Tree::IsSaved(const std::vector<int> &saved_uids) {
  return utils::Contains(saved_uids, uid_);
}

void Tree::Load(const capnp::Tree::Reader &reader, AstTreeStorage *storage,
                std::vector<int> *loaded_uids) {
  uid_ = reader.getUid();
}

void Tree::AddToSaved(std::vector<int> *saved_uids) {
  saved_uids->emplace_back(uid_);
}

// Expression.
void Expression::Save(capnp::Tree::Builder *tree_builder,
                      std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto expr_builder = tree_builder->initExpression();
  Save(&expr_builder, saved_uids);
  AddToSaved(saved_uids);
}

Expression *Expression::Construct(const capnp::Expression::Reader &reader,
                                  AstTreeStorage *storage) {
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
void BaseLiteral::Save(capnp::Expression::Builder *expr_builder,
                       std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto base_literal_builder = expr_builder->initBaseLiteral();
  Save(&base_literal_builder, saved_uids);
}

BaseLiteral *BaseLiteral::Construct(const capnp::BaseLiteral::Reader &reader,
                                    AstTreeStorage *storage) {
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
void PrimitiveLiteral::Save(capnp::BaseLiteral::Builder *base_literal_builder,
                            std::vector<int> *saved_uids) {
  BaseLiteral::Save(base_literal_builder, saved_uids);
  auto primitive_literal_builder = base_literal_builder->initPrimitiveLiteral();
  primitive_literal_builder.setTokenPosition(token_position_);
  auto typed_value_builder = primitive_literal_builder.getValue();
  utils::SaveCapnpTypedValue(value_, typed_value_builder);
}

void PrimitiveLiteral::Load(const capnp::Tree::Reader &reader,
                            AstTreeStorage *storage,
                            std::vector<int> *loaded_uids) {
  BaseLiteral::Load(reader, storage, loaded_uids);
  auto pl_reader =
      reader.getExpression().getBaseLiteral().getPrimitiveLiteral();
  auto typed_value_reader = pl_reader.getValue();
  utils::LoadCapnpTypedValue(value_, typed_value_reader);
  token_position_ = pl_reader.getTokenPosition();
}

PrimitiveLiteral *PrimitiveLiteral::Construct(
    const capnp::PrimitiveLiteral::Reader &reader, AstTreeStorage *storage) {
  return storage->Create<PrimitiveLiteral>();
}

// List Literal.
void ListLiteral::Save(capnp::BaseLiteral::Builder *base_literal_builder,
                       std::vector<int> *saved_uids) {
  BaseLiteral::Save(base_literal_builder, saved_uids);
  auto list_literal_builder = base_literal_builder->initListLiteral();
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      list_literal_builder.initElements(elements_.size());
  for (size_t i = 0; i < elements_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    elements_[i]->Save(&tree_builder, saved_uids);
  }
}

void ListLiteral::Load(const capnp::Tree::Reader &base_reader,
                       AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  BaseLiteral::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getBaseLiteral().getListLiteral();
  for (const auto tree_reader : reader.getElements()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    elements_.push_back(dynamic_cast<Expression *>(tree));
  }
}

ListLiteral *ListLiteral::Construct(const capnp::ListLiteral::Reader &reader,
                                    AstTreeStorage *storage) {
  return storage->Create<ListLiteral>();
}

// Map Literal.
void MapLiteral::Save(capnp::BaseLiteral::Builder *base_literal_builder,
                      std::vector<int> *saved_uids) {
  BaseLiteral::Save(base_literal_builder, saved_uids);
  auto map_literal_builder = base_literal_builder->initMapLiteral();
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
    if (entry.second) entry.second->Save(&value_builder, saved_uids);
    ++i;
  }
}

void MapLiteral::Load(const capnp::Tree::Reader &base_reader,
                      AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  BaseLiteral::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getBaseLiteral().getMapLiteral();
  for (auto entry_reader : reader.getElements()) {
    auto key_pair_reader = entry_reader.getKey();
    auto key_first = key_pair_reader.getFirst().cStr();
    auto storage_property_reader = key_pair_reader.getSecond();
    storage::Property key_second;
    key_second.Load(storage_property_reader);
    const auto value_reader = entry_reader.getValue();
    auto value = storage->Load(value_reader, loaded_uids);
    auto key = std::make_pair(key_first, key_second);
    // TODO Maybe check for nullptr expression?
    elements_.emplace(key, dynamic_cast<Expression *>(value));
  }
}

MapLiteral *MapLiteral::Construct(const capnp::MapLiteral::Reader &reader,
                                  AstTreeStorage *storage) {
  return storage->Create<MapLiteral>();
}

// Binary Operator.
void BinaryOperator::Save(capnp::Expression::Builder *expr_builder,
                          std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initBinaryOperator();
  Save(&builder, saved_uids);
}

void BinaryOperator::Save(capnp::BinaryOperator::Builder *builder,
                          std::vector<int> *saved_uids) {
  if (expression1_) {
    auto expr1_builder = builder->getExpression1();
    expression1_->Save(&expr1_builder, saved_uids);
  }
  if (expression2_) {
    auto expr2_builder = builder->getExpression2();
    expression2_->Save(&expr2_builder, saved_uids);
  }
}

void BinaryOperator::Load(const capnp::Tree::Reader &reader,
                          AstTreeStorage *storage,
                          std::vector<int> *loaded_uids) {
  Expression::Load(reader, storage, loaded_uids);
  auto bop_reader = reader.getExpression().getBinaryOperator();
  if (bop_reader.hasExpression1()) {
    const auto expr1_reader = bop_reader.getExpression1();
    expression1_ =
        dynamic_cast<Expression *>(storage->Load(expr1_reader, loaded_uids));
  }
  if (bop_reader.hasExpression2()) {
    const auto expr2_reader = bop_reader.getExpression2();
    expression2_ =
        dynamic_cast<Expression *>(storage->Load(expr2_reader, loaded_uids));
  }
}

BinaryOperator *BinaryOperator::Construct(
    const capnp::BinaryOperator::Reader &reader, AstTreeStorage *storage) {
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

void OrOperator::Save(capnp::BinaryOperator::Builder *builder,
                      std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initOrOperator();
}

OrOperator *OrOperator::Construct(const capnp::OrOperator::Reader &,
                                  AstTreeStorage *storage) {
  return storage->Create<OrOperator>();
}

void XorOperator::Save(capnp::BinaryOperator::Builder *builder,
                       std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initXorOperator();
}

XorOperator *XorOperator::Construct(const capnp::XorOperator::Reader &,
                                    AstTreeStorage *storage) {
  return storage->Create<XorOperator>();
}

void AndOperator::Save(capnp::BinaryOperator::Builder *builder,
                       std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initAndOperator();
}

AndOperator *AndOperator::Construct(const capnp::AndOperator::Reader &,
                                    AstTreeStorage *storage) {
  return storage->Create<AndOperator>();
}

void AdditionOperator::Save(capnp::BinaryOperator::Builder *builder,
                            std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initAdditionOperator();
}

AdditionOperator *AdditionOperator::Construct(
    const capnp::AdditionOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<AdditionOperator>();
}

void SubtractionOperator::Save(capnp::BinaryOperator::Builder *builder,
                               std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initSubtractionOperator();
}

SubtractionOperator *SubtractionOperator::Construct(
    capnp::SubtractionOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<SubtractionOperator>();
}

void MultiplicationOperator::Save(capnp::BinaryOperator::Builder *builder,
                                  std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initMultiplicationOperator();
}

MultiplicationOperator *MultiplicationOperator::Construct(
    capnp::MultiplicationOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<MultiplicationOperator>();
}

void DivisionOperator::Save(capnp::BinaryOperator::Builder *builder,
                            std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initDivisionOperator();
}

DivisionOperator *DivisionOperator::Construct(
    const capnp::DivisionOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<DivisionOperator>();
}

void ModOperator::Save(capnp::BinaryOperator::Builder *builder,
                       std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initModOperator();
}

ModOperator *ModOperator::Construct(const capnp::ModOperator::Reader &,
                                    AstTreeStorage *storage) {
  return storage->Create<ModOperator>();
}

void NotEqualOperator::Save(capnp::BinaryOperator::Builder *builder,
                            std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initNotEqualOperator();
}

NotEqualOperator *NotEqualOperator::Construct(
    const capnp::NotEqualOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<NotEqualOperator>();
}

void EqualOperator::Save(capnp::BinaryOperator::Builder *builder,
                         std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initEqualOperator();
}

EqualOperator *EqualOperator::Construct(const capnp::EqualOperator::Reader &,
                                        AstTreeStorage *storage) {
  return storage->Create<EqualOperator>();
}

void LessOperator::Save(capnp::BinaryOperator::Builder *builder,
                        std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initLessOperator();
}

LessOperator *LessOperator::Construct(const capnp::LessOperator::Reader &,
                                      AstTreeStorage *storage) {
  return storage->Create<LessOperator>();
}

void GreaterOperator::Save(capnp::BinaryOperator::Builder *builder,
                           std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initGreaterOperator();
}

GreaterOperator *GreaterOperator::Construct(
    const capnp::GreaterOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<GreaterOperator>();
}

void LessEqualOperator::Save(capnp::BinaryOperator::Builder *builder,
                             std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initLessEqualOperator();
}

LessEqualOperator *LessEqualOperator::Construct(
    const capnp::LessEqualOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<LessEqualOperator>();
}

void GreaterEqualOperator::Save(capnp::BinaryOperator::Builder *builder,
                                std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initGreaterEqualOperator();
}

GreaterEqualOperator *GreaterEqualOperator::Construct(
    const capnp::GreaterEqualOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<GreaterEqualOperator>();
}

void InListOperator::Save(capnp::BinaryOperator::Builder *builder,
                          std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initInListOperator();
}

InListOperator *InListOperator::Construct(const capnp::InListOperator::Reader &,
                                          AstTreeStorage *storage) {
  return storage->Create<InListOperator>();
}

void ListMapIndexingOperator::Save(capnp::BinaryOperator::Builder *builder,
                                   std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  builder->initListMapIndexingOperator();
}

ListMapIndexingOperator *ListMapIndexingOperator::Construct(
    capnp::ListMapIndexingOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<ListMapIndexingOperator>();
}

void Aggregation::Save(capnp::BinaryOperator::Builder *builder,
                       std::vector<int> *saved_uids) {
  BinaryOperator::Save(builder, saved_uids);
  auto ag_builder = builder->initAggregation();
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

Aggregation *Aggregation::Construct(const capnp::Aggregation::Reader &reader,
                                    AstTreeStorage *storage) {
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
  return storage->Create<Aggregation>(op);
}

// Unary Operator.
void UnaryOperator::Save(capnp::Expression::Builder *expr_builder,
                         std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initUnaryOperator();
  Save(&builder, saved_uids);
}

void UnaryOperator::Save(capnp::UnaryOperator::Builder *builder,
                         std::vector<int> *saved_uids) {
  if (expression_) {
    auto expr_builder = builder->getExpression();
    expression_->Save(&expr_builder, saved_uids);
  }
}

void UnaryOperator::Load(const capnp::Tree::Reader &reader,
                         AstTreeStorage *storage,
                         std::vector<int> *loaded_uids) {
  Expression::Load(reader, storage, loaded_uids);
  if (reader.hasExpression()) {
    const auto expr_reader =
        reader.getExpression().getUnaryOperator().getExpression();
    expression_ =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
}

UnaryOperator *UnaryOperator::Construct(
    const capnp::UnaryOperator::Reader &reader, AstTreeStorage *storage) {
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
void IsNullOperator::Save(capnp::UnaryOperator::Builder *builder,
                          std::vector<int> *saved_uids) {
  UnaryOperator::Save(builder, saved_uids);
  builder->initIsNullOperator();
}

IsNullOperator *IsNullOperator::Construct(const capnp::IsNullOperator::Reader &,
                                          AstTreeStorage *storage) {
  return storage->Create<IsNullOperator>();
}

// Not Operator.
void NotOperator::Save(capnp::UnaryOperator::Builder *builder,
                       std::vector<int> *saved_uids) {
  UnaryOperator::Save(builder, saved_uids);
  builder->initNotOperator();
}

NotOperator *NotOperator::Construct(const capnp::NotOperator::Reader &,
                                    AstTreeStorage *storage) {
  return storage->Create<NotOperator>();
}

// UnaryPlus Operator.
void UnaryPlusOperator::Save(capnp::UnaryOperator::Builder *builder,
                             std::vector<int> *saved_uids) {
  UnaryOperator::Save(builder, saved_uids);
  builder->initUnaryPlusOperator();
}

UnaryPlusOperator *UnaryPlusOperator::Construct(
    const capnp::UnaryPlusOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<UnaryPlusOperator>();
}

// UnaryMinus Operator.
void UnaryMinusOperator::Save(capnp::UnaryOperator::Builder *builder,
                              std::vector<int> *saved_uids) {
  UnaryOperator::Save(builder, saved_uids);
  builder->initUnaryMinusOperator();
}

UnaryMinusOperator *UnaryMinusOperator::Construct(
    capnp::UnaryMinusOperator::Reader &, AstTreeStorage *storage) {
  return storage->Create<UnaryMinusOperator>();
}

// ListSlicing Operator.
void ListSlicingOperator::Save(capnp::Expression::Builder *expr_builder,
                               std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initListSlicingOperator();
  Save(&builder, saved_uids);
}

void ListSlicingOperator::Save(capnp::ListSlicingOperator::Builder *builder,
                               std::vector<int> *saved_uids) {
  if (list_) {
    auto list_builder = builder->getList();
    list_->Save(&list_builder, saved_uids);
  }
  if (lower_bound_) {
    auto lb_builder = builder->getLowerBound();
    lower_bound_->Save(&lb_builder, saved_uids);
  }
  if (upper_bound_) {
    auto up_builder = builder->getUpperBound();
    upper_bound_->Save(&up_builder, saved_uids);
  }
}

void ListSlicingOperator::Load(const capnp::Tree::Reader &base_reader,
                               AstTreeStorage *storage,
                               std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getListSlicingOperator();
  if (reader.hasList()) {
    const auto list_reader = reader.getList();
    list_ = dynamic_cast<Expression *>(storage->Load(list_reader, loaded_uids));
  }
  if (reader.hasUpperBound()) {
    const auto ub_reader = reader.getUpperBound();
    upper_bound_ =
        dynamic_cast<Expression *>(storage->Load(ub_reader, loaded_uids));
  }
  if (reader.hasLowerBound()) {
    const auto lb_reader = reader.getLowerBound();
    lower_bound_ =
        dynamic_cast<Expression *>(storage->Load(lb_reader, loaded_uids));
  }
}

ListSlicingOperator *ListSlicingOperator::Construct(
    const capnp::ListSlicingOperator::Reader &reader, AstTreeStorage *storage) {
  return storage->Create<ListSlicingOperator>(nullptr, nullptr, nullptr);
}

// If Operator.
void IfOperator::Save(capnp::Expression::Builder *expr_builder,
                      std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initIfOperator();
  Save(&builder, saved_uids);
}

void IfOperator::Save(capnp::IfOperator::Builder *builder,
                      std::vector<int> *saved_uids) {
  auto condition_builder = builder->getCondition();
  condition_->Save(&condition_builder, saved_uids);
  auto then_builder = builder->getThenExpression();
  then_expression_->Save(&then_builder, saved_uids);
  auto else_builder = builder->getElseExpression();
  else_expression_->Save(&else_builder, saved_uids);
}

void IfOperator::Load(const capnp::Tree::Reader &base_reader,
                      AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getIfOperator();
  const auto condition_reader = reader.getCondition();
  condition_ =
      dynamic_cast<Expression *>(storage->Load(condition_reader, loaded_uids));
  const auto then_reader = reader.getThenExpression();
  then_expression_ =
      dynamic_cast<Expression *>(storage->Load(then_reader, loaded_uids));
  const auto else_reader = reader.getElseExpression();
  else_expression_ =
      dynamic_cast<Expression *>(storage->Load(else_reader, loaded_uids));
}

IfOperator *IfOperator::Construct(const capnp::IfOperator::Reader &reader,
                                  AstTreeStorage *storage) {
  return storage->Create<IfOperator>(nullptr, nullptr, nullptr);
}

// All
void All::Save(capnp::Expression::Builder *expr_builder,
               std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initAll();
  Save(&builder, saved_uids);
}

void All::Save(capnp::All::Builder *builder, std::vector<int> *saved_uids) {
  auto identifier_builder = builder->getIdentifier();
  identifier_->Save(&identifier_builder, saved_uids);
  auto expr_builder = builder->getListExpression();
  list_expression_->Save(&expr_builder, saved_uids);
  auto where_builder = builder->getWhere();
  where_->Save(&where_builder, saved_uids);
}

void All::Load(const capnp::Tree::Reader &base_reader, AstTreeStorage *storage,
               std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getAll();
  const auto id_reader = reader.getIdentifier();
  identifier_ =
      dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  const auto expr_reader = reader.getListExpression();
  list_expression_ =
      dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  const auto where_reader = reader.getWhere();
  where_ = dynamic_cast<Where *>(storage->Load(where_reader, loaded_uids));
}

All *All::Construct(const capnp::All::Reader &reader, AstTreeStorage *storage) {
  return storage->Create<All>(nullptr, nullptr, nullptr);
}

// Function
void Function::Save(capnp::Expression::Builder *expr_builder,
                    std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initFunction();
  Save(&builder, saved_uids);
}

void Function::Save(capnp::Function::Builder *builder,
                    std::vector<int> *saved_uids) {
  builder->setFunctionName(function_name_);
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder->initArguments(arguments_.size());
  for (size_t i = 0; i < arguments_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    arguments_[i]->Save(&tree_builder, saved_uids);
  }
}

void Function::Load(const capnp::Tree::Reader &base_reader,
                    AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getFunction();
  function_name_ = reader.getFunctionName().cStr();
  for (const auto tree_reader : reader.getArguments()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    arguments_.push_back(dynamic_cast<Expression *>(tree));
  }
  function_ = NameToFunction(function_name_);
}

Function *Function::Construct(const capnp::Function::Reader &reader,
                              AstTreeStorage *storage) {
  return storage->Create<Function>();
}

// Identifier
void Identifier::Save(capnp::Expression::Builder *expr_builder,
                      std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initIdentifier();
  Save(&builder, saved_uids);
}

void Identifier::Save(capnp::Identifier::Builder *builder,
                      std::vector<int> *saved_uids) {
  builder->setName(name_);
  builder->setUserDeclared(user_declared_);
}

Identifier *Identifier::Construct(const capnp::Identifier::Reader &reader,
                                  AstTreeStorage *storage) {
  auto name = reader.getName().cStr();
  auto user_declared = reader.getUserDeclared();
  return storage->Create<Identifier>(name, user_declared);
}

// LabelsTest
void LabelsTest::Save(capnp::Expression::Builder *expr_builder,
                      std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initLabelsTest();
  Save(&builder, saved_uids);
}

void LabelsTest::Save(capnp::LabelsTest::Builder *builder,
                      std::vector<int> *saved_uids) {
  if (expression_) {
    auto expr_builder = builder->initExpression();
    expression_->Save(&expr_builder, saved_uids);
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder->initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}

void LabelsTest::Load(const capnp::Tree::Reader &base_reader,
                      AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getLabelsTest();
  if (reader.hasExpression()) {
    const auto expr_reader = reader.getExpression();
    expression_ =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    labels_.push_back(label);
  }
}

LabelsTest *LabelsTest::Construct(const capnp::LabelsTest::Reader &reader,
                                  AstTreeStorage *storage) {
  return storage->Create<LabelsTest>(nullptr, std::vector<storage::Label>());
}

// ParameterLookup
void ParameterLookup::Save(capnp::Expression::Builder *expr_builder,
                           std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initParameterLookup();
  Save(&builder, saved_uids);
}

void ParameterLookup::Save(capnp::ParameterLookup::Builder *builder,
                           std::vector<int> *saved_uids) {
  builder->setTokenPosition(token_position_);
}

ParameterLookup *ParameterLookup::Construct(
    const capnp::ParameterLookup::Reader &reader, AstTreeStorage *storage) {
  auto token_position = reader.getTokenPosition();
  return storage->Create<ParameterLookup>(token_position);
}

// PropertyLookup
void PropertyLookup::Save(capnp::Expression::Builder *expr_builder,
                          std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initPropertyLookup();
  Save(&builder, saved_uids);
}

void PropertyLookup::Save(capnp::PropertyLookup::Builder *builder,
                          std::vector<int> *saved_uids) {
  if (expression_) {
    auto expr_builder = builder->initExpression();
    expression_->Save(&expr_builder, saved_uids);
  }
  builder->setPropertyName(property_name_);
  auto storage_property_builder = builder->initProperty();
  property_.Save(storage_property_builder);
}

void PropertyLookup::Load(const capnp::Tree::Reader &base_reader,
                          AstTreeStorage *storage,
                          std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getPropertyLookup();
  if (reader.hasExpression()) {
    const auto expr_reader = reader.getExpression();
    expression_ =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
  property_name_ = reader.getPropertyName().cStr();
  auto storage_property_reader = reader.getProperty();
  property_.Load(storage_property_reader);
}

PropertyLookup *PropertyLookup::Construct(
    const capnp::PropertyLookup::Reader &reader, AstTreeStorage *storage) {
  return storage->Create<PropertyLookup>(nullptr, "", storage::Property());
}

// Reduce
void Reduce::Save(capnp::Expression::Builder *expr_builder,
                  std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initReduce();
  Save(&builder, saved_uids);
}

void Reduce::Save(capnp::Reduce::Builder *builder,
                  std::vector<int> *saved_uids) {
  auto acc_builder = builder->initAccumulator();
  accumulator_->Save(&acc_builder, saved_uids);
  auto init_builder = builder->initInitializer();
  initializer_->Save(&init_builder, saved_uids);
  auto id_builder = builder->initIdentifier();
  identifier_->Save(&id_builder, saved_uids);
  auto list_builder = builder->initList();
  list_->Save(&list_builder, saved_uids);
  auto expr_builder = builder->initExpression();
  expression_->Save(&expr_builder, saved_uids);
}

void Reduce::Load(const capnp::Tree::Reader &base_reader,
                  AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getReduce();
  const auto acc_reader = reader.getAccumulator();
  accumulator_ =
      dynamic_cast<Identifier *>(storage->Load(acc_reader, loaded_uids));
  const auto init_reader = reader.getInitializer();
  initializer_ =
      dynamic_cast<Expression *>(storage->Load(init_reader, loaded_uids));
  const auto id_reader = reader.getIdentifier();
  identifier_ =
      dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  const auto list_reader = reader.getList();
  list_ = dynamic_cast<Expression *>(storage->Load(list_reader, loaded_uids));
  const auto expr_reader = reader.getExpression();
  expression_ =
      dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
}

Reduce *Reduce::Construct(const capnp::Reduce::Reader &reader,
                          AstTreeStorage *storage) {
  return storage->Create<Reduce>(nullptr, nullptr, nullptr, nullptr, nullptr);
}

// Single
void Single::Save(capnp::Expression::Builder *expr_builder,
                  std::vector<int> *saved_uids) {
  Expression::Save(expr_builder, saved_uids);
  auto builder = expr_builder->initSingle();
  Save(&builder, saved_uids);
}

void Single::Save(capnp::Single::Builder *builder,
                  std::vector<int> *saved_uids) {
  auto where_builder = builder->initWhere();
  where_->Save(&where_builder, saved_uids);
  auto id_builder = builder->initIdentifier();
  identifier_->Save(&id_builder, saved_uids);
  auto expr_builder = builder->initListExpression();
  list_expression_->Save(&expr_builder, saved_uids);
}

void Single::Load(const capnp::Tree::Reader &base_reader,
                  AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Expression::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getExpression().getSingle();
  const auto id_reader = reader.getIdentifier();
  identifier_ =
      dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  const auto list_reader = reader.getListExpression();
  list_expression_ =
      dynamic_cast<Expression *>(storage->Load(list_reader, loaded_uids));
  const auto where_reader = reader.getWhere();
  where_ = dynamic_cast<Where *>(storage->Load(where_reader, loaded_uids));
}

Single *Single::Construct(const capnp::Single::Reader &reader,
                          AstTreeStorage *storage) {
  return storage->Create<Single>(nullptr, nullptr, nullptr);
}

// Where
void Where::Save(capnp::Tree::Builder *tree_builder,
                 std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto builder = tree_builder->initWhere();
  Save(&builder, saved_uids);
  AddToSaved(saved_uids);
}

void Where::Save(capnp::Where::Builder *builder, std::vector<int> *saved_uids) {
  if (expression_) {
    auto expr_builder = builder->initExpression();
    expression_->Save(&expr_builder, saved_uids);
  }
}

void Where::Load(const capnp::Tree::Reader &base_reader,
                 AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Tree::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getWhere();
  if (reader.hasExpression()) {
    const auto expr_reader = reader.getExpression();
    expression_ =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
}

Where *Where::Construct(const capnp::Where::Reader &reader,
                        AstTreeStorage *storage) {
  return storage->Create<Where>();
}

// Clause.
void Clause::Save(capnp::Tree::Builder *tree_builder,
                  std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto clause_builder = tree_builder->initClause();
  Save(&clause_builder, saved_uids);
  AddToSaved(saved_uids);
}

Clause *Clause::Construct(const capnp::Clause::Reader &reader,
                          AstTreeStorage *storage) {
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
void Create::Save(capnp::Clause::Builder *builder,
                  std::vector<int> *saved_uids) {
  Clause::Save(builder, saved_uids);
  auto create_builder = builder->initCreate();
  Create::Save(&create_builder, saved_uids);
}

void Create::Save(capnp::Create::Builder *builder,
                  std::vector<int> *saved_uids) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder->initPatterns(patterns_.size());
  for (size_t i = 0; i < patterns_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    patterns_[i]->Save(&tree_builder, saved_uids);
  }
}

void Create::Load(const capnp::Tree::Reader &base_reader,
                  AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getCreate();
  for (const auto pattern_reader : reader.getPatterns()) {
    auto tree = storage->Load(pattern_reader, loaded_uids);
    patterns_.push_back(dynamic_cast<Pattern *>(tree));
  }
}

Create *Create::Construct(const capnp::Create::Reader &reader,
                          AstTreeStorage *storage) {
  return storage->Create<Create>();
}

// CreateIndex.
void CreateIndex::Save(capnp::Clause::Builder *builder,
                       std::vector<int> *saved_uids) {
  Clause::Save(builder, saved_uids);
  auto create_builder = builder->initCreateIndex();
  CreateIndex::Save(&create_builder, saved_uids);
}

void CreateIndex::Save(capnp::CreateIndex::Builder *builder,
                       std::vector<int> *saved_uids) {
  auto label_builder = builder->getLabel();
  label_.Save(label_builder);
  auto property_builder = builder->getProperty();
  property_.Save(property_builder);
}

CreateIndex *CreateIndex::Construct(const capnp::CreateIndex::Reader &reader,
                                    AstTreeStorage *storage) {
  auto label_reader = reader.getLabel();
  storage::Label label;
  label.Load(label_reader);
  auto property_reader = reader.getProperty();
  storage::Property property;
  property.Load(property_reader);
  return storage->Create<CreateIndex>(label, property);
}

// Delete.
void Delete::Save(capnp::Clause::Builder *builder,
                  std::vector<int> *saved_uids) {
  Clause::Save(builder, saved_uids);
  auto del_builder = builder->initDelete();
  Delete::Save(&del_builder, saved_uids);
}

void Delete::Save(capnp::Delete::Builder *builder,
                  std::vector<int> *saved_uids) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder->initExpressions(expressions_.size());
  for (size_t i = 0; i < expressions_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    expressions_[i]->Save(&tree_builder, saved_uids);
  }
  builder->setDetach(detach_);
}

void Delete::Load(const capnp::Tree::Reader &base_reader,
                  AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getDelete();
  for (const auto tree_reader : reader.getExpressions()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    expressions_.push_back(dynamic_cast<Expression *>(tree));
  }
  detach_ = reader.getDetach();
}

Delete *Delete::Construct(const capnp::Delete::Reader &reader,
                          AstTreeStorage *storage) {
  return storage->Create<Delete>();
}

// Match.
void Match::Save(capnp::Clause::Builder *clause_builder,
                 std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initMatch();
  Match::Save(&builder, saved_uids);
}

void Match::Save(capnp::Match::Builder *builder, std::vector<int> *saved_uids) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder->initPatterns(patterns_.size());
  for (size_t i = 0; i < patterns_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    patterns_[i]->Save(&tree_builder, saved_uids);
  }

  if (where_) {
    auto where_builder = builder->initWhere();
    where_->Save(&where_builder, saved_uids);
  }
  builder->setOptional(optional_);
}

void Match::Load(const capnp::Tree::Reader &base_reader,
                 AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getMatch();
  for (const auto tree_reader : reader.getPatterns()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    patterns_.push_back(dynamic_cast<Pattern *>(tree));
  }
  if (reader.hasWhere()) {
    const auto where_reader = reader.getWhere();
    where_ = dynamic_cast<Where *>(storage->Load(where_reader, loaded_uids));
  }
  optional_ = reader.getOptional();
}

Match *Match::Construct(const capnp::Match::Reader &reader,
                        AstTreeStorage *storage) {
  return storage->Create<Match>();
}

// Merge.
void Merge::Save(capnp::Clause::Builder *clause_builder,
                 std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initMerge();
  Merge::Save(&builder, saved_uids);
}

void Merge::Save(capnp::Merge::Builder *builder, std::vector<int> *saved_uids) {
  ::capnp::List<capnp::Tree>::Builder match_builder =
      builder->initOnMatch(on_match_.size());
  for (size_t i = 0; i < on_match_.size(); ++i) {
    auto tree_builder = match_builder[i];
    on_match_[i]->Save(&tree_builder, saved_uids);
  }

  ::capnp::List<capnp::Tree>::Builder create_builder =
      builder->initOnCreate(on_create_.size());
  for (size_t i = 0; i < on_create_.size(); ++i) {
    auto tree_builder = create_builder[i];
    on_create_[i]->Save(&tree_builder, saved_uids);
  }

  if (pattern_) {
    auto pattern_builder = builder->getPattern();
    pattern_->Save(&pattern_builder, saved_uids);
  }
}

void Merge::Load(const capnp::Tree::Reader &base_reader,
                 AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getMerge();
  for (const auto tree_reader : reader.getOnMatch()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    on_match_.push_back(dynamic_cast<Clause *>(tree));
  }

  for (const auto tree_reader : reader.getOnCreate()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    on_create_.push_back(dynamic_cast<Clause *>(tree));
  }
  if (reader.hasPattern()) {
    const auto pattern_reader = reader.getPattern();
    pattern_ =
        dynamic_cast<Pattern *>(storage->Load(pattern_reader, loaded_uids));
  }
}
Merge *Merge::Construct(const capnp::Merge::Reader &reader,
                        AstTreeStorage *storage) {
  return storage->Create<Merge>();
}

// RemoveLabels.
void RemoveLabels::Save(capnp::Clause::Builder *clause_builder,
                        std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initRemoveLabels();
  RemoveLabels::Save(&builder, saved_uids);
}

void RemoveLabels::Save(capnp::RemoveLabels::Builder *builder,
                        std::vector<int> *saved_uids) {
  if (identifier_) {
    auto id_builder = builder->getIdentifier();
    identifier_->Save(&id_builder, saved_uids);
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder->initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}
void RemoveLabels::Load(const capnp::Tree::Reader &base_reader,
                        AstTreeStorage *storage,
                        std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getRemoveLabels();
  if (reader.hasIdentifier()) {
    const auto id_reader = reader.getIdentifier();
    identifier_ =
        dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  }
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    labels_.push_back(label);
  }
}

RemoveLabels *RemoveLabels::Construct(const capnp::RemoveLabels::Reader &reader,
                                      AstTreeStorage *storage) {
  return storage->Create<RemoveLabels>();
}

// RemoveProperty.
void RemoveProperty::Save(capnp::Clause::Builder *clause_builder,
                          std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initRemoveProperty();
  RemoveProperty::Save(&builder, saved_uids);
}

void RemoveProperty::Save(capnp::RemoveProperty::Builder *builder,
                          std::vector<int> *saved_uids) {
  if (property_lookup_) {
    auto pl_builder = builder->getPropertyLookup();
    property_lookup_->Save(&pl_builder, saved_uids);
  }
}

void RemoveProperty::Load(const capnp::Tree::Reader &base_reader,
                          AstTreeStorage *storage,
                          std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getRemoveProperty();
  if (reader.hasPropertyLookup()) {
    const auto pl_reader = reader.getPropertyLookup();
    property_lookup_ =
        dynamic_cast<PropertyLookup *>(storage->Load(pl_reader, loaded_uids));
  }
}

RemoveProperty *RemoveProperty::Construct(
    const capnp::RemoveProperty::Reader &reader, AstTreeStorage *storage) {
  return storage->Create<RemoveProperty>();
}

// Return.
void Return::Save(capnp::Clause::Builder *clause_builder,
                  std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initReturn();
  Return::Save(&builder, saved_uids);
}

void SaveReturnBody(capnp::ReturnBody::Builder *rb_builder, ReturnBody &body,
                    std::vector<int> *saved_uids) {
  rb_builder->setDistinct(body.distinct);
  rb_builder->setAllIdentifiers(body.all_identifiers);

  ::capnp::List<capnp::Tree>::Builder named_expressions =
      rb_builder->initNamedExpressions(body.named_expressions.size());
  for (size_t i = 0; i < body.named_expressions.size(); ++i) {
    auto tree_builder = named_expressions[i];
    body.named_expressions[i]->Save(&tree_builder, saved_uids);
  }

  ::capnp::List<capnp::ReturnBody::Pair>::Builder order_by =
      rb_builder->initOrderBy(body.order_by.size());
  for (size_t i = 0; i < body.order_by.size(); ++i) {
    auto pair_builder = order_by[i];
    auto ordering = body.order_by[i].first == Ordering::ASC
                        ? capnp::Ordering::ASC
                        : capnp::Ordering::DESC;
    pair_builder.setOrdering(ordering);
    auto tree_builder = pair_builder.getExpression();
    body.order_by[i].second->Save(&tree_builder, saved_uids);
  }

  if (body.skip) {
    auto skip_builder = rb_builder->getSkip();
    body.skip->Save(&skip_builder, saved_uids);
  }
  if (body.limit) {
    auto limit_builder = rb_builder->getLimit();
    body.limit->Save(&limit_builder, saved_uids);
  }
}

void Return::Save(capnp::Return::Builder *builder,
                  std::vector<int> *saved_uids) {
  auto rb_builder = builder->initReturnBody();
  SaveReturnBody(&rb_builder, body_, saved_uids);
}

void LoadReturnBody(capnp::ReturnBody::Reader &rb_reader, ReturnBody &body,
                    AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  body.distinct = rb_reader.getDistinct();
  body.all_identifiers = rb_reader.getAllIdentifiers();

  for (const auto tree_reader : rb_reader.getNamedExpressions()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    body.named_expressions.push_back(dynamic_cast<NamedExpression *>(tree));
  }

  for (auto pair_reader : rb_reader.getOrderBy()) {
    auto ordering = pair_reader.getOrdering() == capnp::Ordering::ASC
                        ? Ordering::ASC
                        : Ordering::DESC;
    const auto tree_reader = pair_reader.getExpression();
    // TODO Check if expression is null?
    auto tree =
        dynamic_cast<Expression *>(storage->Load(tree_reader, loaded_uids));
    body.order_by.push_back(std::make_pair(ordering, tree));
  }

  if (rb_reader.hasSkip()) {
    const auto skip_reader = rb_reader.getSkip();
    body.skip =
        dynamic_cast<Expression *>(storage->Load(skip_reader, loaded_uids));
  }
  if (rb_reader.hasLimit()) {
    const auto limit_reader = rb_reader.getLimit();
    body.limit =
        dynamic_cast<Expression *>(storage->Load(limit_reader, loaded_uids));
  }
}

void Return::Load(const capnp::Tree::Reader &base_reader,
                  AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getReturn();
  auto rb_reader = reader.getReturnBody();
  LoadReturnBody(rb_reader, body_, storage, loaded_uids);
}

Return *Return::Construct(const capnp::Return::Reader &reader,
                          AstTreeStorage *storage) {
  return storage->Create<Return>();
}

// SetLabels.
void SetLabels::Save(capnp::Clause::Builder *clause_builder,
                     std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initSetLabels();
  SetLabels::Save(&builder, saved_uids);
}

void SetLabels::Save(capnp::SetLabels::Builder *builder,
                     std::vector<int> *saved_uids) {
  if (identifier_) {
    auto id_builder = builder->getIdentifier();
    identifier_->Save(&id_builder, saved_uids);
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder->initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}

void SetLabels::Load(const capnp::Tree::Reader &base_reader,
                     AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getSetLabels();
  if (reader.hasIdentifier()) {
    const auto id_reader = reader.getIdentifier();
    identifier_ =
        dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  }
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    labels_.push_back(label);
  }
}

SetLabels *SetLabels::Construct(const capnp::SetLabels::Reader &reader,
                                AstTreeStorage *storage) {
  return storage->Create<SetLabels>();
}

// SetProperty.
void SetProperty::Save(capnp::Clause::Builder *clause_builder,
                       std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initSetProperty();
  SetProperty::Save(&builder, saved_uids);
}

void SetProperty::Save(capnp::SetProperty::Builder *builder,
                       std::vector<int> *saved_uids) {
  if (property_lookup_) {
    auto pl_builder = builder->getPropertyLookup();
    property_lookup_->Save(&pl_builder, saved_uids);
  }
  if (expression_) {
    auto expr_builder = builder->getExpression();
    expression_->Save(&expr_builder, saved_uids);
  }
}

void SetProperty::Load(const capnp::Tree::Reader &base_reader,
                       AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getSetProperty();
  if (reader.hasPropertyLookup()) {
    const auto pl_reader = reader.getPropertyLookup();
    property_lookup_ =
        dynamic_cast<PropertyLookup *>(storage->Load(pl_reader, loaded_uids));
  }
  if (reader.hasExpression()) {
    const auto expr_reader = reader.getExpression();
    expression_ =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
}

SetProperty *SetProperty::Construct(const capnp::SetProperty::Reader &reader,
                                    AstTreeStorage *storage) {
  return storage->Create<SetProperty>();
}

// SetProperties.
void SetProperties::Save(capnp::Clause::Builder *clause_builder,
                         std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initSetProperties();
  SetProperties::Save(&builder, saved_uids);
}

void SetProperties::Save(capnp::SetProperties::Builder *builder,
                         std::vector<int> *saved_uids) {
  if (identifier_) {
    auto id_builder = builder->getIdentifier();
    identifier_->Save(&id_builder, saved_uids);
  }
  if (expression_) {
    auto expr_builder = builder->getExpression();
    expression_->Save(&expr_builder, saved_uids);
  }
  builder->setUpdate(update_);
}

void SetProperties::Load(const capnp::Tree::Reader &base_reader,
                         AstTreeStorage *storage,
                         std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getSetProperties();
  if (reader.hasIdentifier()) {
    const auto id_reader = reader.getIdentifier();
    identifier_ =
        dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  }
  if (reader.hasExpression()) {
    const auto expr_reader = reader.getExpression();
    expression_ =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
  update_ = reader.getUpdate();
}

SetProperties *SetProperties::Construct(
    const capnp::SetProperties::Reader &reader, AstTreeStorage *storage) {
  return storage->Create<SetProperties>();
}

// Unwind.
void Unwind::Save(capnp::Clause::Builder *clause_builder,
                  std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initUnwind();
  Unwind::Save(&builder, saved_uids);
}

void Unwind::Save(capnp::Unwind::Builder *builder,
                  std::vector<int> *saved_uids) {
  if (named_expression_) {
    auto expr_builder = builder->getNamedExpression();
    named_expression_->Save(&expr_builder, saved_uids);
  }
}

void Unwind::Load(const capnp::Tree::Reader &base_reader,
                  AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getUnwind();
  if (reader.hasNamedExpression()) {
    const auto expr_reader = reader.getNamedExpression();
    named_expression_ = dynamic_cast<NamedExpression *>(
        storage->Load(expr_reader, loaded_uids));
  }
}

Unwind *Unwind::Construct(const capnp::Unwind::Reader &reader,
                          AstTreeStorage *storage) {
  return storage->Create<Unwind>();
}

// With.
void With::Save(capnp::Clause::Builder *clause_builder,
                std::vector<int> *saved_uids) {
  Clause::Save(clause_builder, saved_uids);
  auto builder = clause_builder->initWith();
  With::Save(&builder, saved_uids);
}

void With::Save(capnp::With::Builder *builder, std::vector<int> *saved_uids) {
  if (where_) {
    auto where_builder = builder->getWhere();
    where_->Save(&where_builder, saved_uids);
  }
  auto rb_builder = builder->initReturnBody();
  SaveReturnBody(&rb_builder, body_, saved_uids);
}

void With::Load(const capnp::Tree::Reader &base_reader, AstTreeStorage *storage,
                std::vector<int> *loaded_uids) {
  Clause::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getClause().getWith();
  if (reader.hasWhere()) {
    const auto where_reader = reader.getWhere();
    where_ = dynamic_cast<Where *>(storage->Load(where_reader, loaded_uids));
  }
  auto rb_reader = reader.getReturnBody();
  LoadReturnBody(rb_reader, body_, storage, loaded_uids);
}

With *With::Construct(const capnp::With::Reader &reader,
                      AstTreeStorage *storage) {
  return storage->Create<With>();
}

// CypherUnion
void CypherUnion::Save(capnp::Tree::Builder *tree_builder,
                       std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto builder = tree_builder->initCypherUnion();
  Save(&builder, saved_uids);
  AddToSaved(saved_uids);
}

void CypherUnion::Save(capnp::CypherUnion::Builder *builder,
                       std::vector<int> *saved_uids) {
  if (single_query_) {
    auto sq_builder = builder->initSingleQuery();
    single_query_->Save(&sq_builder, saved_uids);
  }
  builder->setDistinct(distinct_);
  ::capnp::List<capnp::Symbol>::Builder symbol_builders =
      builder->initUnionSymbols(union_symbols_.size());
  for (size_t i = 0; i < union_symbols_.size(); ++i) {
    auto symbol_builder = symbol_builders[i];
    union_symbols_[i].Save(symbol_builder);
  }
}

void CypherUnion::Load(const capnp::Tree::Reader &base_reader,
                       AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Tree::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getCypherUnion();
  if (reader.hasSingleQuery()) {
    const auto sq_reader = reader.getSingleQuery();
    single_query_ =
        dynamic_cast<SingleQuery *>(storage->Load(sq_reader, loaded_uids));
  }
  distinct_ = reader.getDistinct();
  for (auto symbol_reader : reader.getUnionSymbols()) {
    Symbol symbol;
    symbol.Load(symbol_reader);
    union_symbols_.push_back(symbol);
  }
}

CypherUnion *CypherUnion::Construct(const capnp::CypherUnion::Reader &reader,
                                    AstTreeStorage *storage) {
  return storage->Create<CypherUnion>();
}

// NamedExpression
void NamedExpression::Save(capnp::Tree::Builder *tree_builder,
                           std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto builder = tree_builder->initNamedExpression();
  Save(&builder, saved_uids);
  AddToSaved(saved_uids);
}

void NamedExpression::Save(capnp::NamedExpression::Builder *builder,
                           std::vector<int> *saved_uids) {
  builder->setName(name_);
  builder->setTokenPosition(token_position_);
  if (expression_) {
    auto expr_builder = builder->getExpression();
    expression_->Save(&expr_builder, saved_uids);
  }
}

void NamedExpression::Load(const capnp::Tree::Reader &base_reader,
                           AstTreeStorage *storage,
                           std::vector<int> *loaded_uids) {
  Tree::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getNamedExpression();
  name_ = reader.getName().cStr();
  token_position_ = reader.getTokenPosition();
  if (reader.hasExpression()) {
    const auto expr_reader = reader.getExpression();
    expression_ =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
}

NamedExpression *NamedExpression::Construct(
    const capnp::NamedExpression::Reader &reader, AstTreeStorage *storage) {
  return storage->Create<NamedExpression>();
}

// Pattern
void Pattern::Save(capnp::Tree::Builder *tree_builder,
                   std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto builder = tree_builder->initPattern();
  Save(&builder, saved_uids);
  AddToSaved(saved_uids);
}

void Pattern::Save(capnp::Pattern::Builder *builder,
                   std::vector<int> *saved_uids) {
  if (identifier_) {
    auto id_builder = builder->getIdentifier();
    identifier_->Save(&id_builder, saved_uids);
  }
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder->initAtoms(atoms_.size());
  for (size_t i = 0; i < atoms_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    atoms_[i]->Save(&tree_builder, saved_uids);
  }
}

void Pattern::Load(const capnp::Tree::Reader &base_reader,
                   AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Tree::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getPattern();
  if (reader.hasIdentifier()) {
    const auto id_reader = reader.getIdentifier();
    identifier_ =
        dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  }
  for (const auto tree_reader : reader.getAtoms()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    atoms_.push_back(dynamic_cast<PatternAtom *>(tree));
  }
}

Pattern *Pattern::Construct(const capnp::Pattern::Reader &reader,
                            AstTreeStorage *storage) {
  return storage->Create<Pattern>();
}

// PatternAtom.
void PatternAtom::Save(capnp::Tree::Builder *tree_builder,
                       std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto pattern_builder = tree_builder->initPatternAtom();
  Save(&pattern_builder, saved_uids);
  AddToSaved(saved_uids);
}

void PatternAtom::Save(capnp::PatternAtom::Builder *builder,
                       std::vector<int> *saved_uids) {
  if (identifier_) {
    auto id_builder = builder->getIdentifier();
    identifier_->Save(&id_builder, saved_uids);
  }
}

PatternAtom *PatternAtom::Construct(const capnp::PatternAtom::Reader &reader,
                                    AstTreeStorage *storage) {
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

void PatternAtom::Load(const capnp::Tree::Reader &reader,
                       AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Tree::Load(reader, storage, loaded_uids);
  auto pa_reader = reader.getPatternAtom();
  if (pa_reader.hasIdentifier()) {
    const auto id_reader = pa_reader.getIdentifier();
    identifier_ =
        dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  }
}

// NodeAtom
void NodeAtom::Save(capnp::PatternAtom::Builder *pattern_builder,
                    std::vector<int> *saved_uids) {
  PatternAtom::Save(pattern_builder, saved_uids);
  auto builder = pattern_builder->initNodeAtom();
  Save(&builder, saved_uids);
}

void NodeAtom::Save(capnp::NodeAtom::Builder *builder,
                    std::vector<int> *saved_uids) {
  ::capnp::List<capnp::NodeAtom::Entry>::Builder map_builder =
      builder->initProperties(properties_.size());
  size_t i = 0;
  for (auto &entry : properties_) {
    auto entry_builder = map_builder[i];
    auto key_builder = entry_builder.getKey();
    key_builder.setFirst(entry.first.first);
    auto storage_property_builder = key_builder.getSecond();
    entry.first.second.Save(storage_property_builder);
    auto value_builder = entry_builder.getValue();
    if (entry.second) entry.second->Save(&value_builder, saved_uids);
    ++i;
  }
  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder->initLabels(labels_.size());
  for (size_t i = 0; i < labels_.size(); ++i) {
    auto common_builder = common_builders[i];
    labels_[i].Save(common_builder);
  }
}

void NodeAtom::Load(const capnp::Tree::Reader &base_reader,
                    AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  PatternAtom::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getPatternAtom().getNodeAtom();
  for (auto entry_reader : reader.getProperties()) {
    auto key_pair_reader = entry_reader.getKey();
    auto key_first = key_pair_reader.getFirst().cStr();
    auto storage_property_reader = key_pair_reader.getSecond();
    storage::Property property;
    property.Load(storage_property_reader);
    const auto value_reader = entry_reader.getValue();
    auto value = storage->Load(value_reader, loaded_uids);
    auto key = std::make_pair(key_first, property);
    // TODO Maybe check if expression is nullptr?
    properties_.emplace(key, dynamic_cast<Expression *>(value));
  }
  for (auto label_reader : reader.getLabels()) {
    storage::Label label;
    label.Load(label_reader);
    labels_.push_back(label);
  }
}

NodeAtom *NodeAtom::Construct(const capnp::NodeAtom::Reader &reader,
                              AstTreeStorage *storage) {
  return storage->Create<NodeAtom>();
}

// EdgeAtom
void EdgeAtom::Save(capnp::PatternAtom::Builder *pattern_builder,
                    std::vector<int> *saved_uids) {
  PatternAtom::Save(pattern_builder, saved_uids);
  auto builder = pattern_builder->initEdgeAtom();
  Save(&builder, saved_uids);
}

void SaveLambda(query::EdgeAtom::Lambda &lambda,
                capnp::EdgeAtom::Lambda::Builder *builder,
                std::vector<int> *saved_uids) {
  if (lambda.inner_edge) {
    auto ie_builder = builder->getInnerEdge();
    lambda.inner_edge->Save(&ie_builder, saved_uids);
  }
  if (lambda.inner_node) {
    auto in_builder = builder->getInnerNode();
    lambda.inner_node->Save(&in_builder, saved_uids);
  }
  if (lambda.expression) {
    auto expr_builder = builder->getExpression();
    lambda.expression->Save(&expr_builder, saved_uids);
  }
}

void EdgeAtom::Save(capnp::EdgeAtom::Builder *builder,
                    std::vector<int> *saved_uids) {
  switch (type_) {
    case Type::BREADTH_FIRST:
      builder->setType(capnp::EdgeAtom::Type::BREADTH_FIRST);
      break;
    case Type::DEPTH_FIRST:
      builder->setType(capnp::EdgeAtom::Type::DEPTH_FIRST);
      break;
    case Type::SINGLE:
      builder->setType(capnp::EdgeAtom::Type::SINGLE);
      break;
    case Type::WEIGHTED_SHORTEST_PATH:
      builder->setType(capnp::EdgeAtom::Type::WEIGHTED_SHORTEST_PATH);
      break;
  }

  switch (direction_) {
    case Direction::BOTH:
      builder->setDirection(capnp::EdgeAtom::Direction::BOTH);
      break;
    case Direction::IN:
      builder->setDirection(capnp::EdgeAtom::Direction::IN);
      break;
    case Direction::OUT:
      builder->setDirection(capnp::EdgeAtom::Direction::OUT);
      break;
  }

  ::capnp::List<storage::capnp::Common>::Builder common_builders =
      builder->initEdgeTypes(edge_types_.size());
  for (size_t i = 0; i < edge_types_.size(); ++i) {
    auto common_builder = common_builders[i];
    edge_types_[i].Save(common_builder);
  }

  ::capnp::List<capnp::EdgeAtom::Entry>::Builder map_builder =
      builder->initProperties(properties_.size());
  size_t i = 0;
  for (auto &entry : properties_) {
    auto entry_builder = map_builder[i];
    auto key_builder = entry_builder.getKey();
    key_builder.setFirst(entry.first.first);
    auto storage_property_builder = key_builder.getSecond();
    entry.first.second.Save(storage_property_builder);
    auto value_builder = entry_builder.getValue();
    if (entry.second) entry.second->Save(&value_builder, saved_uids);
    ++i;
  }

  if (lower_bound_) {
    auto lb_builder = builder->getLowerBound();
    lower_bound_->Save(&lb_builder, saved_uids);
  }
  if (upper_bound_) {
    auto ub_builder = builder->getUpperBound();
    upper_bound_->Save(&ub_builder, saved_uids);
  }

  auto filter_builder = builder->initFilterLambda();
  SaveLambda(filter_lambda_, &filter_builder, saved_uids);
  auto weight_builder = builder->initWeightLambda();
  SaveLambda(weight_lambda_, &weight_builder, saved_uids);

  if (total_weight_) {
    auto total_weight_builder = builder->getTotalWeight();
    total_weight_->Save(&total_weight_builder, saved_uids);
  }
}

void LoadLambda(capnp::EdgeAtom::Lambda::Reader &reader,
                query::EdgeAtom::Lambda &lambda, AstTreeStorage *storage,
                std::vector<int> *loaded_uids) {
  if (reader.hasInnerEdge()) {
    const auto ie_reader = reader.getInnerEdge();
    lambda.inner_edge =
        dynamic_cast<Identifier *>(storage->Load(ie_reader, loaded_uids));
  }
  if (reader.hasInnerNode()) {
    const auto in_reader = reader.getInnerNode();
    lambda.inner_node =
        dynamic_cast<Identifier *>(storage->Load(in_reader, loaded_uids));
  }
  if (reader.hasExpression()) {
    const auto expr_reader = reader.getExpression();
    lambda.expression =
        dynamic_cast<Expression *>(storage->Load(expr_reader, loaded_uids));
  }
}

void EdgeAtom::Load(const capnp::Tree::Reader &base_reader,
                    AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  PatternAtom::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getPatternAtom().getEdgeAtom();
  switch (reader.getType()) {
    case capnp::EdgeAtom::Type::BREADTH_FIRST:
      type_ = Type::BREADTH_FIRST;
      break;
    case capnp::EdgeAtom::Type::DEPTH_FIRST:
      type_ = Type::DEPTH_FIRST;
      break;
    case capnp::EdgeAtom::Type::SINGLE:
      type_ = Type::SINGLE;
      break;
    case capnp::EdgeAtom::Type::WEIGHTED_SHORTEST_PATH:
      type_ = Type::WEIGHTED_SHORTEST_PATH;
      break;
  }

  switch (reader.getDirection()) {
    case capnp::EdgeAtom::Direction::BOTH:
      direction_ = Direction::BOTH;
      break;
    case capnp::EdgeAtom::Direction::IN:
      direction_ = Direction::IN;
      break;
    case capnp::EdgeAtom::Direction::OUT:
      direction_ = Direction::OUT;
      break;
  }

  if (reader.hasTotalWeight()) {
    const auto id_reader = reader.getTotalWeight();
    total_weight_ =
        dynamic_cast<Identifier *>(storage->Load(id_reader, loaded_uids));
  }
  if (reader.hasLowerBound()) {
    const auto lb_reader = reader.getLowerBound();
    lower_bound_ =
        dynamic_cast<Expression *>(storage->Load(lb_reader, loaded_uids));
  }
  if (reader.hasUpperBound()) {
    const auto ub_reader = reader.getUpperBound();
    upper_bound_ =
        dynamic_cast<Expression *>(storage->Load(ub_reader, loaded_uids));
  }
  auto filter_reader = reader.getFilterLambda();
  LoadLambda(filter_reader, filter_lambda_, storage, loaded_uids);
  auto weight_reader = reader.getWeightLambda();
  LoadLambda(weight_reader, weight_lambda_, storage, loaded_uids);

  for (auto entry_reader : reader.getProperties()) {
    auto key_pair_reader = entry_reader.getKey();
    auto key_first = key_pair_reader.getFirst().cStr();
    auto storage_property_reader = key_pair_reader.getSecond();
    storage::Property property;
    property.Load(storage_property_reader);
    const auto value_reader = entry_reader.getValue();
    auto value = storage->Load(value_reader, loaded_uids);
    auto key = std::make_pair(key_first, property);
    // TODO Check if expression is null?
    properties_.emplace(key, dynamic_cast<Expression *>(value));
  }

  for (auto edge_type_reader : reader.getEdgeTypes()) {
    storage::EdgeType edge_type;
    edge_type.Load(edge_type_reader);
    edge_types_.push_back(edge_type);
  }
}

EdgeAtom *EdgeAtom::Construct(const capnp::EdgeAtom::Reader &reader,
                              AstTreeStorage *storage) {
  return storage->Create<EdgeAtom>();
}

// Query
void Query::Save(capnp::Tree::Builder *tree_builder,
                 std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto builder = tree_builder->initQuery();
  Save(&builder, saved_uids);
  AddToSaved(saved_uids);
}

void Query::Save(capnp::Query::Builder *builder, std::vector<int> *saved_uids) {
  if (single_query_) {
    auto sq_builder = builder->initSingleQuery();
    single_query_->Save(&sq_builder, saved_uids);
  }
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder->initCypherUnions(cypher_unions_.size());
  for (size_t i = 0; i < cypher_unions_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    cypher_unions_[i]->Save(&tree_builder, saved_uids);
  }
}

void Query::Load(const capnp::Tree::Reader &reader, AstTreeStorage *storage,
                 std::vector<int> *loaded_uids) {
  Tree::Load(reader, storage, loaded_uids);
  auto query_reader = reader.getQuery();
  if (query_reader.hasSingleQuery()) {
    const auto sq_reader = query_reader.getSingleQuery();
    single_query_ =
        dynamic_cast<SingleQuery *>(storage->Load(sq_reader, loaded_uids));
  }
  for (const auto tree_reader : query_reader.getCypherUnions()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    cypher_unions_.push_back(dynamic_cast<CypherUnion *>(tree));
  }
}

// SingleQuery
void SingleQuery::Save(capnp::Tree::Builder *tree_builder,
                       std::vector<int> *saved_uids) {
  Tree::Save(tree_builder, saved_uids);
  if (IsSaved(*saved_uids)) {
    return;
  }
  auto builder = tree_builder->initSingleQuery();
  Save(&builder, saved_uids);
  AddToSaved(saved_uids);
}

void SingleQuery::Save(capnp::SingleQuery::Builder *builder,
                       std::vector<int> *saved_uids) {
  ::capnp::List<capnp::Tree>::Builder tree_builders =
      builder->initClauses(clauses_.size());
  for (size_t i = 0; i < clauses_.size(); ++i) {
    auto tree_builder = tree_builders[i];
    clauses_[i]->Save(&tree_builder, saved_uids);
  }
}

void SingleQuery::Load(const capnp::Tree::Reader &base_reader,
                       AstTreeStorage *storage, std::vector<int> *loaded_uids) {
  Tree::Load(base_reader, storage, loaded_uids);
  auto reader = base_reader.getSingleQuery();
  for (const auto tree_reader : reader.getClauses()) {
    auto tree = storage->Load(tree_reader, loaded_uids);
    clauses_.push_back(dynamic_cast<Clause *>(tree));
  }
}

SingleQuery *SingleQuery::Construct(const capnp::SingleQuery::Reader &reader,
                                    AstTreeStorage *storage) {
  return storage->Create<SingleQuery>();
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
