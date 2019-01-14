#include <algorithm>
#include <climits>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include <antlr4-runtime.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "communication/rpc/serialization.hpp"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_serialization.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/stripped.hpp"
#include "query/typed_value.hpp"

#include "capnp/message.h"

namespace {

using namespace query;
using namespace query::frontend;
using query::TypedValue;
using testing::ElementsAre;
using testing::Pair;
using testing::UnorderedElementsAre;

// Base class for all test types
class Base {
 public:
  explicit Base(const std::string &query) : query_string_(query) {}

  ParsingContext context_;
  Parameters parameters_;
  std::string query_string_;

  virtual PropertyIx Prop(const std::string &prop_name) = 0;

  virtual LabelIx Label(const std::string &label_name) = 0;

  virtual EdgeTypeIx EdgeType(const std::string &edge_type_name) = 0;

  TypedValue LiteralValue(Expression *expression) {
    if (context_.is_query_cached) {
      auto *param_lookup = dynamic_cast<ParameterLookup *>(expression);
      return parameters_.AtTokenPosition(param_lookup->token_position_);
    } else {
      auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
      return literal->value_;
    }
  }

  void CheckLiteral(Expression *expression, const TypedValue &expected,
                    const std::experimental::optional<int> &token_position =
                        std::experimental::nullopt) {
    TypedValue value;
    if (!expected.IsNull() && context_.is_query_cached) {
      auto *param_lookup = dynamic_cast<ParameterLookup *>(expression);
      ASSERT_TRUE(param_lookup);
      if (token_position)
        EXPECT_EQ(param_lookup->token_position_, *token_position);
      value = parameters_.AtTokenPosition(param_lookup->token_position_);
    } else {
      auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
      ASSERT_TRUE(literal);
      if (token_position) ASSERT_EQ(literal->token_position_, *token_position);
      value = literal->value_;
    }
    EXPECT_TRUE(TypedValue::BoolEqual{}(value, expected));
  }
};

class CapnpAstGenerator : public Base {
 public:
  explicit CapnpAstGenerator(const std::string &query) : Base(query) {
    ::frontend::opencypher::Parser parser(query);
    AstStorage tmp_storage;
    CypherMainVisitor visitor(context_, &tmp_storage);
    visitor.visit(parser.tree());

    ::capnp::MallocMessageBuilder message;
    {
      query::capnp::Tree::Builder builder =
          message.initRoot<query::capnp::Tree>();
      std::vector<int> saved_uids;
      Save(*visitor.query(), &builder, &saved_uids);
    }

    {
      const query::capnp::Tree::Reader reader =
          message.getRoot<query::capnp::Tree>();
      std::vector<int> loaded_uids;
      query_ = dynamic_cast<Query *>(Load(&storage_, reader, &loaded_uids));
    }
  }

  PropertyIx Prop(const std::string &prop_name) override {
    return storage_.GetPropertyIx(prop_name);
  }

  LabelIx Label(const std::string &name) override {
    return storage_.GetLabelIx(name);
  }

  EdgeTypeIx EdgeType(const std::string &name) override {
    return storage_.GetEdgeTypeIx(name);
  }

  AstStorage storage_;
  Query *query_;
};

class SlkAstGenerator : public Base {
 public:
  explicit SlkAstGenerator(const std::string &query) : Base(query) {
    ::frontend::opencypher::Parser parser(query);
    AstStorage tmp_storage;
    CypherMainVisitor visitor(context_, &tmp_storage);
    visitor.visit(parser.tree());

    slk::Builder builder;
    {
      std::vector<int32_t> saved_uids;
      SaveAstPointer(visitor.query(), &builder, &saved_uids);
    }

    {
      slk::Reader reader(builder.data(), builder.size());
      std::vector<int32_t> loaded_uids;
      query_ = LoadAstPointer<Query>(&storage_, &reader, &loaded_uids);
    }
  }

  PropertyIx Prop(const std::string &prop_name) override {
    return storage_.GetPropertyIx(prop_name);
  }

  LabelIx Label(const std::string &name) override {
    return storage_.GetLabelIx(name);
  }

  EdgeTypeIx EdgeType(const std::string &name) override {
    return storage_.GetEdgeTypeIx(name);
  }

  AstStorage storage_;
  Query *query_;
};

template <typename T>
class CypherMainVisitorTest : public ::testing::Test {};

typedef ::testing::Types<CapnpAstGenerator, SlkAstGenerator> AstGeneratorTypes;

TYPED_TEST_CASE(CypherMainVisitorTest, AstGeneratorTypes);

TYPED_TEST(CypherMainVisitorTest, SyntaxException) {
  ASSERT_THROW(TypeParam("CREATE ()-[*1....2]-()"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, SyntaxExceptionOnTrailingText) {
  ASSERT_THROW(TypeParam("RETURN 2 + 2 mirko"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, PropertyLookup) {
  TypeParam ast_generator("RETURN n.x");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *property_lookup = dynamic_cast<PropertyLookup *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(property_lookup->expression_);
  auto identifier = dynamic_cast<Identifier *>(property_lookup->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_EQ(property_lookup->property_, ast_generator.Prop("x"));
}

TYPED_TEST(CypherMainVisitorTest, LabelsTest) {
  TypeParam ast_generator("RETURN n:x:y");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(labels_test->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_,
              ElementsAre(ast_generator.Label("x"), ast_generator.Label("y")));
}

TYPED_TEST(CypherMainVisitorTest, EscapedLabel) {
  TypeParam ast_generator("RETURN n:`l-$\"'ab``e````l`");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_,
              ElementsAre(ast_generator.Label("l-$\"'ab`e``l")));
}

TYPED_TEST(CypherMainVisitorTest, KeywordLabel) {
  for (const auto &label : {"DeLete", "UsER"}) {
    TypeParam ast_generator(fmt::format("RETURN n:{}", label));
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
    auto *labels_test = dynamic_cast<LabelsTest *>(
        return_clause->body_.named_expressions[0]->expression_);
    auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
    ASSERT_EQ(identifier->name_, "n");
    ASSERT_THAT(labels_test->labels_, ElementsAre(ast_generator.Label(label)));
  }
}

TYPED_TEST(CypherMainVisitorTest, HexLetterLabel) {
  TypeParam ast_generator("RETURN n:a");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  EXPECT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_, ElementsAre(ast_generator.Label("a")));
}

TYPED_TEST(CypherMainVisitorTest, ReturnNoDistinctNoBagSemantics) {
  TypeParam ast_generator("RETURN x");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.order_by.size(), 0U);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
  ASSERT_FALSE(return_clause->body_.limit);
  ASSERT_FALSE(return_clause->body_.skip);
  ASSERT_FALSE(return_clause->body_.distinct);
}

TYPED_TEST(CypherMainVisitorTest, ReturnDistinct) {
  TypeParam ast_generator("RETURN DISTINCT x");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.distinct);
}

TYPED_TEST(CypherMainVisitorTest, ReturnLimit) {
  TypeParam ast_generator("RETURN x LIMIT 5");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.limit);
  ast_generator.CheckLiteral(return_clause->body_.limit, 5);
}

TYPED_TEST(CypherMainVisitorTest, ReturnSkip) {
  TypeParam ast_generator("RETURN x SKIP 5");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.skip);
  ast_generator.CheckLiteral(return_clause->body_.skip, 5);
}

TYPED_TEST(CypherMainVisitorTest, ReturnOrderBy) {
  TypeParam ast_generator("RETURN x, y, z ORDER BY z ASC, x, y DESC");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.order_by.size(), 3U);
  std::vector<std::pair<Ordering, std::string>> ordering;
  for (const auto &sort_item : return_clause->body_.order_by) {
    auto *identifier = dynamic_cast<Identifier *>(sort_item.expression);
    ordering.emplace_back(sort_item.ordering, identifier->name_);
  }
  ASSERT_THAT(ordering, UnorderedElementsAre(Pair(Ordering::ASC, "z"),
                                             Pair(Ordering::ASC, "x"),
                                             Pair(Ordering::DESC, "y")));
}

TYPED_TEST(CypherMainVisitorTest, ReturnNamedIdentifier) {
  TypeParam ast_generator("RETURN var AS var5");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  auto *named_expr = return_clause->body_.named_expressions[0];
  ASSERT_EQ(named_expr->name_, "var5");
  auto *identifier = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_EQ(identifier->name_, "var");
}

TYPED_TEST(CypherMainVisitorTest, ReturnAsterisk) {
  TypeParam ast_generator("RETURN *");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 0U);
}

TYPED_TEST(CypherMainVisitorTest, IntegerLiteral) {
  TypeParam ast_generator("RETURN 42");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 42, 1);
}

TYPED_TEST(CypherMainVisitorTest, IntegerLiteralTooLarge) {
  ASSERT_THROW(TypeParam("RETURN 10000000000000000000000000"),
               SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, BooleanLiteralTrue) {
  TypeParam ast_generator("RETURN TrUe");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, true, 1);
}

TYPED_TEST(CypherMainVisitorTest, BooleanLiteralFalse) {
  TypeParam ast_generator("RETURN faLSE");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, false, 1);
}

TYPED_TEST(CypherMainVisitorTest, NullLiteral) {
  TypeParam ast_generator("RETURN nULl");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, TypedValue::Null,
      1);
}

TYPED_TEST(CypherMainVisitorTest, ParenthesizedExpression) {
  TypeParam ast_generator("RETURN (2)");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, OrOperator) {
  TypeParam ast_generator("RETURN true Or false oR n");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *or_operator2 = dynamic_cast<OrOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(or_operator2);
  auto *or_operator1 = dynamic_cast<OrOperator *>(or_operator2->expression1_);
  ASSERT_TRUE(or_operator1);
  ast_generator.CheckLiteral(or_operator1->expression1_, true);
  ast_generator.CheckLiteral(or_operator1->expression2_, false);
  auto *operand3 = dynamic_cast<Identifier *>(or_operator2->expression2_);
  ASSERT_TRUE(operand3);
  ASSERT_EQ(operand3->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, XorOperator) {
  TypeParam ast_generator("RETURN true xOr false");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *xor_operator = dynamic_cast<XorOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(xor_operator->expression1_, true);
  ast_generator.CheckLiteral(xor_operator->expression2_, false);
}

TYPED_TEST(CypherMainVisitorTest, AndOperator) {
  TypeParam ast_generator("RETURN true and false");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *and_operator = dynamic_cast<AndOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(and_operator->expression1_, true);
  ast_generator.CheckLiteral(and_operator->expression2_, false);
}

TYPED_TEST(CypherMainVisitorTest, AdditionSubtractionOperators) {
  TypeParam ast_generator("RETURN 1 - 2 + 3");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *addition_operator = dynamic_cast<AdditionOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(addition_operator);
  auto *subtraction_operator =
      dynamic_cast<SubtractionOperator *>(addition_operator->expression1_);
  ASSERT_TRUE(subtraction_operator);
  ast_generator.CheckLiteral(subtraction_operator->expression1_, 1);
  ast_generator.CheckLiteral(subtraction_operator->expression2_, 2);
  ast_generator.CheckLiteral(addition_operator->expression2_, 3);
}

TYPED_TEST(CypherMainVisitorTest, MulitplicationOperator) {
  TypeParam ast_generator("RETURN 2 * 3");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *mult_operator = dynamic_cast<MultiplicationOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(mult_operator->expression1_, 2);
  ast_generator.CheckLiteral(mult_operator->expression2_, 3);
}

TYPED_TEST(CypherMainVisitorTest, DivisionOperator) {
  TypeParam ast_generator("RETURN 2 / 3");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *div_operator = dynamic_cast<DivisionOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(div_operator->expression1_, 2);
  ast_generator.CheckLiteral(div_operator->expression2_, 3);
}

TYPED_TEST(CypherMainVisitorTest, ModOperator) {
  TypeParam ast_generator("RETURN 2 % 3");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *mod_operator = dynamic_cast<ModOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(mod_operator->expression1_, 2);
  ast_generator.CheckLiteral(mod_operator->expression2_, 3);
}

#define CHECK_COMPARISON(TYPE, VALUE1, VALUE2)                             \
  do {                                                                     \
    auto *and_operator = dynamic_cast<AndOperator *>(_operator);           \
    ASSERT_TRUE(and_operator);                                             \
    _operator = and_operator->expression1_;                                \
    auto *cmp_operator = dynamic_cast<TYPE *>(and_operator->expression2_); \
    ASSERT_TRUE(cmp_operator);                                             \
    ast_generator.CheckLiteral(cmp_operator->expression1_, VALUE1);        \
    ast_generator.CheckLiteral(cmp_operator->expression2_, VALUE2);        \
  } while (0)

TYPED_TEST(CypherMainVisitorTest, ComparisonOperators) {
  TypeParam ast_generator("RETURN 2 = 3 != 4 <> 5 < 6 > 7 <= 8 >= 9");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  Expression *_operator =
      return_clause->body_.named_expressions[0]->expression_;
  CHECK_COMPARISON(GreaterEqualOperator, 8, 9);
  CHECK_COMPARISON(LessEqualOperator, 7, 8);
  CHECK_COMPARISON(GreaterOperator, 6, 7);
  CHECK_COMPARISON(LessOperator, 5, 6);
  CHECK_COMPARISON(NotEqualOperator, 4, 5);
  CHECK_COMPARISON(NotEqualOperator, 3, 4);
  auto *cmp_operator = dynamic_cast<EqualOperator *>(_operator);
  ASSERT_TRUE(cmp_operator);
  ast_generator.CheckLiteral(cmp_operator->expression1_, 2);
  ast_generator.CheckLiteral(cmp_operator->expression2_, 3);
}

#undef CHECK_COMPARISON

TYPED_TEST(CypherMainVisitorTest, ListIndexing) {
  TypeParam ast_generator("RETURN [1,2,3] [ 2 ]");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *list_index_op = dynamic_cast<SubscriptOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_index_op);
  auto *list = dynamic_cast<ListLiteral *>(list_index_op->expression1_);
  EXPECT_TRUE(list);
  ast_generator.CheckLiteral(list_index_op->expression2_, 2);
}

TYPED_TEST(CypherMainVisitorTest, ListSlicingOperatorNoBounds) {
  ASSERT_THROW(TypeParam("RETURN [1,2,3] [ .. ]"), SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, ListSlicingOperator) {
  TypeParam ast_generator("RETURN [1,2,3] [ .. 2 ]");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *list_slicing_op = dynamic_cast<ListSlicingOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_slicing_op);
  auto *list = dynamic_cast<ListLiteral *>(list_slicing_op->list_);
  EXPECT_TRUE(list);
  EXPECT_FALSE(list_slicing_op->lower_bound_);
  ast_generator.CheckLiteral(list_slicing_op->upper_bound_, 2);
}

TYPED_TEST(CypherMainVisitorTest, InListOperator) {
  TypeParam ast_generator("RETURN 5 IN [1,2]");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *in_list_operator = dynamic_cast<InListOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(in_list_operator);
  ast_generator.CheckLiteral(in_list_operator->expression1_, 5);
  auto *list = dynamic_cast<ListLiteral *>(in_list_operator->expression2_);
  ASSERT_TRUE(list);
}

TYPED_TEST(CypherMainVisitorTest, InWithListIndexing) {
  TypeParam ast_generator("RETURN 1 IN [[1,2]][0]");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *in_list_operator = dynamic_cast<InListOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(in_list_operator);
  ast_generator.CheckLiteral(in_list_operator->expression1_, 1);
  auto *list_indexing =
      dynamic_cast<SubscriptOperator *>(in_list_operator->expression2_);
  ASSERT_TRUE(list_indexing);
  auto *list = dynamic_cast<ListLiteral *>(list_indexing->expression1_);
  EXPECT_TRUE(list);
  ast_generator.CheckLiteral(list_indexing->expression2_, 0);
}

TYPED_TEST(CypherMainVisitorTest, CaseGenericForm) {
  TypeParam ast_generator(
      "RETURN CASE WHEN n < 10 THEN 1 WHEN n > 10 THEN 2 END");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(if_operator);
  auto *condition = dynamic_cast<LessOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  ast_generator.CheckLiteral(if_operator->then_expression_, 1);

  auto *if_operator2 =
      dynamic_cast<IfOperator *>(if_operator->else_expression_);
  ASSERT_TRUE(if_operator2);
  auto *condition2 = dynamic_cast<GreaterOperator *>(if_operator2->condition_);
  ASSERT_TRUE(condition2);
  ast_generator.CheckLiteral(if_operator2->then_expression_, 2);
  ast_generator.CheckLiteral(if_operator2->else_expression_, TypedValue::Null);
}

TYPED_TEST(CypherMainVisitorTest, CaseGenericFormElse) {
  TypeParam ast_generator("RETURN CASE WHEN n < 10 THEN 1 ELSE 2 END");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto *condition = dynamic_cast<LessOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  ast_generator.CheckLiteral(if_operator->then_expression_, 1);
  ast_generator.CheckLiteral(if_operator->else_expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, CaseSimpleForm) {
  TypeParam ast_generator("RETURN CASE 5 WHEN 10 THEN 1 END");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto *condition = dynamic_cast<EqualOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  ast_generator.CheckLiteral(condition->expression1_, 5);
  ast_generator.CheckLiteral(condition->expression2_, 10);
  ast_generator.CheckLiteral(if_operator->then_expression_, 1);
  ast_generator.CheckLiteral(if_operator->else_expression_, TypedValue::Null);
}

TYPED_TEST(CypherMainVisitorTest, IsNull) {
  TypeParam ast_generator("RETURN 2 iS NulL");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *is_type_operator = dynamic_cast<IsNullOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(is_type_operator->expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, IsNotNull) {
  TypeParam ast_generator("RETURN 2 iS nOT NulL");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *not_operator = dynamic_cast<NotOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto *is_type_operator =
      dynamic_cast<IsNullOperator *>(not_operator->expression_);
  ast_generator.CheckLiteral(is_type_operator->expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, NotOperator) {
  TypeParam ast_generator("RETURN not true");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *not_operator = dynamic_cast<NotOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(not_operator->expression_, true);
}

TYPED_TEST(CypherMainVisitorTest, UnaryMinusPlusOperators) {
  TypeParam ast_generator("RETURN -+5");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *unary_minus_operator = dynamic_cast<UnaryMinusOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(unary_minus_operator);
  auto *unary_plus_operator =
      dynamic_cast<UnaryPlusOperator *>(unary_minus_operator->expression_);
  ASSERT_TRUE(unary_plus_operator);
  ast_generator.CheckLiteral(unary_plus_operator->expression_, 5);
}

TYPED_TEST(CypherMainVisitorTest, Aggregation) {
  TypeParam ast_generator(
      "RETURN COUNT(a), MIN(b), MAX(c), SUM(d), AVG(e), COLLECT(f), COUNT(*)");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 7U);
  Aggregation::Op ops[] = {
      Aggregation::Op::COUNT, Aggregation::Op::MIN,
      Aggregation::Op::MAX,   Aggregation::Op::SUM,
      Aggregation::Op::AVG,   Aggregation::Op::COLLECT_LIST};
  std::string ids[] = {"a", "b", "c", "d", "e", "f"};
  for (int i = 0; i < 6; ++i) {
    auto *aggregation = dynamic_cast<Aggregation *>(
        return_clause->body_.named_expressions[i]->expression_);
    ASSERT_TRUE(aggregation);
    ASSERT_EQ(aggregation->op_, ops[i]);
    auto *identifier = dynamic_cast<Identifier *>(aggregation->expression1_);
    ASSERT_TRUE(identifier);
    ASSERT_EQ(identifier->name_, ids[i]);
  }
  auto *aggregation = dynamic_cast<Aggregation *>(
      return_clause->body_.named_expressions[6]->expression_);
  ASSERT_TRUE(aggregation);
  ASSERT_EQ(aggregation->op_, Aggregation::Op::COUNT);
  ASSERT_FALSE(aggregation->expression1_);
}

TYPED_TEST(CypherMainVisitorTest, UndefinedFunction) {
  ASSERT_THROW(TypeParam("RETURN "
                         "IHopeWeWillNeverHaveAwesomeMemgraphProcedureWithS"
                         "uchALongAndAwesomeNameSinceThisTestWouldFail(1)"),
               SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, Function) {
  TypeParam ast_generator("RETURN abs(n, 2)");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1);
  auto *function = dynamic_cast<Function *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(function);
  ASSERT_TRUE(function->function_);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralDoubleQuotes) {
  TypeParam ast_generator("RETURN \"mi'rko\"");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, "mi'rko", 1);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralSingleQuotes) {
  TypeParam ast_generator("RETURN 'mi\"rko'");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, "mi\"rko", 1);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedChars) {
  TypeParam ast_generator("RETURN '\\\\\\'\\\"\\b\\B\\f\\F\\n\\N\\r\\R\\t\\T'");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_,
      "\\'\"\b\b\f\f\n\n\r\r\t\t", 1);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedUtf16) {
  TypeParam ast_generator("RETURN '\\u221daaa\\u221daaa'");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_,
      u8"\u221daaa\u221daaa", 1);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedUtf16Error) {
  ASSERT_THROW(TypeParam("RETURN '\\U221daaa'"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedUtf32) {
  TypeParam ast_generator("RETURN '\\U0001F600aaaa\\U0001F600aaaaaaaa'");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_,
      u8"\U0001F600aaaa\U0001F600aaaaaaaa", 1);
}

TYPED_TEST(CypherMainVisitorTest, DoubleLiteral) {
  TypeParam ast_generator("RETURN 3.5");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 3.5, 1);
}

TYPED_TEST(CypherMainVisitorTest, DoubleLiteralExponent) {
  TypeParam ast_generator("RETURN 5e-1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 0.5, 1);
}

TYPED_TEST(CypherMainVisitorTest, ListLiteral) {
  TypeParam ast_generator("RETURN [3, [], 'johhny']");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *list_literal = dynamic_cast<ListLiteral *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_literal);
  ASSERT_EQ(3, list_literal->elements_.size());
  ast_generator.CheckLiteral(list_literal->elements_[0], 3);
  auto *elem_1 = dynamic_cast<ListLiteral *>(list_literal->elements_[1]);
  ASSERT_TRUE(elem_1);
  EXPECT_EQ(0, elem_1->elements_.size());
  ast_generator.CheckLiteral(list_literal->elements_[2], "johhny");
}

TYPED_TEST(CypherMainVisitorTest, MapLiteral) {
  TypeParam ast_generator("RETURN {a: 1, b: 'bla', c: [1, {a: 42}]}");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *map_literal = dynamic_cast<MapLiteral *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(map_literal);
  ASSERT_EQ(3, map_literal->elements_.size());
  ast_generator.CheckLiteral(map_literal->elements_[ast_generator.Prop("a")],
                             1);
  ast_generator.CheckLiteral(map_literal->elements_[ast_generator.Prop("b")],
                             "bla");
  auto *elem_2 = dynamic_cast<ListLiteral *>(
      map_literal->elements_[ast_generator.Prop("c")]);
  ASSERT_TRUE(elem_2);
  EXPECT_EQ(2, elem_2->elements_.size());
  auto *elem_2_1 = dynamic_cast<MapLiteral *>(elem_2->elements_[1]);
  ASSERT_TRUE(elem_2_1);
  EXPECT_EQ(1, elem_2_1->elements_.size());
}

TYPED_TEST(CypherMainVisitorTest, NodePattern) {
  TypeParam ast_generator(
      "MATCH (:label1:label2:label3 {a : 5, b : 10}) RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_FALSE(match->optional_);
  EXPECT_FALSE(match->where_);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_TRUE(match->patterns_[0]);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 1U);
  auto node = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node);
  ASSERT_TRUE(node->identifier_);
  EXPECT_EQ(node->identifier_->name_,
            CypherMainVisitor::kAnonPrefix + std::to_string(1));
  EXPECT_FALSE(node->identifier_->user_declared_);
  EXPECT_THAT(node->labels_,
              UnorderedElementsAre(ast_generator.Label("label1"),
                                   ast_generator.Label("label2"),
                                   ast_generator.Label("label3")));
  std::unordered_map<PropertyIx, int64_t> properties;
  for (auto x : node->properties_) {
    TypedValue value = ast_generator.LiteralValue(x.second);
    ASSERT_TRUE(value.type() == TypedValue::Type::Int);
    properties[x.first] = value.Value<int64_t>();
  }
  EXPECT_THAT(properties,
              UnorderedElementsAre(Pair(ast_generator.Prop("a"), 5),
                                   Pair(ast_generator.Prop("b"), 10)));
}

TYPED_TEST(CypherMainVisitorTest, PropertyMapSameKeyAppearsTwice) {
  EXPECT_THROW(TypeParam("MATCH ({a : 1, a : 2})"), SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, NodePatternIdentifier) {
  TypeParam ast_generator("MATCH (var) RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_FALSE(match->optional_);
  EXPECT_FALSE(match->where_);
  auto node = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node);
  ASSERT_TRUE(node->identifier_);
  EXPECT_EQ(node->identifier_->name_, "var");
  EXPECT_TRUE(node->identifier_->user_declared_);
  EXPECT_THAT(node->labels_, UnorderedElementsAre());
  EXPECT_THAT(node->properties_, UnorderedElementsAre());
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternNoDetails) {
  TypeParam ast_generator("MATCH ()--() RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_FALSE(match->optional_);
  EXPECT_FALSE(match->where_);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_TRUE(match->patterns_[0]);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *node1 = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node1);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(edge);
  auto *node2 = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[2]);
  ASSERT_TRUE(node2);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::BOTH);
  ASSERT_TRUE(edge->identifier_);
  EXPECT_THAT(edge->identifier_->name_,
              CypherMainVisitor::kAnonPrefix + std::to_string(2));
  EXPECT_FALSE(edge->identifier_->user_declared_);
}

// PatternPart in braces.
TYPED_TEST(CypherMainVisitorTest, PatternPartBraces) {
  TypeParam ast_generator("MATCH ((()--())) RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_FALSE(match->where_);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_TRUE(match->patterns_[0]);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *node1 = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node1);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(edge);
  auto *node2 = dynamic_cast<NodeAtom *>(match->patterns_[0]->atoms_[2]);
  ASSERT_TRUE(node2);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::BOTH);
  ASSERT_TRUE(edge->identifier_);
  EXPECT_THAT(edge->identifier_->name_,
              CypherMainVisitor::kAnonPrefix + std::to_string(2));
  EXPECT_FALSE(edge->identifier_->user_declared_);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternDetails) {
  TypeParam ast_generator(
      "MATCH ()<-[:type1|type2 {a : 5, b : 10}]-() RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_FALSE(match->optional_);
  EXPECT_FALSE(match->where_);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::IN);
  EXPECT_THAT(edge->edge_types_,
              UnorderedElementsAre(ast_generator.EdgeType("type1"),
                                   ast_generator.EdgeType("type2")));
  std::unordered_map<PropertyIx, int64_t> properties;
  for (auto x : edge->properties_) {
    TypedValue value = ast_generator.LiteralValue(x.second);
    ASSERT_TRUE(value.type() == TypedValue::Type::Int);
    properties[x.first] = value.Value<int64_t>();
  }
  EXPECT_THAT(properties,
              UnorderedElementsAre(Pair(ast_generator.Prop("a"), 5),
                                   Pair(ast_generator.Prop("b"), 10)));
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternVariable) {
  TypeParam ast_generator("MATCH ()-[var]->() RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_FALSE(match->optional_);
  EXPECT_FALSE(match->where_);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  ASSERT_TRUE(edge->identifier_);
  EXPECT_THAT(edge->identifier_->name_, "var");
  EXPECT_TRUE(edge->identifier_->user_declared_);
}

// Assert that match has a single pattern with a single edge atom and store it
// in edge parameter.
void AssertMatchSingleEdgeAtom(Match *match, EdgeAtom *&edge) {
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(edge);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternUnbounded) {
  TypeParam ast_generator("MATCH ()-[r*]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  EXPECT_EQ(edge->upper_bound_, nullptr);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternLowerBounded) {
  TypeParam ast_generator("MATCH ()-[r*42..]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  ast_generator.CheckLiteral(edge->lower_bound_, 42);
  EXPECT_EQ(edge->upper_bound_, nullptr);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternUpperBounded) {
  TypeParam ast_generator("MATCH ()-[r*..42]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  ast_generator.CheckLiteral(edge->upper_bound_, 42);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternLowerUpperBounded) {
  TypeParam ast_generator("MATCH ()-[r*24..42]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  ast_generator.CheckLiteral(edge->lower_bound_, 24);
  ast_generator.CheckLiteral(edge->upper_bound_, 42);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternFixedRange) {
  TypeParam ast_generator("MATCH ()-[r*42]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  ast_generator.CheckLiteral(edge->lower_bound_, 42);
  ast_generator.CheckLiteral(edge->upper_bound_, 42);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternFloatingUpperBound) {
  // [r*1...2] should be parsed as [r*1..0.2]
  TypeParam ast_generator("MATCH ()-[r*1...2]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  ast_generator.CheckLiteral(edge->lower_bound_, 1);
  ast_generator.CheckLiteral(edge->upper_bound_, 0.2);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternUnboundedWithProperty) {
  TypeParam ast_generator("MATCH ()-[r* {prop: 42}]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  EXPECT_EQ(edge->upper_bound_, nullptr);
  ast_generator.CheckLiteral(edge->properties_[ast_generator.Prop("prop")], 42);
}

TYPED_TEST(CypherMainVisitorTest,
           RelationshipPatternDotsUnboundedWithEdgeTypeProperty) {
  TypeParam ast_generator("MATCH ()-[r:edge_type*..{prop: 42}]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  EXPECT_EQ(edge->upper_bound_, nullptr);
  ast_generator.CheckLiteral(edge->properties_[ast_generator.Prop("prop")], 42);
  ASSERT_EQ(edge->edge_types_.size(), 1U);
  auto edge_type = ast_generator.EdgeType("edge_type");
  EXPECT_EQ(edge->edge_types_[0], edge_type);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternUpperBoundedWithProperty) {
  TypeParam ast_generator("MATCH ()-[r*..2{prop: 42}]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_EQ(edge->type_, EdgeAtom::Type::DEPTH_FIRST);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  ast_generator.CheckLiteral(edge->upper_bound_, 2);
  ast_generator.CheckLiteral(edge->properties_[ast_generator.Prop("prop")], 42);
}

TYPED_TEST(CypherMainVisitorTest, ReturnUnanemdIdentifier) {
  TypeParam ast_generator("RETURN var");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
  auto *named_expr = return_clause->body_.named_expressions[0];
  ASSERT_TRUE(named_expr);
  ASSERT_EQ(named_expr->name_, "var");
  auto *identifier = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "var");
  ASSERT_TRUE(identifier->user_declared_);
}

TYPED_TEST(CypherMainVisitorTest, Create) {
  TypeParam ast_generator("CREATE (n)");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *create = dynamic_cast<Create *>(single_query->clauses_[0]);
  ASSERT_TRUE(create);
  ASSERT_EQ(create->patterns_.size(), 1U);
  ASSERT_TRUE(create->patterns_[0]);
  ASSERT_EQ(create->patterns_[0]->atoms_.size(), 1U);
  auto node = dynamic_cast<NodeAtom *>(create->patterns_[0]->atoms_[0]);
  ASSERT_TRUE(node);
  ASSERT_TRUE(node->identifier_);
  ASSERT_EQ(node->identifier_->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, Delete) {
  TypeParam ast_generator("DELETE n, m");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *del = dynamic_cast<Delete *>(single_query->clauses_[0]);
  ASSERT_TRUE(del);
  ASSERT_FALSE(del->detach_);
  ASSERT_EQ(del->expressions_.size(), 2U);
  auto *identifier1 = dynamic_cast<Identifier *>(del->expressions_[0]);
  ASSERT_TRUE(identifier1);
  ASSERT_EQ(identifier1->name_, "n");
  auto *identifier2 = dynamic_cast<Identifier *>(del->expressions_[1]);
  ASSERT_TRUE(identifier2);
  ASSERT_EQ(identifier2->name_, "m");
}

TYPED_TEST(CypherMainVisitorTest, DeleteDetach) {
  TypeParam ast_generator("DETACH DELETE n");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *del = dynamic_cast<Delete *>(single_query->clauses_[0]);
  ASSERT_TRUE(del);
  ASSERT_TRUE(del->detach_);
  ASSERT_EQ(del->expressions_.size(), 1U);
  auto *identifier1 = dynamic_cast<Identifier *>(del->expressions_[0]);
  ASSERT_TRUE(identifier1);
  ASSERT_EQ(identifier1->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, OptionalMatchWhere) {
  TypeParam ast_generator("OPTIONAL MATCH (n) WHERE m RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_TRUE(match->optional_);
  ASSERT_TRUE(match->where_);
  auto *identifier = dynamic_cast<Identifier *>(match->where_->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "m");
}

TYPED_TEST(CypherMainVisitorTest, Set) {
  TypeParam ast_generator("SET a.x = b, c = d, e += f, g : h : i ");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 4U);

  {
    auto *set_property = dynamic_cast<SetProperty *>(single_query->clauses_[0]);
    ASSERT_TRUE(set_property);
    ASSERT_TRUE(set_property->property_lookup_);
    auto *identifier1 =
        dynamic_cast<Identifier *>(set_property->property_lookup_->expression_);
    ASSERT_TRUE(identifier1);
    ASSERT_EQ(identifier1->name_, "a");
    ASSERT_EQ(set_property->property_lookup_->property_,
              ast_generator.Prop("x"));
    auto *identifier2 = dynamic_cast<Identifier *>(set_property->expression_);
    ASSERT_EQ(identifier2->name_, "b");
  }

  {
    auto *set_properties_assignment =
        dynamic_cast<SetProperties *>(single_query->clauses_[1]);
    ASSERT_TRUE(set_properties_assignment);
    ASSERT_FALSE(set_properties_assignment->update_);
    ASSERT_TRUE(set_properties_assignment->identifier_);
    ASSERT_EQ(set_properties_assignment->identifier_->name_, "c");
    auto *identifier =
        dynamic_cast<Identifier *>(set_properties_assignment->expression_);
    ASSERT_EQ(identifier->name_, "d");
  }

  {
    auto *set_properties_update =
        dynamic_cast<SetProperties *>(single_query->clauses_[2]);
    ASSERT_TRUE(set_properties_update);
    ASSERT_TRUE(set_properties_update->update_);
    ASSERT_TRUE(set_properties_update->identifier_);
    ASSERT_EQ(set_properties_update->identifier_->name_, "e");
    auto *identifier =
        dynamic_cast<Identifier *>(set_properties_update->expression_);
    ASSERT_EQ(identifier->name_, "f");
  }

  {
    auto *set_labels = dynamic_cast<SetLabels *>(single_query->clauses_[3]);
    ASSERT_TRUE(set_labels);
    ASSERT_TRUE(set_labels->identifier_);
    ASSERT_EQ(set_labels->identifier_->name_, "g");
    ASSERT_THAT(set_labels->labels_,
                UnorderedElementsAre(ast_generator.Label("h"),
                                     ast_generator.Label("i")));
  }
}

TYPED_TEST(CypherMainVisitorTest, Remove) {
  TypeParam ast_generator("REMOVE a.x, g : h : i");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  {
    auto *remove_property =
        dynamic_cast<RemoveProperty *>(single_query->clauses_[0]);
    ASSERT_TRUE(remove_property);
    ASSERT_TRUE(remove_property->property_lookup_);
    auto *identifier1 = dynamic_cast<Identifier *>(
        remove_property->property_lookup_->expression_);
    ASSERT_TRUE(identifier1);
    ASSERT_EQ(identifier1->name_, "a");
    ASSERT_EQ(remove_property->property_lookup_->property_,
              ast_generator.Prop("x"));
  }
  {
    auto *remove_labels =
        dynamic_cast<RemoveLabels *>(single_query->clauses_[1]);
    ASSERT_TRUE(remove_labels);
    ASSERT_TRUE(remove_labels->identifier_);
    ASSERT_EQ(remove_labels->identifier_->name_, "g");
    ASSERT_THAT(remove_labels->labels_,
                UnorderedElementsAre(ast_generator.Label("h"),
                                     ast_generator.Label("i")));
  }
}

TYPED_TEST(CypherMainVisitorTest, With) {
  TypeParam ast_generator("WITH n AS m RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(single_query->clauses_[0]);
  ASSERT_TRUE(with);
  ASSERT_FALSE(with->body_.distinct);
  ASSERT_FALSE(with->body_.limit);
  ASSERT_FALSE(with->body_.skip);
  ASSERT_EQ(with->body_.order_by.size(), 0U);
  ASSERT_FALSE(with->where_);
  ASSERT_EQ(with->body_.named_expressions.size(), 1U);
  auto *named_expr = with->body_.named_expressions[0];
  ASSERT_EQ(named_expr->name_, "m");
  auto *identifier = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_EQ(identifier->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, WithNonAliasedExpression) {
  ASSERT_THROW(TypeParam("WITH n.x RETURN 1"), SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, WithNonAliasedVariable) {
  TypeParam ast_generator("WITH n RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(single_query->clauses_[0]);
  ASSERT_TRUE(with);
  ASSERT_EQ(with->body_.named_expressions.size(), 1U);
  auto *named_expr = with->body_.named_expressions[0];
  ASSERT_EQ(named_expr->name_, "n");
  auto *identifier = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_EQ(identifier->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, WithDistinct) {
  TypeParam ast_generator("WITH DISTINCT n AS m RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(single_query->clauses_[0]);
  ASSERT_TRUE(with->body_.distinct);
  ASSERT_FALSE(with->where_);
  ASSERT_EQ(with->body_.named_expressions.size(), 1U);
  auto *named_expr = with->body_.named_expressions[0];
  ASSERT_EQ(named_expr->name_, "m");
  auto *identifier = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_EQ(identifier->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, WithBag) {
  TypeParam ast_generator("WITH n as m ORDER BY m SKIP 1 LIMIT 2 RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(single_query->clauses_[0]);
  ASSERT_FALSE(with->body_.distinct);
  ASSERT_FALSE(with->where_);
  ASSERT_EQ(with->body_.named_expressions.size(), 1U);
  // No need to check contents of body. That is checked in RETURN clause tests.
  ASSERT_EQ(with->body_.order_by.size(), 1U);
  ASSERT_TRUE(with->body_.limit);
  ASSERT_TRUE(with->body_.skip);
}

TYPED_TEST(CypherMainVisitorTest, WithWhere) {
  TypeParam ast_generator("WITH n AS m WHERE k RETURN 1");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(single_query->clauses_[0]);
  ASSERT_TRUE(with);
  ASSERT_TRUE(with->where_);
  auto *identifier = dynamic_cast<Identifier *>(with->where_->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "k");
  ASSERT_EQ(with->body_.named_expressions.size(), 1U);
  auto *named_expr = with->body_.named_expressions[0];
  ASSERT_EQ(named_expr->name_, "m");
  auto *identifier2 = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_EQ(identifier2->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, WithAnonymousVariableCapture) {
  TypeParam ast_generator("WITH 5 as anon1 MATCH () return *");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 3U);
  auto *match = dynamic_cast<Match *>(single_query->clauses_[1]);
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  auto *pattern = match->patterns_[0];
  ASSERT_TRUE(pattern);
  ASSERT_EQ(pattern->atoms_.size(), 1U);
  auto *atom = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
  ASSERT_TRUE(atom);
  ASSERT_NE("anon1", atom->identifier_->name_);
}

TYPED_TEST(CypherMainVisitorTest, ClausesOrdering) {
  // Obviously some of the ridiculous combinations don't fail here, but they
  // will fail in semantic analysis or they make perfect sense as a part of
  // bigger query.
  TypeParam("RETURN 1");
  ASSERT_THROW(TypeParam("RETURN 1 RETURN 1"), SemanticException);
  ASSERT_THROW(TypeParam("RETURN 1 MATCH (n) RETURN n"), SemanticException);
  ASSERT_THROW(TypeParam("RETURN 1 DELETE n"), SemanticException);
  ASSERT_THROW(TypeParam("RETURN 1 MERGE (n)"), SemanticException);
  ASSERT_THROW(TypeParam("RETURN 1 WITH n AS m RETURN 1"), SemanticException);
  ASSERT_THROW(TypeParam("RETURN 1 AS n UNWIND n AS x RETURN x"),
               SemanticException);

  ASSERT_THROW(TypeParam("OPTIONAL MATCH (n) MATCH (m) RETURN n, m"),
               SemanticException);
  TypeParam("OPTIONAL MATCH (n) WITH n MATCH (m) RETURN n, m");
  TypeParam("OPTIONAL MATCH (n) OPTIONAL MATCH (m) RETURN n, m");
  TypeParam("MATCH (n) OPTIONAL MATCH (m) RETURN n, m");

  TypeParam("CREATE (n)");
  ASSERT_THROW(TypeParam("SET n:x MATCH (n) RETURN n"), SemanticException);
  TypeParam("REMOVE n.x SET n.x = 1");
  TypeParam("REMOVE n:L RETURN n");
  TypeParam("SET n.x = 1 WITH n AS m RETURN m");

  ASSERT_THROW(TypeParam("MATCH (n)"), SemanticException);
  TypeParam("MATCH (n) MATCH (n) RETURN n");
  TypeParam("MATCH (n) SET n = m");
  TypeParam("MATCH (n) RETURN n");
  TypeParam("MATCH (n) WITH n AS m RETURN m");

  ASSERT_THROW(TypeParam("WITH 1 AS n"), SemanticException);
  TypeParam("WITH 1 AS n WITH n AS m RETURN m");
  TypeParam("WITH 1 AS n RETURN n");
  TypeParam("WITH 1 AS n SET n += m");
  TypeParam("WITH 1 AS n MATCH (n) RETURN n");

  ASSERT_THROW(TypeParam("UNWIND [1,2,3] AS x"), SemanticException);
  ASSERT_THROW(TypeParam("CREATE (n) UNWIND [1,2,3] AS x RETURN x"),
               SemanticException);
  TypeParam("UNWIND [1,2,3] AS x CREATE (n) RETURN x");
  TypeParam("CREATE (n) WITH n UNWIND [1,2,3] AS x RETURN x");
}

TYPED_TEST(CypherMainVisitorTest, Merge) {
  TypeParam ast_generator(
      "MERGE (a) -[:r]- (b) ON MATCH SET a.x = b.x "
      "ON CREATE SET b :label ON MATCH SET b = a");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *merge = dynamic_cast<Merge *>(single_query->clauses_[0]);
  ASSERT_TRUE(merge);
  EXPECT_TRUE(dynamic_cast<Pattern *>(merge->pattern_));
  ASSERT_EQ(merge->on_match_.size(), 2U);
  EXPECT_TRUE(dynamic_cast<SetProperty *>(merge->on_match_[0]));
  EXPECT_TRUE(dynamic_cast<SetProperties *>(merge->on_match_[1]));
  ASSERT_EQ(merge->on_create_.size(), 1U);
  EXPECT_TRUE(dynamic_cast<SetLabels *>(merge->on_create_[0]));
}

TYPED_TEST(CypherMainVisitorTest, Unwind) {
  TypeParam ast_generator("UNWIND [1,2,3] AS elem RETURN elem");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *unwind = dynamic_cast<Unwind *>(single_query->clauses_[0]);
  ASSERT_TRUE(unwind);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[1]);
  EXPECT_TRUE(ret);
  ASSERT_TRUE(unwind->named_expression_);
  EXPECT_EQ(unwind->named_expression_->name_, "elem");
  auto *expr = unwind->named_expression_->expression_;
  ASSERT_TRUE(expr);
  ASSERT_TRUE(dynamic_cast<ListLiteral *>(expr));
}

TYPED_TEST(CypherMainVisitorTest, UnwindWithoutAsError) {
  EXPECT_THROW(TypeParam("UNWIND [1,2,3] RETURN 42"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, CreateIndex) {
  TypeParam ast_generator("Create InDeX oN :mirko(slavko)");
  auto *index_query = dynamic_cast<IndexQuery *>(ast_generator.query_);
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::CREATE);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko")};
  EXPECT_EQ(index_query->properties_, expected_properties);
}

TYPED_TEST(CypherMainVisitorTest, CreateUniqueIndex) {
  TypeParam ast_generator("Create unIqUe InDeX oN :mirko(slavko, pero)");
  auto *index_query = dynamic_cast<IndexQuery *>(ast_generator.query_);
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::CREATE_UNIQUE);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko"),
                                              ast_generator.Prop("pero")};
  ASSERT_EQ(index_query->properties_, expected_properties);
}

TYPED_TEST(CypherMainVisitorTest, CreateUniqueIndexWithoutProperties) {
  EXPECT_THROW(TypeParam ast_generator("Create unIqUe InDeX oN :mirko()"),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, CreateUniqueIndexWithSingleProperty) {
  TypeParam ast_generator("Create unIqUe InDeX oN :mirko(slavko)");
  auto *index_query = dynamic_cast<IndexQuery *>(ast_generator.query_);
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::CREATE_UNIQUE);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko")};
  ASSERT_EQ(index_query->properties_, expected_properties);
}

TYPED_TEST(CypherMainVisitorTest, DropIndex) {
  TypeParam ast_generator("dRoP InDeX oN :mirko(slavko)");
  auto *index_query = dynamic_cast<IndexQuery *>(ast_generator.query_);
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::DROP);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko")};
  EXPECT_EQ(index_query->properties_, expected_properties);
}

TYPED_TEST(CypherMainVisitorTest, DropIndexWithoutProperties) {
  EXPECT_THROW(TypeParam ast_generator("dRoP InDeX oN :mirko()"),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, DropIndexWithMultipleProperties) {
  EXPECT_THROW(TypeParam ast_generator("dRoP InDeX oN :mirko(slavko, pero)"),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, ReturnAll) {
  { EXPECT_THROW(TypeParam("RETURN all(x in [1,2,3])"), SyntaxException); }
  {
    TypeParam ast_generator("RETURN all(x IN [1,2,3] WHERE x = 2)");
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
    auto *all =
        dynamic_cast<All *>(ret->body_.named_expressions[0]->expression_);
    ASSERT_TRUE(all);
    EXPECT_EQ(all->identifier_->name_, "x");
    auto *list_literal = dynamic_cast<ListLiteral *>(all->list_expression_);
    EXPECT_TRUE(list_literal);
    auto *eq = dynamic_cast<EqualOperator *>(all->where_->expression_);
    EXPECT_TRUE(eq);
  }
}

TYPED_TEST(CypherMainVisitorTest, ReturnSingle) {
  { EXPECT_THROW(TypeParam("RETURN single(x in [1,2,3])"), SyntaxException); }
  TypeParam ast_generator("RETURN single(x IN [1,2,3] WHERE x = 2)");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(ret);
  ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
  auto *single =
      dynamic_cast<Single *>(ret->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(single);
  EXPECT_EQ(single->identifier_->name_, "x");
  auto *list_literal = dynamic_cast<ListLiteral *>(single->list_expression_);
  EXPECT_TRUE(list_literal);
  auto *eq = dynamic_cast<EqualOperator *>(single->where_->expression_);
  EXPECT_TRUE(eq);
}

TYPED_TEST(CypherMainVisitorTest, ReturnReduce) {
  TypeParam ast_generator("RETURN reduce(sum = 0, x IN [1,2,3] | sum + x)");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(ret);
  ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
  auto *reduce =
      dynamic_cast<Reduce *>(ret->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(reduce);
  EXPECT_EQ(reduce->accumulator_->name_, "sum");
  ast_generator.CheckLiteral(reduce->initializer_, 0);
  EXPECT_EQ(reduce->identifier_->name_, "x");
  auto *list_literal = dynamic_cast<ListLiteral *>(reduce->list_);
  EXPECT_TRUE(list_literal);
  auto *add = dynamic_cast<AdditionOperator *>(reduce->expression_);
  EXPECT_TRUE(add);
}

TYPED_TEST(CypherMainVisitorTest, ReturnExtract) {
  TypeParam ast_generator("RETURN extract(x IN [1,2,3] | sum + x)");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(ret);
  ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
  auto *extract =
      dynamic_cast<Extract *>(ret->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(extract);
  EXPECT_EQ(extract->identifier_->name_, "x");
  auto *list_literal = dynamic_cast<ListLiteral *>(extract->list_);
  EXPECT_TRUE(list_literal);
  auto *add = dynamic_cast<AdditionOperator *>(extract->expression_);
  EXPECT_TRUE(add);
}

TYPED_TEST(CypherMainVisitorTest, MatchBfsReturn) {
  TypeParam ast_generator(
      "MATCH (n) -[r:type1|type2 *bfs..10 (e, n|e.prop = 42)]-> (m) RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *bfs = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(bfs);
  EXPECT_TRUE(bfs->IsVariable());
  EXPECT_EQ(bfs->direction_, EdgeAtom::Direction::OUT);
  EXPECT_THAT(bfs->edge_types_,
              UnorderedElementsAre(ast_generator.EdgeType("type1"),
                                   ast_generator.EdgeType("type2")));
  EXPECT_EQ(bfs->identifier_->name_, "r");
  EXPECT_EQ(bfs->filter_lambda_.inner_edge->name_, "e");
  EXPECT_TRUE(bfs->filter_lambda_.inner_edge->user_declared_);
  EXPECT_EQ(bfs->filter_lambda_.inner_node->name_, "n");
  EXPECT_TRUE(bfs->filter_lambda_.inner_node->user_declared_);
  ast_generator.CheckLiteral(bfs->upper_bound_, 10);
  auto *eq = dynamic_cast<EqualOperator *>(bfs->filter_lambda_.expression);
  ASSERT_TRUE(eq);
}

TYPED_TEST(CypherMainVisitorTest, MatchVariableLambdaSymbols) {
  TypeParam ast_generator("MATCH () -[*]- () RETURN *");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *var_expand = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(var_expand);
  ASSERT_TRUE(var_expand->IsVariable());
  EXPECT_FALSE(var_expand->filter_lambda_.inner_edge->user_declared_);
  EXPECT_FALSE(var_expand->filter_lambda_.inner_node->user_declared_);
}

TYPED_TEST(CypherMainVisitorTest, MatchWShortestReturn) {
  TypeParam ast_generator(
      "MATCH ()-[r:type1|type2 *wShortest 10 (we, wn | 42) total_weight "
      "(e, n | true)]->() RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *shortest = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(shortest);
  EXPECT_TRUE(shortest->IsVariable());
  EXPECT_EQ(shortest->type_, EdgeAtom::Type::WEIGHTED_SHORTEST_PATH);
  EXPECT_EQ(shortest->direction_, EdgeAtom::Direction::OUT);
  EXPECT_THAT(shortest->edge_types_,
              UnorderedElementsAre(ast_generator.EdgeType("type1"),
                                   ast_generator.EdgeType("type2")));
  ast_generator.CheckLiteral(shortest->upper_bound_, 10);
  EXPECT_FALSE(shortest->lower_bound_);
  EXPECT_EQ(shortest->identifier_->name_, "r");
  EXPECT_EQ(shortest->filter_lambda_.inner_edge->name_, "e");
  EXPECT_TRUE(shortest->filter_lambda_.inner_edge->user_declared_);
  EXPECT_EQ(shortest->filter_lambda_.inner_node->name_, "n");
  EXPECT_TRUE(shortest->filter_lambda_.inner_node->user_declared_);
  ast_generator.CheckLiteral(shortest->filter_lambda_.expression, true);
  EXPECT_EQ(shortest->weight_lambda_.inner_edge->name_, "we");
  EXPECT_TRUE(shortest->weight_lambda_.inner_edge->user_declared_);
  EXPECT_EQ(shortest->weight_lambda_.inner_node->name_, "wn");
  EXPECT_TRUE(shortest->weight_lambda_.inner_node->user_declared_);
  ast_generator.CheckLiteral(shortest->weight_lambda_.expression, 42);
  ASSERT_TRUE(shortest->total_weight_);
  EXPECT_EQ(shortest->total_weight_->name_, "total_weight");
  EXPECT_TRUE(shortest->total_weight_->user_declared_);
}

TYPED_TEST(CypherMainVisitorTest, MatchWShortestNoFilterReturn) {
  TypeParam ast_generator(
      "MATCH ()-[r:type1|type2 *wShortest 10 (we, wn | 42)]->() "
      "RETURN r");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(single_query->clauses_[0]);
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *shortest = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(shortest);
  EXPECT_TRUE(shortest->IsVariable());
  EXPECT_EQ(shortest->type_, EdgeAtom::Type::WEIGHTED_SHORTEST_PATH);
  EXPECT_EQ(shortest->direction_, EdgeAtom::Direction::OUT);
  EXPECT_THAT(shortest->edge_types_,
              UnorderedElementsAre(ast_generator.EdgeType("type1"),
                                   ast_generator.EdgeType("type2")));
  ast_generator.CheckLiteral(shortest->upper_bound_, 10);
  EXPECT_FALSE(shortest->lower_bound_);
  EXPECT_EQ(shortest->identifier_->name_, "r");
  EXPECT_FALSE(shortest->filter_lambda_.expression);
  EXPECT_FALSE(shortest->filter_lambda_.inner_edge->user_declared_);
  EXPECT_FALSE(shortest->filter_lambda_.inner_node->user_declared_);
  EXPECT_EQ(shortest->weight_lambda_.inner_edge->name_, "we");
  EXPECT_TRUE(shortest->weight_lambda_.inner_edge->user_declared_);
  EXPECT_EQ(shortest->weight_lambda_.inner_node->name_, "wn");
  EXPECT_TRUE(shortest->weight_lambda_.inner_node->user_declared_);
  ast_generator.CheckLiteral(shortest->weight_lambda_.expression, 42);
  ASSERT_TRUE(shortest->total_weight_);
  EXPECT_FALSE(shortest->total_weight_->user_declared_);
}

TYPED_TEST(CypherMainVisitorTest, SemanticExceptionOnWShortestLowerBound) {
  ASSERT_THROW(
      TypeParam("MATCH ()-[r *wShortest 10.. (e, n | 42)]-() RETURN r"),
      SemanticException);
  ASSERT_THROW(
      TypeParam("MATCH ()-[r *wShortest 10..20 (e, n | 42)]-() RETURN r"),
      SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, SemanticExceptionOnWShortestWithoutLambda) {
  ASSERT_THROW(TypeParam("MATCH ()-[r *wShortest]-() RETURN r"),
               SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, SemanticExceptionOnUnionTypeMix) {
  ASSERT_THROW(
      TypeParam("RETURN 5 as X UNION ALL RETURN 6 AS X UNION RETURN 7 AS X"),
      SemanticException);
  ASSERT_THROW(
      TypeParam("RETURN 5 as X UNION RETURN 6 AS X UNION ALL RETURN 7 AS X"),
      SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, Union) {
  TypeParam ast_generator("RETURN 5 AS X, 6 AS Y UNION RETURN 6 AS X, 5 AS Y");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);

  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.order_by.size(), 0U);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 2U);
  ASSERT_FALSE(return_clause->body_.limit);
  ASSERT_FALSE(return_clause->body_.skip);
  ASSERT_FALSE(return_clause->body_.distinct);

  ASSERT_EQ(query->cypher_unions_.size(), 1);
  auto *cypher_union = query->cypher_unions_.at(0);
  ASSERT_TRUE(cypher_union);
  ASSERT_TRUE(cypher_union->distinct_);
  ASSERT_TRUE(single_query = cypher_union->single_query_);
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.order_by.size(), 0U);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 2U);
  ASSERT_FALSE(return_clause->body_.limit);
  ASSERT_FALSE(return_clause->body_.skip);
  ASSERT_FALSE(return_clause->body_.distinct);
}

TYPED_TEST(CypherMainVisitorTest, UnionAll) {
  TypeParam ast_generator(
      "RETURN 5 AS X UNION ALL RETURN 6 AS X UNION ALL RETURN 7 AS X");
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.query_);
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);

  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.order_by.size(), 0U);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
  ASSERT_FALSE(return_clause->body_.limit);
  ASSERT_FALSE(return_clause->body_.skip);
  ASSERT_FALSE(return_clause->body_.distinct);

  ASSERT_EQ(query->cypher_unions_.size(), 2);

  auto *cypher_union = query->cypher_unions_.at(0);
  ASSERT_TRUE(cypher_union);
  ASSERT_FALSE(cypher_union->distinct_);
  ASSERT_TRUE(single_query = cypher_union->single_query_);
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.order_by.size(), 0U);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
  ASSERT_FALSE(return_clause->body_.limit);
  ASSERT_FALSE(return_clause->body_.skip);
  ASSERT_FALSE(return_clause->body_.distinct);

  cypher_union = query->cypher_unions_.at(1);
  ASSERT_TRUE(cypher_union);
  ASSERT_FALSE(cypher_union->distinct_);
  ASSERT_TRUE(single_query = cypher_union->single_query_);
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.order_by.size(), 0U);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
  ASSERT_FALSE(return_clause->body_.limit);
  ASSERT_FALSE(return_clause->body_.skip);
  ASSERT_FALSE(return_clause->body_.distinct);
}

template <typename AstGeneratorT>
void check_auth_query(std::string input, AuthQuery::Action action,
                      std::string user, std::string role,
                      std::string user_or_role,
                      std::experimental::optional<TypedValue> password,
                      std::vector<AuthQuery::Privilege> privileges) {
  AstGeneratorT ast_generator(input);
  auto *auth_query = dynamic_cast<AuthQuery *>(ast_generator.query_);
  ASSERT_TRUE(auth_query);
  EXPECT_EQ(auth_query->action_, action);
  EXPECT_EQ(auth_query->user_, user);
  EXPECT_EQ(auth_query->role_, role);
  EXPECT_EQ(auth_query->user_or_role_, user_or_role);
  ASSERT_EQ(static_cast<bool>(auth_query->password_),
            static_cast<bool>(password));
  if (password) {
    ast_generator.CheckLiteral(auth_query->password_, *password);
  }
  EXPECT_EQ(auth_query->privileges_, privileges);
}

TYPED_TEST(CypherMainVisitorTest, UserOrRoleName) {
  ASSERT_THROW(TypeParam("CREATE ROLE `us|er`"), SyntaxException);
  ASSERT_THROW(TypeParam("CREATE ROLE `us er`"), SyntaxException);
  check_auth_query<TypeParam>("CREATE ROLE `user`",
                              AuthQuery::Action::CREATE_ROLE, "", "user", "",
                              {}, {});
  check_auth_query<TypeParam>("CREATE ROLE us___er",
                              AuthQuery::Action::CREATE_ROLE, "", "us___er", "",
                              {}, {});
  check_auth_query<TypeParam>("CREATE ROLE `us+er`",
                              AuthQuery::Action::CREATE_ROLE, "", "us+er", "",
                              {}, {});
}

TYPED_TEST(CypherMainVisitorTest, CreateRole) {
  ASSERT_THROW(TypeParam("CREATE ROLE"), SyntaxException);
  check_auth_query<TypeParam>("CREATE ROLE rola",
                              AuthQuery::Action::CREATE_ROLE, "", "rola", "",
                              {}, {});
  ASSERT_THROW(TypeParam("CREATE ROLE lagano rolamo"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, DropRole) {
  ASSERT_THROW(TypeParam("DROP ROLE"), SyntaxException);
  check_auth_query<TypeParam>("DROP ROLE rola", AuthQuery::Action::DROP_ROLE,
                              "", "rola", "", {}, {});
  ASSERT_THROW(TypeParam("DROP ROLE lagano rolamo"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, ShowRoles) {
  ASSERT_THROW(TypeParam("SHOW ROLES ROLES"), SyntaxException);
  check_auth_query<TypeParam>("SHOW ROLES", AuthQuery::Action::SHOW_ROLES, "",
                              "", "", {}, {});
}

TYPED_TEST(CypherMainVisitorTest, CreateUser) {
  ASSERT_THROW(TypeParam("CREATE USER"), SyntaxException);
  ASSERT_THROW(TypeParam("CREATE USER 123"), SyntaxException);
  check_auth_query<TypeParam>("CREATE USER user",
                              AuthQuery::Action::CREATE_USER, "user", "", "",
                              {}, {});
  check_auth_query<TypeParam>("CREATE USER user IDENTIFIED BY 'password'",
                              AuthQuery::Action::CREATE_USER, "user", "", "",
                              "password", {});
  check_auth_query<TypeParam>("CREATE USER user IDENTIFIED BY ''",
                              AuthQuery::Action::CREATE_USER, "user", "", "",
                              "", {});
  check_auth_query<TypeParam>("CREATE USER user IDENTIFIED BY null",
                              AuthQuery::Action::CREATE_USER, "user", "", "",
                              TypedValue::Null, {});
  ASSERT_THROW(TypeParam("CRATE USER user IDENTIFIED BY password"),
               SyntaxException);
  ASSERT_THROW(TypeParam("CREATE USER user IDENTIFIED BY 5"), SyntaxException);
  ASSERT_THROW(TypeParam("CREATE USER user IDENTIFIED BY "), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, SetPassword) {
  ASSERT_THROW(TypeParam("SET PASSWORD FOR"), SyntaxException);
  ASSERT_THROW(TypeParam("SET PASSWORD FOR user "), SyntaxException);
  check_auth_query<TypeParam>("SET PASSWORD FOR user TO null",
                              AuthQuery::Action::SET_PASSWORD, "user", "", "",
                              TypedValue::Null, {});
  check_auth_query<TypeParam>("SET PASSWORD FOR user TO 'password'",
                              AuthQuery::Action::SET_PASSWORD, "user", "", "",
                              "password", {});
  ASSERT_THROW(TypeParam("SET PASSWORD FOR user To 5"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, DropUser) {
  ASSERT_THROW(TypeParam("DROP USER"), SyntaxException);
  check_auth_query<TypeParam>("DROP USER user", AuthQuery::Action::DROP_USER,
                              "user", "", "", {}, {});
  ASSERT_THROW(TypeParam("DROP USER lagano rolamo"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, ShowUsers) {
  ASSERT_THROW(TypeParam("SHOW USERS ROLES"), SyntaxException);
  check_auth_query<TypeParam>("SHOW USERS", AuthQuery::Action::SHOW_USERS, "",
                              "", "", {}, {});
}

TYPED_TEST(CypherMainVisitorTest, SetRole) {
  ASSERT_THROW(TypeParam("SET ROLE"), SyntaxException);
  ASSERT_THROW(TypeParam("SET ROLE user"), SyntaxException);
  ASSERT_THROW(TypeParam("SET ROLE FOR user"), SyntaxException);
  ASSERT_THROW(TypeParam("SET ROLE FOR user TO"), SyntaxException);
  check_auth_query<TypeParam>("SET ROLE FOR user TO role",
                              AuthQuery::Action::SET_ROLE, "user", "role", "",
                              {}, {});
  check_auth_query<TypeParam>("SET ROLE FOR user TO null",
                              AuthQuery::Action::SET_ROLE, "user", "null", "",
                              {}, {});
}

TYPED_TEST(CypherMainVisitorTest, ClearRole) {
  ASSERT_THROW(TypeParam("CLEAR ROLE"), SyntaxException);
  ASSERT_THROW(TypeParam("CLEAR ROLE user"), SyntaxException);
  ASSERT_THROW(TypeParam("CLEAR ROLE FOR user TO"), SyntaxException);
  check_auth_query<TypeParam>("CLEAR ROLE FOR user",
                              AuthQuery::Action::CLEAR_ROLE, "user", "", "", {},
                              {});
}

TYPED_TEST(CypherMainVisitorTest, GrantPrivilege) {
  ASSERT_THROW(TypeParam("GRANT"), SyntaxException);
  ASSERT_THROW(TypeParam("GRANT TO user"), SyntaxException);
  ASSERT_THROW(TypeParam("GRANT BLABLA TO user"), SyntaxException);
  ASSERT_THROW(TypeParam("GRANT MATCH, TO user"), SyntaxException);
  ASSERT_THROW(TypeParam("GRANT MATCH, BLABLA TO user"), SyntaxException);
  check_auth_query<TypeParam>("GRANT MATCH TO user",
                              AuthQuery::Action::GRANT_PRIVILEGE, "", "",
                              "user", {}, {AuthQuery::Privilege::MATCH});
  check_auth_query<TypeParam>(
      "GRANT MATCH, AUTH TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "",
      "user", {}, {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH});
}

TYPED_TEST(CypherMainVisitorTest, DenyPrivilege) {
  ASSERT_THROW(TypeParam("DENY"), SyntaxException);
  ASSERT_THROW(TypeParam("DENY TO user"), SyntaxException);
  ASSERT_THROW(TypeParam("DENY BLABLA TO user"), SyntaxException);
  ASSERT_THROW(TypeParam("DENY MATCH, TO user"), SyntaxException);
  ASSERT_THROW(TypeParam("DENY MATCH, BLABLA TO user"), SyntaxException);
  check_auth_query<TypeParam>("DENY MATCH TO user",
                              AuthQuery::Action::DENY_PRIVILEGE, "", "", "user",
                              {}, {AuthQuery::Privilege::MATCH});
  check_auth_query<TypeParam>(
      "DENY MATCH, AUTH TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "",
      "user", {}, {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH});
}

TYPED_TEST(CypherMainVisitorTest, RevokePrivilege) {
  ASSERT_THROW(TypeParam("REVOKE"), SyntaxException);
  ASSERT_THROW(TypeParam("REVOKE FROM user"), SyntaxException);
  ASSERT_THROW(TypeParam("REVOKE BLABLA FROM user"), SyntaxException);
  ASSERT_THROW(TypeParam("REVOKE MATCH, FROM user"), SyntaxException);
  ASSERT_THROW(TypeParam("REVOKE MATCH, BLABLA FROM user"), SyntaxException);
  check_auth_query<TypeParam>("REVOKE MATCH FROM user",
                              AuthQuery::Action::REVOKE_PRIVILEGE, "", "",
                              "user", {}, {AuthQuery::Privilege::MATCH});
  check_auth_query<TypeParam>(
      "REVOKE MATCH, AUTH FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "",
      "", "user", {},
      {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH});
  check_auth_query<TypeParam>(
      "REVOKE ALL PRIVILEGES FROM user", AuthQuery::Action::REVOKE_PRIVILEGE,
      "", "", "user", {},
      {AuthQuery::Privilege::CREATE, AuthQuery::Privilege::DELETE,
       AuthQuery::Privilege::MATCH, AuthQuery::Privilege::MERGE,
       AuthQuery::Privilege::SET, AuthQuery::Privilege::REMOVE,
       AuthQuery::Privilege::INDEX, AuthQuery::Privilege::AUTH,
       AuthQuery::Privilege::STREAM});
}

TYPED_TEST(CypherMainVisitorTest, ShowPrivileges) {
  ASSERT_THROW(TypeParam("SHOW PRIVILEGES FOR"), SyntaxException);
  check_auth_query<TypeParam>("SHOW PRIVILEGES FOR user",
                              AuthQuery::Action::SHOW_PRIVILEGES, "", "",
                              "user", {}, {});
  ASSERT_THROW(TypeParam("SHOW PRIVILEGES FOR user1, user2"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, ShowRoleForUser) {
  ASSERT_THROW(TypeParam("SHOW ROLE FOR "), SyntaxException);
  check_auth_query<TypeParam>("SHOW ROLE FOR user",
                              AuthQuery::Action::SHOW_ROLE_FOR_USER, "user", "",
                              "", {}, {});
  ASSERT_THROW(TypeParam("SHOW ROLE FOR user1, user2"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, ShowUsersForRole) {
  ASSERT_THROW(TypeParam("SHOW USERS FOR "), SyntaxException);
  check_auth_query<TypeParam>("SHOW USERS FOR role",
                              AuthQuery::Action::SHOW_USERS_FOR_ROLE, "",
                              "role", "", {}, {});
  ASSERT_THROW(TypeParam("SHOW USERS FOR role1, role2"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, CreateStream) {
  auto check_create_stream =
      [](std::string input, const std::string &stream_name,
         const std::string &stream_uri, const std::string &stream_topic,
         const std::string &transform_uri,
         std::experimental::optional<int64_t> batch_interval_in_ms,
         std::experimental::optional<int64_t> batch_size) {
        TypeParam ast_generator(input);
        auto *stream_query = dynamic_cast<StreamQuery *>(ast_generator.query_);
        ASSERT_TRUE(stream_query);
        EXPECT_EQ(stream_query->action_, StreamQuery::Action::CREATE_STREAM);
        EXPECT_EQ(stream_query->stream_name_, stream_name);
        ASSERT_TRUE(stream_query->stream_uri_);
        ast_generator.CheckLiteral(stream_query->stream_uri_,
                                   TypedValue(stream_uri));
        ASSERT_TRUE(stream_query->stream_topic_);
        ast_generator.CheckLiteral(stream_query->stream_topic_,
                                   TypedValue(stream_topic));
        ASSERT_TRUE(stream_query->transform_uri_);
        ast_generator.CheckLiteral(stream_query->transform_uri_,
                                   TypedValue(transform_uri));
        if (batch_interval_in_ms) {
          ASSERT_TRUE(stream_query->batch_interval_in_ms_);
          ast_generator.CheckLiteral(stream_query->batch_interval_in_ms_,
                                     TypedValue(*batch_interval_in_ms));
        } else {
          EXPECT_EQ(stream_query->batch_interval_in_ms_, nullptr);
        }
        if (batch_size) {
          ASSERT_TRUE(stream_query->batch_size_);
          ast_generator.CheckLiteral(stream_query->batch_size_,
                                     TypedValue(*batch_size));
        } else {
          EXPECT_EQ(stream_query->batch_size_, nullptr);
        }
      };

  check_create_stream(
      "CREATE STREAM stream AS LOAD DATA KAFKA 'localhost' "
      "WITH TOPIC 'tropika' "
      "WITH TRANSFORM 'localhost/test.py'",
      "stream", "localhost", "tropika", "localhost/test.py",
      std::experimental::nullopt, std::experimental::nullopt);

  check_create_stream(
      "CreaTE StreaM stream AS LOad daTA KAFKA 'localhost' "
      "WitH TopIC 'tropika' "
      "WITH TRAnsFORM 'localhost/test.py' bAtCH inTErvAL 168",
      "stream", "localhost", "tropika", "localhost/test.py", 168,
      std::experimental::nullopt);

  check_create_stream(
      "CreaTE StreaM stream AS LOad daTA KAFKA 'localhost' "
      "WITH TopIC 'tropika' "
      "WITH TRAnsFORM 'localhost/test.py' bAtCH SizE 17",
      "stream", "localhost", "tropika", "localhost/test.py",
      std::experimental::nullopt, 17);

  check_create_stream(
      "CreaTE StreaM stream AS LOad daTA KAFKA 'localhost' "
      "WitH TOPic 'tropika' "
      "WITH TRAnsFORM 'localhost/test.py' bAtCH inTErvAL 168 Batch SIze 17",
      "stream", "localhost", "tropika", "localhost/test.py", 168, 17);

  EXPECT_THROW(check_create_stream(
                   "CREATE STREAM stream AS LOAD DATA KAFKA 'localhost' "
                   "WITH TRANSFORM 'localhost/test.py' BATCH INTERVAL 'jedan' ",
                   "stream", "localhost", "tropika", "localhost/test.py", 168,
                   std::experimental::nullopt),
               SyntaxException);
  EXPECT_THROW(check_create_stream(
                   "CREATE STREAM stream AS LOAD DATA KAFKA 'localhost' "
                   "WITH TOPIC 'tropika' "
                   "WITH TRANSFORM 'localhost/test.py' BATCH SIZE 'jedan' ",
                   "stream", "localhost", "tropika", "localhost/test.py",
                   std::experimental::nullopt, 17),
               SyntaxException);
  EXPECT_THROW(check_create_stream(
                   "CREATE STREAM 123 AS LOAD DATA KAFKA 'localhost' "
                   "WITH TOPIC 'tropika' "
                   "WITH TRANSFORM 'localhost/test.py' BATCH INTERVAL 168 ",
                   "stream", "localhost", "tropika", "localhost/test.py", 168,
                   std::experimental::nullopt),
               SyntaxException);
  EXPECT_THROW(check_create_stream(
                   "CREATE STREAM stream AS LOAD DATA KAFKA localhost "
                   "WITH TOPIC 'tropika' "
                   "WITH TRANSFORM 'localhost/test.py'",
                   "stream", "localhost", "tropika", "localhost/test.py",
                   std::experimental::nullopt, std::experimental::nullopt),
               SyntaxException);
  EXPECT_THROW(check_create_stream(
                   "CREATE STREAM stream AS LOAD DATA KAFKA 'localhost' "
                   "WITH TOPIC 2"
                   "WITH TRANSFORM localhost/test.py BATCH INTERVAL 168 ",
                   "stream", "localhost", "tropika", "localhost/test.py", 168,
                   std::experimental::nullopt),
               SyntaxException);
  EXPECT_THROW(check_create_stream(
                   "CREATE STREAM stream AS LOAD DATA KAFKA 'localhost' "
                   "WITH TOPIC 'tropika'"
                   "WITH TRANSFORM localhost/test.py BATCH INTERVAL 168 ",
                   "stream", "localhost", "tropika", "localhost/test.py", 168,
                   std::experimental::nullopt),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, DropStream) {
  auto check_drop_stream = [](std::string input,
                              const std::string &stream_name) {
    TypeParam ast_generator(input);
    auto *stream_query = dynamic_cast<StreamQuery *>(ast_generator.query_);
    ASSERT_TRUE(stream_query);
    EXPECT_EQ(stream_query->action_, StreamQuery::Action::DROP_STREAM);
    EXPECT_EQ(stream_query->stream_name_, stream_name);
  };

  check_drop_stream("DRop stREAm stream", "stream");
  check_drop_stream("DRop stREAm strim", "strim");

  EXPECT_THROW(check_drop_stream("DROp sTREAM", ""), SyntaxException);

  EXPECT_THROW(check_drop_stream("DROP STreAM 123", "123"), SyntaxException);

  EXPECT_THROW(check_drop_stream("DroP STREAM '123'", "123"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, ShowStreams) {
  auto check_show_streams = [](std::string input) {
    TypeParam ast_generator(input);
    auto *stream_query = dynamic_cast<StreamQuery *>(ast_generator.query_);
    ASSERT_TRUE(stream_query);
    EXPECT_EQ(stream_query->action_, StreamQuery::Action::SHOW_STREAMS);
  };

  check_show_streams("SHOW STREAMS");

  EXPECT_THROW(check_show_streams("SHOW STREAMS lololo"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, StartStopStream) {
  auto check_start_stop_stream =
      [](std::string input, const std::string &stream_name, bool is_start,
         std::experimental::optional<int64_t> limit_batches) {
        TypeParam ast_generator(input);
        auto *stream_query = dynamic_cast<StreamQuery *>(ast_generator.query_);
        ASSERT_TRUE(stream_query);

        EXPECT_EQ(stream_query->stream_name_, stream_name);
        EXPECT_EQ(stream_query->action_,
                  is_start ? StreamQuery::Action::START_STREAM
                           : StreamQuery::Action::STOP_STREAM);

        if (limit_batches) {
          ASSERT_TRUE(is_start);
          ASSERT_TRUE(stream_query->limit_batches_);
          ast_generator.CheckLiteral(stream_query->limit_batches_,
                                     TypedValue(*limit_batches));
        } else {
          EXPECT_EQ(stream_query->limit_batches_, nullptr);
        }
      };

  check_start_stop_stream("stARt STreaM STREAM", "STREAM", true,
                          std::experimental::nullopt);
  check_start_stop_stream("stARt STreaM strim", "strim", true,
                          std::experimental::nullopt);
  check_start_stop_stream("StARt STreAM strim LimIT 10 BATchES", "strim", true,
                          10);

  check_start_stop_stream("StoP StrEAM strim", "strim", false,
                          std::experimental::nullopt);

  EXPECT_THROW(check_start_stop_stream("staRT STReaM 'strim'", "strim", true,
                                       std::experimental::nullopt),
               SyntaxException);
  EXPECT_THROW(check_start_stop_stream("sTART STReaM strim LImiT 'dva' BATCheS",
                                       "strim", true, 2),
               SyntaxException);
  EXPECT_THROW(check_start_stop_stream("StoP STreAM 'strim'", "strim", false,
                                       std::experimental::nullopt),
               SyntaxException);
  EXPECT_THROW(check_start_stop_stream("STOp sTREAM strim LIMit 2 baTCHES",
                                       "strim", false, 2),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, StartStopAllStreams) {
  auto check_start_stop_all_streams = [](std::string input, bool is_start) {
    TypeParam ast_generator(input);
    auto *stream_query = dynamic_cast<StreamQuery *>(ast_generator.query_);
    ASSERT_TRUE(stream_query);
    EXPECT_EQ(stream_query->action_,
              is_start ? StreamQuery::Action::START_ALL_STREAMS
                       : StreamQuery::Action::STOP_ALL_STREAMS);
  };

  check_start_stop_all_streams("STarT AlL StreAMs", true);

  check_start_stop_all_streams("StoP aLL STrEAMs", false);

  EXPECT_THROW(check_start_stop_all_streams("StaRT aLL STreAM", true),
               SyntaxException);

  EXPECT_THROW(check_start_stop_all_streams("SToP AlL STREaM", false),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, TestStream) {
  auto check_test_stream =
      [](std::string input, const std::string &stream_name,
         std::experimental::optional<int64_t> limit_batches) {
        TypeParam ast_generator(input);
        auto *stream_query = dynamic_cast<StreamQuery *>(ast_generator.query_);
        ASSERT_TRUE(stream_query);
        EXPECT_EQ(stream_query->stream_name_, stream_name);
        EXPECT_EQ(stream_query->action_, StreamQuery::Action::TEST_STREAM);

        if (limit_batches) {
          ASSERT_TRUE(stream_query->limit_batches_);
          ast_generator.CheckLiteral(stream_query->limit_batches_,
                                     TypedValue(*limit_batches));
        } else {
          EXPECT_EQ(stream_query->limit_batches_, nullptr);
        }
      };

  check_test_stream("TesT STreaM strim", "strim", std::experimental::nullopt);
  check_test_stream("TesT STreaM STREAM", "STREAM", std::experimental::nullopt);
  check_test_stream("tESt STreAM STREAM LimIT 10 BATchES", "STREAM", 10);

  check_test_stream("Test StrEAM STREAM", "STREAM", std::experimental::nullopt);

  EXPECT_THROW(check_test_stream("tEST STReaM 'strim'", "strim",
                                 std::experimental::nullopt),
               SyntaxException);
  EXPECT_THROW(
      check_test_stream("test STReaM strim LImiT 'dva' BATCheS", "strim", 2),
      SyntaxException);
  EXPECT_THROW(check_test_stream("test STreAM 'strim'", "strim",
                                 std::experimental::nullopt),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, TestExplainRegularQuery) {
  {
    TypeParam ast_generator("EXPLAIN RETURN n");
    EXPECT_TRUE(dynamic_cast<ExplainQuery *>(ast_generator.query_));
  }
}

TYPED_TEST(CypherMainVisitorTest, TestExplainExplainQuery) {
  EXPECT_THROW(TypeParam ast_generator("EXPLAIN EXPLAIN RETURN n"),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, TestExplainAuthQuery) {
  EXPECT_THROW(TypeParam ast_generator("EXPLAIN SHOW ROLES"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, TestExplainStreamQuery) {
  EXPECT_THROW(TypeParam ast_generator("EXPLAIN SHOW STREAMS"),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, TestProfileRegularQuery) {
  {
    TypeParam ast_generator("PROFILE RETURN n");
    EXPECT_TRUE(dynamic_cast<ProfileQuery *>(ast_generator.query_));
  }
}

TYPED_TEST(CypherMainVisitorTest, TestProfileComplicatedQuery) {
  {
    TypeParam ast_generator(
        "profile optional match (n) where n.hello = 5 "
        "return n union optional match (n) where n.there = 10 "
        "return n");
    EXPECT_TRUE(dynamic_cast<ProfileQuery *>(ast_generator.query_));
  }
}

TYPED_TEST(CypherMainVisitorTest, TestProfileProfileQuery) {
  EXPECT_THROW(TypeParam ast_generator("PROFILE PROFILE RETURN n"),
               SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, TestProfileAuthQuery) {
  EXPECT_THROW(TypeParam ast_generator("PROFILE SHOW ROLES"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, TestProfileStreamQuery) {
  EXPECT_THROW(TypeParam ast_generator("PROFILE SHOW STREAMS"),
               SyntaxException);
}

}  // namespace
