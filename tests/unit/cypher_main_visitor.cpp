#include <algorithm>
#include <climits>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include "antlr4-runtime.h"
#include "database/dbms.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/stripped.hpp"
#include "query/typed_value.hpp"

namespace {

using namespace query;
using namespace query::frontend;
using query::TypedValue;
using testing::Pair;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

// Base class for all test types
class Base {
 public:
  Base(const std::string &query) : query_string_(query) {}
  Dbms dbms_;
  std::unique_ptr<GraphDbAccessor> db_accessor_ = dbms_.active();
  Context context_{*db_accessor_};
  std::string query_string_;

  auto Prop(const std::string &prop_name) {
    return db_accessor_->Property(prop_name);
  }

  auto PropPair(const std::string &prop_name) {
    return std::make_pair(prop_name, Prop(prop_name));
  }
};

// This generator uses ast constructed by parsing the query.
class AstGenerator : public Base {
 public:
  AstGenerator(const std::string &query)
      : Base(query), parser_(query), visitor_(context_), query_([&]() {
          visitor_.visit(parser_.tree());
          return visitor_.query();
        }()) {}

  ::frontend::opencypher::Parser parser_;
  CypherMainVisitor visitor_;
  Query *query_;
};

// This clones ast, but uses original one. This done just to ensure that cloning
// doesn't change original.
class OriginalAfterCloningAstGenerator : public AstGenerator {
 public:
  OriginalAfterCloningAstGenerator(const std::string &query)
      : AstGenerator(query) {
    AstTreeStorage storage;
    visitor_.query()->Clone(storage);
  }
};

// This generator clones parsed ast and uses that one.
// Original ast is cleared after cloning to ensure that cloned ast doesn't reuse
// any data from original ast.
class ClonedAstGenerator : public Base {
 public:
  ClonedAstGenerator(const std::string &query)
      : Base(query), query_([&]() {
          ::frontend::opencypher::Parser parser(query);
          CypherMainVisitor visitor(context_);
          visitor.visit(parser.tree());
          return visitor.query()->Clone(storage);
        }()) {}

  AstTreeStorage storage;
  Query *query_;
};

// This generator strips ast, clones it and then plugs stripped out literals in
// the same way it is done in ast cacheing in interpreter.
class CachedAstGenerator : public Base {
 public:
  CachedAstGenerator(const std::string &query)
      : Base(query),
        storage_([&]() {
          context_.is_query_cached_ = true;
          StrippedQuery stripped(query_string_);
          context_.parameters_ = stripped.literals();
          ::frontend::opencypher::Parser parser(stripped.query());
          CypherMainVisitor visitor(context_);
          visitor.visit(parser.tree());
          AstTreeStorage new_ast;
          visitor.storage().query()->Clone(new_ast);
          return new_ast;
        }()),
        query_(storage_.query()) {}

  AstTreeStorage storage_;
  Query *query_;
};

template <typename T>
class CypherMainVisitorTest : public ::testing::Test {};

typedef ::testing::Types<AstGenerator, OriginalAfterCloningAstGenerator,
                         ClonedAstGenerator, CachedAstGenerator>
    AstGeneratorTypes;
TYPED_TEST_CASE(CypherMainVisitorTest, AstGeneratorTypes);

TYPED_TEST(CypherMainVisitorTest, SyntaxException) {
  ASSERT_THROW(TypeParam("CREATE ()-[*1....2]-()"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, SyntaxExceptionOnTrailingText) {
  ASSERT_THROW(TypeParam("RETURN 2 + 2 mirko"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, PropertyLookup) {
  TypeParam ast_generator("RETURN n.x");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *property_lookup = dynamic_cast<PropertyLookup *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(property_lookup->expression_);
  auto identifier = dynamic_cast<Identifier *>(property_lookup->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_EQ(property_lookup->property_,
            ast_generator.db_accessor_->Property("x"));
}

TYPED_TEST(CypherMainVisitorTest, LabelsTest) {
  TypeParam ast_generator("RETURN n:x:y");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(labels_test->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_,
              ElementsAre(ast_generator.db_accessor_->Label("x"),
                          ast_generator.db_accessor_->Label("y")));
}

TYPED_TEST(CypherMainVisitorTest, EscapedLabel) {
  TypeParam ast_generator("RETURN n:`l-$\"'ab``e````l`");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_,
              ElementsAre(ast_generator.db_accessor_->Label("l-$\"'ab`e``l")));
}

TYPED_TEST(CypherMainVisitorTest, KeywordLabel) {
  ASSERT_THROW(TypeParam("RETURN n:DEletE"), SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, HexLetterLabel) {
  TypeParam ast_generator("RETURN n:a");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  EXPECT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_,
              ElementsAre(ast_generator.db_accessor_->Label("a")));
}

TYPED_TEST(CypherMainVisitorTest, ReturnNoDistinctNoBagSemantics) {
  TypeParam ast_generator("RETURN x");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.order_by.size(), 0U);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
  ASSERT_FALSE(return_clause->body_.limit);
  ASSERT_FALSE(return_clause->body_.skip);
  ASSERT_FALSE(return_clause->body_.distinct);
}

TYPED_TEST(CypherMainVisitorTest, ReturnDistinct) {
  TypeParam ast_generator("RETURN DISTINCT x");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.distinct);
}

TypedValue LiteralValue(const Context &context, Expression *expression) {
  if (context.is_query_cached_) {
    auto *param_lookup = dynamic_cast<ParameterLookup *>(expression);
    return context.parameters_.AtTokenPosition(param_lookup->token_position_);
  } else {
    auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
    return literal->value_;
  }
}

void CheckLiteral(const Context &context, Expression *expression,
                  const TypedValue &expected,
                  const std::experimental::optional<int> &token_position =
                      std::experimental::nullopt) {
  TypedValue value;
  if (!expected.IsNull() && context.is_query_cached_) {
    auto *param_lookup = dynamic_cast<ParameterLookup *>(expression);
    ASSERT_TRUE(param_lookup);
    if (token_position)
      EXPECT_EQ(param_lookup->token_position_, *token_position);
    value = context.parameters_.AtTokenPosition(param_lookup->token_position_);
  } else {
    auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
    ASSERT_TRUE(literal);
    if (token_position) ASSERT_EQ(literal->token_position_, *token_position);
    value = literal->value_;
  }
  EXPECT_TRUE(TypedValue::BoolEqual{}(value, expected));
}

TYPED_TEST(CypherMainVisitorTest, ReturnLimit) {
  TypeParam ast_generator("RETURN x LIMIT 5");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.limit);
  CheckLiteral(ast_generator.context_, return_clause->body_.limit, 5);
}

TYPED_TEST(CypherMainVisitorTest, ReturnSkip) {
  TypeParam ast_generator("RETURN x SKIP 5");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.skip);
  CheckLiteral(ast_generator.context_, return_clause->body_.skip, 5);
}

TYPED_TEST(CypherMainVisitorTest, ReturnOrderBy) {
  TypeParam ast_generator("RETURN x, y, z ORDER BY z ASC, x, y DESC");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.order_by.size(), 3U);
  std::vector<std::pair<Ordering, std::string>> ordering;
  for (const auto &sort_item : return_clause->body_.order_by) {
    auto *identifier = dynamic_cast<Identifier *>(sort_item.second);
    ordering.emplace_back(sort_item.first, identifier->name_);
  }
  ASSERT_THAT(ordering, UnorderedElementsAre(Pair(Ordering::ASC, "z"),
                                             Pair(Ordering::ASC, "x"),
                                             Pair(Ordering::DESC, "y")));
}

TYPED_TEST(CypherMainVisitorTest, ReturnNamedIdentifier) {
  TypeParam ast_generator("RETURN var AS var5");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_FALSE(return_clause->body_.all_identifiers);
  auto *named_expr = return_clause->body_.named_expressions[0];
  ASSERT_EQ(named_expr->name_, "var5");
  auto *identifier = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_EQ(identifier->name_, "var");
}

TYPED_TEST(CypherMainVisitorTest, ReturnAsterisk) {
  TypeParam ast_generator("RETURN *");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 0U);
}

TYPED_TEST(CypherMainVisitorTest, IntegerLiteral) {
  TypeParam ast_generator("RETURN 42");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_, 42, 2);
}

TYPED_TEST(CypherMainVisitorTest, IntegerLiteralTooLarge) {
  ASSERT_THROW(TypeParam("RETURN 10000000000000000000000000"),
               SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, BooleanLiteralTrue) {
  TypeParam ast_generator("RETURN TrUe");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_, true, 2);
}

TYPED_TEST(CypherMainVisitorTest, BooleanLiteralFalse) {
  TypeParam ast_generator("RETURN faLSE");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_, false,
               2);
}

TYPED_TEST(CypherMainVisitorTest, NullLiteral) {
  TypeParam ast_generator("RETURN nULl");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_,
               TypedValue::Null, 2);
}

TYPED_TEST(CypherMainVisitorTest, ParenthesizedExpression) {
  TypeParam ast_generator("RETURN (2)");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, OrOperator) {
  TypeParam ast_generator("RETURN true Or false oR n");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *or_operator2 = dynamic_cast<OrOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(or_operator2);
  auto *or_operator1 = dynamic_cast<OrOperator *>(or_operator2->expression1_);
  ASSERT_TRUE(or_operator1);
  CheckLiteral(ast_generator.context_, or_operator1->expression1_, true);
  CheckLiteral(ast_generator.context_, or_operator1->expression2_, false);
  auto *operand3 = dynamic_cast<Identifier *>(or_operator2->expression2_);
  ASSERT_TRUE(operand3);
  ASSERT_EQ(operand3->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, XorOperator) {
  TypeParam ast_generator("RETURN true xOr false");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *xor_operator = dynamic_cast<XorOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  CheckLiteral(ast_generator.context_, xor_operator->expression1_, true);
  CheckLiteral(ast_generator.context_, xor_operator->expression2_, false);
}

TYPED_TEST(CypherMainVisitorTest, AndOperator) {
  TypeParam ast_generator("RETURN true and false");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *and_operator = dynamic_cast<AndOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  CheckLiteral(ast_generator.context_, and_operator->expression1_, true);
  CheckLiteral(ast_generator.context_, and_operator->expression2_, false);
}

TYPED_TEST(CypherMainVisitorTest, AdditionSubtractionOperators) {
  TypeParam ast_generator("RETURN 1 - 2 + 3");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *addition_operator = dynamic_cast<AdditionOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(addition_operator);
  auto *subtraction_operator =
      dynamic_cast<SubtractionOperator *>(addition_operator->expression1_);
  ASSERT_TRUE(subtraction_operator);
  CheckLiteral(ast_generator.context_, subtraction_operator->expression1_, 1);
  CheckLiteral(ast_generator.context_, subtraction_operator->expression2_, 2);
  CheckLiteral(ast_generator.context_, addition_operator->expression2_, 3);
}

TYPED_TEST(CypherMainVisitorTest, MulitplicationOperator) {
  TypeParam ast_generator("RETURN 2 * 3");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *mult_operator = dynamic_cast<MultiplicationOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  CheckLiteral(ast_generator.context_, mult_operator->expression1_, 2);
  CheckLiteral(ast_generator.context_, mult_operator->expression2_, 3);
}

TYPED_TEST(CypherMainVisitorTest, DivisionOperator) {
  TypeParam ast_generator("RETURN 2 / 3");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *div_operator = dynamic_cast<DivisionOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  CheckLiteral(ast_generator.context_, div_operator->expression1_, 2);
  CheckLiteral(ast_generator.context_, div_operator->expression2_, 3);
}

TYPED_TEST(CypherMainVisitorTest, ModOperator) {
  TypeParam ast_generator("RETURN 2 % 3");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *mod_operator = dynamic_cast<ModOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  CheckLiteral(ast_generator.context_, mod_operator->expression1_, 2);
  CheckLiteral(ast_generator.context_, mod_operator->expression2_, 3);
}

#define CHECK_COMPARISON(TYPE, VALUE1, VALUE2)                                \
  do {                                                                        \
    auto *and_operator = dynamic_cast<AndOperator *>(_operator);              \
    ASSERT_TRUE(and_operator);                                                \
    _operator = and_operator->expression1_;                                   \
    auto *cmp_operator = dynamic_cast<TYPE *>(and_operator->expression2_);    \
    ASSERT_TRUE(cmp_operator);                                                \
    CheckLiteral(ast_generator.context_, cmp_operator->expression1_, VALUE1); \
    CheckLiteral(ast_generator.context_, cmp_operator->expression2_, VALUE2); \
  } while (0)

TYPED_TEST(CypherMainVisitorTest, ComparisonOperators) {
  TypeParam ast_generator("RETURN 2 = 3 != 4 <> 5 < 6 > 7 <= 8 >= 9");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
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
  CheckLiteral(ast_generator.context_, cmp_operator->expression1_, 2);
  CheckLiteral(ast_generator.context_, cmp_operator->expression2_, 3);
}

#undef CHECK_COMPARISON

TYPED_TEST(CypherMainVisitorTest, ListIndexing) {
  TypeParam ast_generator("RETURN [1,2,3] [ 2 ]");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *list_index_op = dynamic_cast<ListMapIndexingOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_index_op);
  auto *list = dynamic_cast<ListLiteral *>(list_index_op->expression1_);
  EXPECT_TRUE(list);
  CheckLiteral(ast_generator.context_, list_index_op->expression2_, 2);
}

TYPED_TEST(CypherMainVisitorTest, ListSlicingOperatorNoBounds) {
  ASSERT_THROW(TypeParam("RETURN [1,2,3] [ .. ]"), SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, ListSlicingOperator) {
  TypeParam ast_generator("RETURN [1,2,3] [ .. 2 ]");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *list_slicing_op = dynamic_cast<ListSlicingOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_slicing_op);
  auto *list = dynamic_cast<ListLiteral *>(list_slicing_op->list_);
  EXPECT_TRUE(list);
  EXPECT_FALSE(list_slicing_op->lower_bound_);
  CheckLiteral(ast_generator.context_, list_slicing_op->upper_bound_, 2);
}

TYPED_TEST(CypherMainVisitorTest, InListOperator) {
  TypeParam ast_generator("RETURN 5 IN [1,2]");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *in_list_operator = dynamic_cast<InListOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(in_list_operator);
  CheckLiteral(ast_generator.context_, in_list_operator->expression1_, 5);
  auto *list = dynamic_cast<ListLiteral *>(in_list_operator->expression2_);
  ASSERT_TRUE(list);
}

TYPED_TEST(CypherMainVisitorTest, InWithListIndexing) {
  TypeParam ast_generator("RETURN 1 IN [[1,2]][0]");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *in_list_operator = dynamic_cast<InListOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(in_list_operator);
  CheckLiteral(ast_generator.context_, in_list_operator->expression1_, 1);
  auto *list_indexing =
      dynamic_cast<ListMapIndexingOperator *>(in_list_operator->expression2_);
  ASSERT_TRUE(list_indexing);
  auto *list = dynamic_cast<ListLiteral *>(list_indexing->expression1_);
  EXPECT_TRUE(list);
  CheckLiteral(ast_generator.context_, list_indexing->expression2_, 0);
}

TYPED_TEST(CypherMainVisitorTest, CaseGenericForm) {
  TypeParam ast_generator(
      "RETURN CASE WHEN n < 10 THEN 1 WHEN n > 10 THEN 2 END");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(if_operator);
  auto *condition = dynamic_cast<LessOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  CheckLiteral(ast_generator.context_, if_operator->then_expression_, 1);

  auto *if_operator2 =
      dynamic_cast<IfOperator *>(if_operator->else_expression_);
  ASSERT_TRUE(if_operator2);
  auto *condition2 = dynamic_cast<GreaterOperator *>(if_operator2->condition_);
  ASSERT_TRUE(condition2);
  CheckLiteral(ast_generator.context_, if_operator2->then_expression_, 2);
  CheckLiteral(ast_generator.context_, if_operator2->else_expression_,
               TypedValue::Null);
}

TYPED_TEST(CypherMainVisitorTest, CaseGenericFormElse) {
  TypeParam ast_generator("RETURN CASE WHEN n < 10 THEN 1 ELSE 2 END");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto *condition = dynamic_cast<LessOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  CheckLiteral(ast_generator.context_, if_operator->then_expression_, 1);
  CheckLiteral(ast_generator.context_, if_operator->else_expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, CaseSimpleForm) {
  TypeParam ast_generator("RETURN CASE 5 WHEN 10 THEN 1 END");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto *condition = dynamic_cast<EqualOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  CheckLiteral(ast_generator.context_, condition->expression1_, 5);
  CheckLiteral(ast_generator.context_, condition->expression2_, 10);
  CheckLiteral(ast_generator.context_, if_operator->then_expression_, 1);
  CheckLiteral(ast_generator.context_, if_operator->else_expression_,
               TypedValue::Null);
}

TYPED_TEST(CypherMainVisitorTest, IsNull) {
  TypeParam ast_generator("RETURN 2 iS NulL");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *is_type_operator = dynamic_cast<IsNullOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  CheckLiteral(ast_generator.context_, is_type_operator->expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, IsNotNull) {
  TypeParam ast_generator("RETURN 2 iS nOT NulL");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *not_operator = dynamic_cast<NotOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  auto *is_type_operator =
      dynamic_cast<IsNullOperator *>(not_operator->expression_);
  CheckLiteral(ast_generator.context_, is_type_operator->expression_, 2);
}

TYPED_TEST(CypherMainVisitorTest, NotOperator) {
  TypeParam ast_generator("RETURN not true");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *not_operator = dynamic_cast<NotOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  CheckLiteral(ast_generator.context_, not_operator->expression_, true);
}

TYPED_TEST(CypherMainVisitorTest, UnaryMinusPlusOperators) {
  TypeParam ast_generator("RETURN -+5");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *unary_minus_operator = dynamic_cast<UnaryMinusOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(unary_minus_operator);
  auto *unary_plus_operator =
      dynamic_cast<UnaryPlusOperator *>(unary_minus_operator->expression_);
  ASSERT_TRUE(unary_plus_operator);
  CheckLiteral(ast_generator.context_, unary_plus_operator->expression_, 5);
}

TYPED_TEST(CypherMainVisitorTest, Aggregation) {
  TypeParam ast_generator(
      "RETURN COUNT(a), MIN(b), MAX(c), SUM(d), AVG(e), COLLECT(f), COUNT(*)");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1);
  auto *function = dynamic_cast<Function *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(function);
  ASSERT_TRUE(function->function_);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralDoubleQuotes) {
  TypeParam ast_generator("RETURN \"mi'rko\"");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_, "mi'rko",
               2);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralSingleQuotes) {
  TypeParam ast_generator("RETURN 'mi\"rko'");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_,
               "mi\"rko", 2);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedChars) {
  TypeParam ast_generator("RETURN '\\\\\\'\\\"\\b\\B\\f\\F\\n\\N\\r\\R\\t\\T'");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_,
               "\\'\"\b\b\f\f\n\n\r\r\t\t", 2);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedUtf16) {
  TypeParam ast_generator("RETURN '\\u221daaa\\u221daaa'");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_,
               u8"\u221daaa\u221daaa", 2);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedUtf16Error) {
  ASSERT_THROW(TypeParam("RETURN '\\U221daaa'"), SyntaxException);
}

TYPED_TEST(CypherMainVisitorTest, StringLiteralEscapedUtf32) {
  TypeParam ast_generator("RETURN '\\U0001F600aaaa\\U0001F600aaaaaaaa'");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_,
               u8"\U0001F600aaaa\U0001F600aaaaaaaa", 2);
}

TYPED_TEST(CypherMainVisitorTest, DoubleLiteral) {
  TypeParam ast_generator("RETURN 3.5");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_, 3.5, 2);
}

TYPED_TEST(CypherMainVisitorTest, DoubleLiteralExponent) {
  TypeParam ast_generator("RETURN 5e-1");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  CheckLiteral(ast_generator.context_,
               return_clause->body_.named_expressions[0]->expression_, 0.5, 2);
}

TYPED_TEST(CypherMainVisitorTest, ListLiteral) {
  TypeParam ast_generator("RETURN [3, [], 'johhny']");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *list_literal = dynamic_cast<ListLiteral *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_literal);
  ASSERT_EQ(3, list_literal->elements_.size());
  CheckLiteral(ast_generator.context_, list_literal->elements_[0], 3);
  auto *elem_1 = dynamic_cast<ListLiteral *>(list_literal->elements_[1]);
  ASSERT_TRUE(elem_1);
  EXPECT_EQ(0, elem_1->elements_.size());
  CheckLiteral(ast_generator.context_, list_literal->elements_[2], "johhny");
}

TYPED_TEST(CypherMainVisitorTest, MapLiteral) {
  TypeParam ast_generator("RETURN {a: 1, b: 'bla', c: [1, {a: 42}]}");
  auto *query = ast_generator.query_;
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
  auto *map_literal = dynamic_cast<MapLiteral *>(
      return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(map_literal);
  ASSERT_EQ(3, map_literal->elements_.size());
  CheckLiteral(ast_generator.context_,
               map_literal->elements_[ast_generator.PropPair("a")], 1);
  CheckLiteral(ast_generator.context_,
               map_literal->elements_[ast_generator.PropPair("b")], "bla");
  auto *elem_2 = dynamic_cast<ListLiteral *>(
      map_literal->elements_[ast_generator.PropPair("c")]);
  ASSERT_TRUE(elem_2);
  EXPECT_EQ(2, elem_2->elements_.size());
  auto *elem_2_1 = dynamic_cast<MapLiteral *>(elem_2->elements_[1]);
  ASSERT_TRUE(elem_2_1);
  EXPECT_EQ(1, elem_2_1->elements_.size());
}

TYPED_TEST(CypherMainVisitorTest, NodePattern) {
  TypeParam ast_generator(
      "MATCH (:label1:label2:label3 {a : 5, b : 10}) RETURN 1");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
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
  EXPECT_THAT(node->labels_, UnorderedElementsAre(
                                 ast_generator.db_accessor_->Label("label1"),
                                 ast_generator.db_accessor_->Label("label2"),
                                 ast_generator.db_accessor_->Label("label3")));
  std::map<std::pair<std::string, GraphDbTypes::Property>, int64_t> properties;
  for (auto x : node->properties_) {
    TypedValue value = LiteralValue(ast_generator.context_, x.second);
    ASSERT_TRUE(value.type() == TypedValue::Type::Int);
    properties[x.first] = value.Value<int64_t>();
  }
  EXPECT_THAT(properties,
              UnorderedElementsAre(Pair(ast_generator.PropPair("a"), 5),
                                   Pair(ast_generator.PropPair("b"), 10)));
}

TYPED_TEST(CypherMainVisitorTest, PropertyMapSameKeyAppearsTwice) {
  EXPECT_THROW(TypeParam("MATCH ({a : 1, a : 2})"), SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, NodePatternIdentifier) {
  TypeParam ast_generator("MATCH (var) RETURN 1");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_FALSE(match->optional_);
  EXPECT_FALSE(match->where_);
  auto *edge = dynamic_cast<EdgeAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::IN);
  EXPECT_THAT(
      edge->edge_types_,
      UnorderedElementsAre(ast_generator.db_accessor_->EdgeType("type1"),
                           ast_generator.db_accessor_->EdgeType("type2")));
  std::map<std::pair<std::string, GraphDbTypes::Property>, int64_t> properties;
  for (auto x : edge->properties_) {
    TypedValue value = LiteralValue(ast_generator.context_, x.second);
    ASSERT_TRUE(value.type() == TypedValue::Type::Int);
    properties[x.first] = value.Value<int64_t>();
  }
  EXPECT_THAT(properties,
              UnorderedElementsAre(Pair(ast_generator.PropPair("a"), 5),
                                   Pair(ast_generator.PropPair("b"), 10)));
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternVariable) {
  TypeParam ast_generator("MATCH ()-[var]->() RETURN 1");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  EXPECT_EQ(edge->upper_bound_, nullptr);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternLowerBounded) {
  TypeParam ast_generator("MATCH ()-[r*42..]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  CheckLiteral(ast_generator.context_, edge->lower_bound_, 42);
  EXPECT_EQ(edge->upper_bound_, nullptr);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternUpperBounded) {
  TypeParam ast_generator("MATCH ()-[r*..42]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  CheckLiteral(ast_generator.context_, edge->upper_bound_, 42);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternLowerUpperBounded) {
  TypeParam ast_generator("MATCH ()-[r*24..42]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  CheckLiteral(ast_generator.context_, edge->lower_bound_, 24);
  CheckLiteral(ast_generator.context_, edge->upper_bound_, 42);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternFixedRange) {
  TypeParam ast_generator("MATCH ()-[r*42]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  CheckLiteral(ast_generator.context_, edge->lower_bound_, 42);
  CheckLiteral(ast_generator.context_, edge->upper_bound_, 42);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternFloatingUpperBound) {
  // [r*1...2] should be parsed as [r*1..0.2]
  TypeParam ast_generator("MATCH ()-[r*1...2]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  CheckLiteral(ast_generator.context_, edge->lower_bound_, 1);
  CheckLiteral(ast_generator.context_, edge->upper_bound_, 0.2);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternUnboundedWithProperty) {
  TypeParam ast_generator("MATCH ()-[r* {prop: 42}]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  EXPECT_EQ(edge->upper_bound_, nullptr);
  CheckLiteral(ast_generator.context_,
               edge->properties_[ast_generator.PropPair("prop")], 42);
}

TYPED_TEST(CypherMainVisitorTest,
           RelationshipPatternDotsUnboundedWithEdgeTypeProperty) {
  TypeParam ast_generator("MATCH ()-[r:edge_type*..{prop: 42}]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  EXPECT_EQ(edge->upper_bound_, nullptr);
  CheckLiteral(ast_generator.context_,
               edge->properties_[ast_generator.PropPair("prop")], 42);
  ASSERT_EQ(edge->edge_types_.size(), 1U);
  auto edge_type = ast_generator.db_accessor_->EdgeType("edge_type");
  EXPECT_EQ(edge->edge_types_[0], edge_type);
}

TYPED_TEST(CypherMainVisitorTest, RelationshipPatternUpperBoundedWithProperty) {
  TypeParam ast_generator("MATCH ()-[r*..2{prop: 42}]->() RETURN r");
  auto *query = ast_generator.query_;
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  EdgeAtom *edge = nullptr;
  AssertMatchSingleEdgeAtom(match, edge);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::OUT);
  EXPECT_TRUE(edge->has_range_);
  EXPECT_EQ(edge->lower_bound_, nullptr);
  CheckLiteral(ast_generator.context_, edge->upper_bound_, 2);
  CheckLiteral(ast_generator.context_,
               edge->properties_[ast_generator.PropPair("prop")], 42);
}

// // PatternPart with variable.
// TYPED_TEST(CypherMainVisitorTest, PatternPartVariable) {
//   ParserTables parser("CREATE var=()--()");
//   ASSERT_EQ(parser.identifiers_map_.size(), 1U);
//   ASSERT_EQ(parser.pattern_parts_.size(), 1U);
//   ASSERT_EQ(parser.relationships_.size(), 1U);
//   ASSERT_EQ(parser.nodes_.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.nodes.size(), 2U);
//   ASSERT_EQ(parser.pattern_parts_.begin()->second.relationships.size(), 1U);
//   ASSERT_NE(parser.identifiers_map_.find("var"),
//   parser.identifiers_map_.end());
//   auto output_identifier = parser.identifiers_map_["var"];
//   ASSERT_NE(parser.pattern_parts_.find(output_identifier),
//             parser.pattern_parts_.end());
// }

TYPED_TEST(CypherMainVisitorTest, ReturnUnanemdIdentifier) {
  TypeParam ast_generator("RETURN var");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *create = dynamic_cast<Create *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *del = dynamic_cast<Delete *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *del = dynamic_cast<Delete *>(query->clauses_[0]);
  ASSERT_TRUE(del);
  ASSERT_TRUE(del->detach_);
  ASSERT_EQ(del->expressions_.size(), 1U);
  auto *identifier1 = dynamic_cast<Identifier *>(del->expressions_[0]);
  ASSERT_TRUE(identifier1);
  ASSERT_EQ(identifier1->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, OptionalMatchWhere) {
  TypeParam ast_generator("OPTIONAL MATCH (n) WHERE m RETURN 1");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  ASSERT_TRUE(match);
  EXPECT_TRUE(match->optional_);
  ASSERT_TRUE(match->where_);
  auto *identifier = dynamic_cast<Identifier *>(match->where_->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "m");
}

TYPED_TEST(CypherMainVisitorTest, Set) {
  TypeParam ast_generator("SET a.x = b, c = d, e += f, g : h : i ");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 4U);

  {
    auto *set_property = dynamic_cast<SetProperty *>(query->clauses_[0]);
    ASSERT_TRUE(set_property);
    ASSERT_TRUE(set_property->property_lookup_);
    auto *identifier1 =
        dynamic_cast<Identifier *>(set_property->property_lookup_->expression_);
    ASSERT_TRUE(identifier1);
    ASSERT_EQ(identifier1->name_, "a");
    ASSERT_EQ(set_property->property_lookup_->property_,
              ast_generator.db_accessor_->Property("x"));
    auto *identifier2 = dynamic_cast<Identifier *>(set_property->expression_);
    ASSERT_EQ(identifier2->name_, "b");
  }

  {
    auto *set_properties_assignment =
        dynamic_cast<SetProperties *>(query->clauses_[1]);
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
        dynamic_cast<SetProperties *>(query->clauses_[2]);
    ASSERT_TRUE(set_properties_update);
    ASSERT_TRUE(set_properties_update->update_);
    ASSERT_TRUE(set_properties_update->identifier_);
    ASSERT_EQ(set_properties_update->identifier_->name_, "e");
    auto *identifier =
        dynamic_cast<Identifier *>(set_properties_update->expression_);
    ASSERT_EQ(identifier->name_, "f");
  }

  {
    auto *set_labels = dynamic_cast<SetLabels *>(query->clauses_[3]);
    ASSERT_TRUE(set_labels);
    ASSERT_TRUE(set_labels->identifier_);
    ASSERT_EQ(set_labels->identifier_->name_, "g");
    ASSERT_THAT(set_labels->labels_,
                UnorderedElementsAre(ast_generator.db_accessor_->Label("h"),
                                     ast_generator.db_accessor_->Label("i")));
  }
}

TYPED_TEST(CypherMainVisitorTest, Remove) {
  TypeParam ast_generator("REMOVE a.x, g : h : i");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);

  {
    auto *remove_property = dynamic_cast<RemoveProperty *>(query->clauses_[0]);
    ASSERT_TRUE(remove_property);
    ASSERT_TRUE(remove_property->property_lookup_);
    auto *identifier1 = dynamic_cast<Identifier *>(
        remove_property->property_lookup_->expression_);
    ASSERT_TRUE(identifier1);
    ASSERT_EQ(identifier1->name_, "a");
    ASSERT_EQ(remove_property->property_lookup_->property_,
              ast_generator.db_accessor_->Property("x"));
  }
  {
    auto *remove_labels = dynamic_cast<RemoveLabels *>(query->clauses_[1]);
    ASSERT_TRUE(remove_labels);
    ASSERT_TRUE(remove_labels->identifier_);
    ASSERT_EQ(remove_labels->identifier_->name_, "g");
    ASSERT_THAT(remove_labels->labels_,
                UnorderedElementsAre(ast_generator.db_accessor_->Label("h"),
                                     ast_generator.db_accessor_->Label("i")));
  }
}

TYPED_TEST(CypherMainVisitorTest, With) {
  TypeParam ast_generator("WITH n AS m RETURN 1");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(query->clauses_[0]);
  ASSERT_TRUE(with);
  ASSERT_EQ(with->body_.named_expressions.size(), 1U);
  auto *named_expr = with->body_.named_expressions[0];
  ASSERT_EQ(named_expr->name_, "n");
  auto *identifier = dynamic_cast<Identifier *>(named_expr->expression_);
  ASSERT_EQ(identifier->name_, "n");
}

TYPED_TEST(CypherMainVisitorTest, WithDistinct) {
  TypeParam ast_generator("WITH DISTINCT n AS m RETURN 1");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *with = dynamic_cast<With *>(query->clauses_[0]);
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

  TypeParam("CREATE INDEX ON :a(b)");
  ASSERT_THROW(TypeParam("CREATE INDEX ON :a(n) CREATE INDEX ON :b(c)"),
               SemanticException);
  ASSERT_THROW(TypeParam("CREATE (n) CREATE INDEX ON :a(n)"),
               SemanticException);
  ASSERT_THROW(TypeParam("CREATE INDEX ON :a(n) RETURN 2 + 2"),
               SemanticException);
}

TYPED_TEST(CypherMainVisitorTest, Merge) {
  TypeParam ast_generator(
      "MERGE (a) -[:r]- (b) ON MATCH SET a.x = b.x "
      "ON CREATE SET b :label ON MATCH SET b = a");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *merge = dynamic_cast<Merge *>(query->clauses_[0]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *unwind = dynamic_cast<Unwind *>(query->clauses_[0]);
  ASSERT_TRUE(unwind);
  auto *ret = dynamic_cast<Return *>(query->clauses_[1]);
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
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *create_index = dynamic_cast<CreateIndex *>(query->clauses_[0]);
  ASSERT_TRUE(create_index);
  ASSERT_EQ(create_index->label_, ast_generator.db_accessor_->Label("mirko"));
  ASSERT_EQ(create_index->property_,
            ast_generator.db_accessor_->Property("slavko"));
}

TYPED_TEST(CypherMainVisitorTest, ReturnAll) {
  TypeParam ast_generator("RETURN all(x IN [1,2,3] WHERE x = 2)");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 1U);
  auto *ret = dynamic_cast<Return *>(query->clauses_[0]);
  ASSERT_TRUE(ret);
  ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
  auto *all = dynamic_cast<All *>(ret->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(all);
  EXPECT_EQ(all->identifier_->name_, "x");
  auto *list_literal = dynamic_cast<ListLiteral *>(all->list_expression_);
  EXPECT_TRUE(list_literal);
  auto *eq = dynamic_cast<EqualOperator *>(all->where_->expression_);
  EXPECT_TRUE(eq);
}

TYPED_TEST(CypherMainVisitorTest, MatchBfsReturn) {
  TypeParam ast_generator(
      "MATCH (n) -bfs[r:type1|type2](e, n|e.prop = 42, 10)-> (m) RETURN r");
  auto *query = ast_generator.query_;
  ASSERT_EQ(query->clauses_.size(), 2U);
  auto *match = dynamic_cast<Match *>(query->clauses_[0]);
  ASSERT_TRUE(match);
  ASSERT_EQ(match->patterns_.size(), 1U);
  ASSERT_EQ(match->patterns_[0]->atoms_.size(), 3U);
  auto *bfs = dynamic_cast<BreadthFirstAtom *>(match->patterns_[0]->atoms_[1]);
  ASSERT_TRUE(bfs);
  EXPECT_EQ(bfs->direction_, EdgeAtom::Direction::OUT);
  EXPECT_THAT(
      bfs->edge_types_,
      UnorderedElementsAre(ast_generator.db_accessor_->EdgeType("type1"),
                           ast_generator.db_accessor_->EdgeType("type2")));
  EXPECT_EQ(bfs->identifier_->name_, "r");
  EXPECT_EQ(bfs->traversed_edge_identifier_->name_, "e");
  EXPECT_EQ(bfs->next_node_identifier_->name_, "n");
  CheckLiteral(ast_generator.context_, bfs->max_depth_, 10);
  auto *eq = dynamic_cast<EqualOperator *>(bfs->filter_expression_);
  ASSERT_TRUE(eq);
}
}
