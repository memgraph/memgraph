#include <algorithm>
#include <climits>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include <antlr4-runtime.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/stripped.hpp"
#include "query/typed_value.hpp"

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
  ParsingContext context_;
  Parameters parameters_;

  virtual ~Base() {}

  virtual Query *ParseQuery(const std::string &query_string) = 0;

  virtual PropertyIx Prop(const std::string &prop_name) = 0;

  virtual LabelIx Label(const std::string &label_name) = 0;

  virtual EdgeTypeIx EdgeType(const std::string &edge_type_name) = 0;

  TypedValue LiteralValue(Expression *expression) {
    if (context_.is_query_cached) {
      auto *param_lookup = dynamic_cast<ParameterLookup *>(expression);
      return TypedValue(
          parameters_.AtTokenPosition(param_lookup->token_position_));
    } else {
      auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
      return TypedValue(literal->value_);
    }
  }

  template <class TValue>
  void CheckLiteral(Expression *expression, const TValue &expected,
                    const std::optional<int> &token_position = std::nullopt) {
    TypedValue value;
    // NOLINTNEXTLINE(performance-unnecessary-copy-initialization)
    TypedValue expected_tv(expected);
    if (!expected_tv.IsNull() && context_.is_query_cached) {
      auto *param_lookup = dynamic_cast<ParameterLookup *>(expression);
      ASSERT_TRUE(param_lookup);
      if (token_position)
        EXPECT_EQ(param_lookup->token_position_, *token_position);
      value = TypedValue(
          parameters_.AtTokenPosition(param_lookup->token_position_));
    } else {
      auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
      ASSERT_TRUE(literal);
      if (token_position) ASSERT_EQ(literal->token_position_, *token_position);
      value = TypedValue(literal->value_);
    }
    EXPECT_TRUE(TypedValue::BoolEqual{}(value, expected_tv));
  }
};

// This generator uses ast constructed by parsing the query.
class AstGenerator : public Base {
 public:
  Query *ParseQuery(const std::string &query_string) override {
    ::frontend::opencypher::Parser parser(query_string);
    CypherMainVisitor visitor(context_, &ast_storage_);
    visitor.visit(parser.tree());
    return visitor.query();
  }

  PropertyIx Prop(const std::string &prop_name) override {
    return ast_storage_.GetPropertyIx(prop_name);
  }

  LabelIx Label(const std::string &name) override {
    return ast_storage_.GetLabelIx(name);
  }

  EdgeTypeIx EdgeType(const std::string &name) override {
    return ast_storage_.GetEdgeTypeIx(name);
  }

  AstStorage ast_storage_;
};

// This clones ast, but uses original one. This done just to ensure that cloning
// doesn't change original.
class OriginalAfterCloningAstGenerator : public AstGenerator {
 public:
  Query *ParseQuery(const std::string &query_string) override {
    auto *original_query = AstGenerator::ParseQuery(query_string);
    AstStorage storage;
    original_query->Clone(&storage);
    return original_query;
  }
};

// This generator clones parsed ast and uses that one.
// Original ast is cleared after cloning to ensure that cloned ast doesn't reuse
// any data from original ast.
class ClonedAstGenerator : public Base {
 public:
  Query *ParseQuery(const std::string &query_string) override {
    ::frontend::opencypher::Parser parser(query_string);
    AstStorage tmp_storage;
    {
      // Add a label, property and edge type into temporary storage so
      // indices have to change in cloned AST.
      tmp_storage.GetLabelIx("jkfdklajfkdalsj");
      tmp_storage.GetPropertyIx("fdjakfjdklfjdaslk");
      tmp_storage.GetEdgeTypeIx("fdjkalfjdlkajfdkla");
    }
    CypherMainVisitor visitor(context_, &tmp_storage);
    visitor.visit(parser.tree());
    return visitor.query()->Clone(&ast_storage_);
  }

  PropertyIx Prop(const std::string &prop_name) override {
    return ast_storage_.GetPropertyIx(prop_name);
  }

  LabelIx Label(const std::string &name) override {
    return ast_storage_.GetLabelIx(name);
  }

  EdgeTypeIx EdgeType(const std::string &name) override {
    return ast_storage_.GetEdgeTypeIx(name);
  }

  AstStorage ast_storage_;
};

// This generator strips ast, clones it and then plugs stripped out literals in
// the same way it is done in ast cacheing in interpreter.
class CachedAstGenerator : public Base {
 public:
  Query *ParseQuery(const std::string &query_string) override {
    context_.is_query_cached = true;
    StrippedQuery stripped(query_string);
    parameters_ = stripped.literals();
    ::frontend::opencypher::Parser parser(stripped.query());
    AstStorage tmp_storage;
    CypherMainVisitor visitor(context_, &tmp_storage);
    visitor.visit(parser.tree());
    return visitor.query()->Clone(&ast_storage_);
  }

  PropertyIx Prop(const std::string &prop_name) override {
    return ast_storage_.GetPropertyIx(prop_name);
  }

  LabelIx Label(const std::string &name) override {
    return ast_storage_.GetLabelIx(name);
  }

  EdgeTypeIx EdgeType(const std::string &name) override {
    return ast_storage_.GetEdgeTypeIx(name);
  }

  AstStorage ast_storage_;
};

class CypherMainVisitorTest
    : public ::testing::TestWithParam<std::shared_ptr<Base>> {};

std::shared_ptr<Base> gAstGeneratorTypes[] = {
    std::make_shared<AstGenerator>(),
    std::make_shared<OriginalAfterCloningAstGenerator>(),
    std::make_shared<ClonedAstGenerator>(),
    std::make_shared<CachedAstGenerator>(),
};

INSTANTIATE_TEST_CASE_P(AstGeneratorTypes, CypherMainVisitorTest,
                        ::testing::ValuesIn(gAstGeneratorTypes));

// NOTE: The above used to use *Typed Tests* functionality of gtest library.
// Unfortunately, the compilation time of this test increased to full 2 minutes!
// Although using Typed Tests is the recommended way to achieve what we want, we
// are (ab)using *Value-Parameterized Tests* functionality instead. This cuts
// down the compilation time to about 20 seconds. The original code is here for
// future reference in case someone gets the idea to change to *appropriate*
// Typed Tests mechanism and ruin the compilation times.
//
// typedef ::testing::Types<AstGenerator, OriginalAfterCloningAstGenerator,
//                          ClonedAstGenerator, CachedAstGenerator>
//     AstGeneratorTypes;
//
// TYPED_TEST_CASE(CypherMainVisitorTest, AstGeneratorTypes);

TEST_P(CypherMainVisitorTest, SyntaxException) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ()-[*1....2]-()"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, SyntaxExceptionOnTrailingText) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 2 + 2 mirko"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, PropertyLookup) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN n.x"));
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

TEST_P(CypherMainVisitorTest, LabelsTest) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN n:x:y"));
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

TEST_P(CypherMainVisitorTest, EscapedLabel) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN n:`l-$\"'ab``e````l`"));
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

TEST_P(CypherMainVisitorTest, KeywordLabel) {
  for (const auto &label : {"DeLete", "UsER"}) {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery(fmt::format("RETURN n:{}", label)));
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

TEST_P(CypherMainVisitorTest, HexLetterLabel) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN n:a"));
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

TEST_P(CypherMainVisitorTest, ReturnNoDistinctNoBagSemantics) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x"));
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

TEST_P(CypherMainVisitorTest, ReturnDistinct) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN DISTINCT x"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.distinct);
}

TEST_P(CypherMainVisitorTest, ReturnLimit) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x LIMIT 5"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.limit);
  ast_generator.CheckLiteral(return_clause->body_.limit, 5);
}

TEST_P(CypherMainVisitorTest, ReturnSkip) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x SKIP 5"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.skip);
  ast_generator.CheckLiteral(return_clause->body_.skip, 5);
}

TEST_P(CypherMainVisitorTest, ReturnOrderBy) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN x, y, z ORDER BY z ASC, x, y DESC"));
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

TEST_P(CypherMainVisitorTest, ReturnNamedIdentifier) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN var AS var5"));
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

TEST_P(CypherMainVisitorTest, ReturnAsterisk) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN *"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 0U);
}

TEST_P(CypherMainVisitorTest, IntegerLiteral) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 42"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 42, 1);
}

TEST_P(CypherMainVisitorTest, IntegerLiteralTooLarge) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 10000000000000000000000000"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, BooleanLiteralTrue) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN TrUe"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, true, 1);
}

TEST_P(CypherMainVisitorTest, BooleanLiteralFalse) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN faLSE"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, false, 1);
}

TEST_P(CypherMainVisitorTest, NullLiteral) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN nULl"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, TypedValue(),
      1);
}

TEST_P(CypherMainVisitorTest, ParenthesizedExpression) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN (2)"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 2);
}

TEST_P(CypherMainVisitorTest, OrOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN true Or false oR n"));
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

TEST_P(CypherMainVisitorTest, XorOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN true xOr false"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *xor_operator = dynamic_cast<XorOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(xor_operator->expression1_, true);
  ast_generator.CheckLiteral(xor_operator->expression2_, false);
}

TEST_P(CypherMainVisitorTest, AndOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN true and false"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *and_operator = dynamic_cast<AndOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(and_operator->expression1_, true);
  ast_generator.CheckLiteral(and_operator->expression2_, false);
}

TEST_P(CypherMainVisitorTest, AdditionSubtractionOperators) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 1 - 2 + 3"));
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

TEST_P(CypherMainVisitorTest, MulitplicationOperator) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 * 3"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *mult_operator = dynamic_cast<MultiplicationOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(mult_operator->expression1_, 2);
  ast_generator.CheckLiteral(mult_operator->expression2_, 3);
}

TEST_P(CypherMainVisitorTest, DivisionOperator) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 / 3"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *div_operator = dynamic_cast<DivisionOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(div_operator->expression1_, 2);
  ast_generator.CheckLiteral(div_operator->expression2_, 3);
}

TEST_P(CypherMainVisitorTest, ModOperator) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 % 3"));
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

TEST_P(CypherMainVisitorTest, ComparisonOperators) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN 2 = 3 != 4 <> 5 < 6 > 7 <= 8 >= 9"));
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

TEST_P(CypherMainVisitorTest, ListIndexing) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN [1,2,3] [ 2 ]"));
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

TEST_P(CypherMainVisitorTest, ListSlicingOperatorNoBounds) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN [1,2,3] [ .. ]"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, ListSlicingOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN [1,2,3] [ .. 2 ]"));
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

TEST_P(CypherMainVisitorTest, InListOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN 5 IN [1,2]"));
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

TEST_P(CypherMainVisitorTest, InWithListIndexing) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN 1 IN [[1,2]][0]"));
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

TEST_P(CypherMainVisitorTest, CaseGenericForm) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "RETURN CASE WHEN n < 10 THEN 1 WHEN n > 10 THEN 2 END"));
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
  ast_generator.CheckLiteral(if_operator2->else_expression_, TypedValue());
}

TEST_P(CypherMainVisitorTest, CaseGenericFormElse) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN CASE WHEN n < 10 THEN 1 ELSE 2 END"));
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

TEST_P(CypherMainVisitorTest, CaseSimpleForm) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN CASE 5 WHEN 10 THEN 1 END"));
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
  ast_generator.CheckLiteral(if_operator->else_expression_, TypedValue());
}

TEST_P(CypherMainVisitorTest, IsNull) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 iS NulL"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *is_type_operator = dynamic_cast<IsNullOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(is_type_operator->expression_, 2);
}

TEST_P(CypherMainVisitorTest, IsNotNull) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN 2 iS nOT NulL"));
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

TEST_P(CypherMainVisitorTest, NotOperator) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN not true"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *not_operator = dynamic_cast<NotOperator *>(
      return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(not_operator->expression_, true);
}

TEST_P(CypherMainVisitorTest, UnaryMinusPlusOperators) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN -+5"));
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

TEST_P(CypherMainVisitorTest, Aggregation) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "RETURN COUNT(a), MIN(b), MAX(c), SUM(d), AVG(e), COLLECT(f), COUNT(*)"));
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

TEST_P(CypherMainVisitorTest, UndefinedFunction) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery(
                   "RETURN "
                   "IHopeWeWillNeverHaveAwesomeMemgraphProcedureWithS"
                   "uchALongAndAwesomeNameSinceThisTestWouldFail(1)"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, Function) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN abs(n, 2)"));
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

TEST_P(CypherMainVisitorTest, StringLiteralDoubleQuotes) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN \"mi'rko\""));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, "mi'rko", 1);
}

TEST_P(CypherMainVisitorTest, StringLiteralSingleQuotes) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 'mi\"rko'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, "mi\"rko", 1);
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedChars) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "RETURN '\\\\\\'\\\"\\b\\B\\f\\F\\n\\N\\r\\R\\t\\T'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_,
      "\\'\"\b\b\f\f\n\n\r\r\t\t", 1);
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedUtf16) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN '\\u221daaa\\u221daaa'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_,
      u8"\u221daaa\u221daaa", 1);
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedUtf16Error) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN '\\U221daaa'"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedUtf32) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN '\\U0001F600aaaa\\U0001F600aaaaaaaa'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_,
      u8"\U0001F600aaaa\U0001F600aaaaaaaa", 1);
}

TEST_P(CypherMainVisitorTest, DoubleLiteral) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 3.5"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 3.5, 1);
}

TEST_P(CypherMainVisitorTest, DoubleLiteralExponent) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 5e-1"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(
      return_clause->body_.named_expressions[0]->expression_, 0.5, 1);
}

TEST_P(CypherMainVisitorTest, ListLiteral) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN [3, [], 'johhny']"));
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

TEST_P(CypherMainVisitorTest, MapLiteral) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN {a: 1, b: 'bla', c: [1, {a: 42}]}"));
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

TEST_P(CypherMainVisitorTest, NodePattern) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "MATCH (:label1:label2:label3 {a : 5, b : 10}) RETURN 1"));
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
    properties[x.first] = value.ValueInt();
  }
  EXPECT_THAT(properties,
              UnorderedElementsAre(Pair(ast_generator.Prop("a"), 5),
                                   Pair(ast_generator.Prop("b"), 10)));
}

TEST_P(CypherMainVisitorTest, PropertyMapSameKeyAppearsTwice) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("MATCH ({a : 1, a : 2})"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, NodePatternIdentifier) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH (var) RETURN 1"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternNoDetails) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()--() RETURN 1"));
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
  ASSERT_TRUE(node1->identifier_);
  ASSERT_TRUE(edge->identifier_);
  ASSERT_TRUE(node2->identifier_);
  EXPECT_THAT(
      std::vector<std::string>({node1->identifier_->name_,
                                edge->identifier_->name_,
                                node2->identifier_->name_}),
      UnorderedElementsAre(CypherMainVisitor::kAnonPrefix + std::to_string(1),
                           CypherMainVisitor::kAnonPrefix + std::to_string(2),
                           CypherMainVisitor::kAnonPrefix + std::to_string(3)));
  EXPECT_FALSE(node1->identifier_->user_declared_);
  EXPECT_FALSE(edge->identifier_->user_declared_);
  EXPECT_FALSE(node2->identifier_->user_declared_);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::BOTH);
}

// PatternPart in braces.
TEST_P(CypherMainVisitorTest, PatternPartBraces) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ((()--())) RETURN 1"));
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
  ASSERT_TRUE(node1->identifier_);
  ASSERT_TRUE(edge->identifier_);
  ASSERT_TRUE(node2->identifier_);
  EXPECT_THAT(
      std::vector<std::string>({node1->identifier_->name_,
                                edge->identifier_->name_,
                                node2->identifier_->name_}),
      UnorderedElementsAre(CypherMainVisitor::kAnonPrefix + std::to_string(1),
                           CypherMainVisitor::kAnonPrefix + std::to_string(2),
                           CypherMainVisitor::kAnonPrefix + std::to_string(3)));
  EXPECT_FALSE(node1->identifier_->user_declared_);
  EXPECT_FALSE(edge->identifier_->user_declared_);
  EXPECT_FALSE(node2->identifier_->user_declared_);
  EXPECT_EQ(edge->direction_, EdgeAtom::Direction::BOTH);
}

TEST_P(CypherMainVisitorTest, RelationshipPatternDetails) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "MATCH ()<-[:type1|type2 {a : 5, b : 10}]-() RETURN 1"));
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
    properties[x.first] = value.ValueInt();
  }
  EXPECT_THAT(properties,
              UnorderedElementsAre(Pair(ast_generator.Prop("a"), 5),
                                   Pair(ast_generator.Prop("b"), 10)));
}

TEST_P(CypherMainVisitorTest, RelationshipPatternVariable) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[var]->() RETURN 1"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternUnbounded) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r*]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternLowerBounded) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r*42..]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternUpperBounded) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r*..42]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternLowerUpperBounded) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r*24..42]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternFixedRange) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r*42]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternFloatingUpperBound) {
  // [r*1...2] should be parsed as [r*1..0.2]
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r*1...2]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternUnboundedWithProperty) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r* {prop: 42}]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest,
       RelationshipPatternDotsUnboundedWithEdgeTypeProperty) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "MATCH ()-[r:edge_type*..{prop: 42}]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, RelationshipPatternUpperBoundedWithProperty) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r*..2{prop: 42}]->() RETURN r"));
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

// TODO maybe uncomment
// // PatternPart with variable.
// TEST_P(CypherMainVisitorTest, PatternPartVariable) {
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

TEST_P(CypherMainVisitorTest, ReturnUnanemdIdentifier) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN var"));
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

TEST_P(CypherMainVisitorTest, Create) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CREATE (n)"));
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

TEST_P(CypherMainVisitorTest, Delete) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("DELETE n, m"));
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

TEST_P(CypherMainVisitorTest, DeleteDetach) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("DETACH DELETE n"));
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

TEST_P(CypherMainVisitorTest, OptionalMatchWhere) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("OPTIONAL MATCH (n) WHERE m RETURN 1"));
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

TEST_P(CypherMainVisitorTest, Set) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("SET a.x = b, c = d, e += f, g : h : i "));
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

TEST_P(CypherMainVisitorTest, Remove) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("REMOVE a.x, g : h : i"));
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

TEST_P(CypherMainVisitorTest, With) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("WITH n AS m RETURN 1"));
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

TEST_P(CypherMainVisitorTest, WithNonAliasedExpression) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("WITH n.x RETURN 1"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, WithNonAliasedVariable) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH n RETURN 1"));
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

TEST_P(CypherMainVisitorTest, WithDistinct) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("WITH DISTINCT n AS m RETURN 1"));
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

TEST_P(CypherMainVisitorTest, WithBag) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "WITH n as m ORDER BY m SKIP 1 LIMIT 2 RETURN 1"));
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

TEST_P(CypherMainVisitorTest, WithWhere) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("WITH n AS m WHERE k RETURN 1"));
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

TEST_P(CypherMainVisitorTest, WithAnonymousVariableCapture) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("WITH 5 as anon1 MATCH () return *"));
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

TEST_P(CypherMainVisitorTest, ClausesOrdering) {
  // Obviously some of the ridiculous combinations don't fail here, but they
  // will fail in semantic analysis or they make perfect sense as a part of
  // bigger query.
  auto &ast_generator = *GetParam();
  ast_generator.ParseQuery("RETURN 1");
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 RETURN 1"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 MATCH (n) RETURN n"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 DELETE n"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 MERGE (n)"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 WITH n AS m RETURN 1"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 AS n UNWIND n AS x RETURN x"),
               SemanticException);

  ASSERT_THROW(
      ast_generator.ParseQuery("OPTIONAL MATCH (n) MATCH (m) RETURN n, m"),
      SemanticException);
  ast_generator.ParseQuery("OPTIONAL MATCH (n) WITH n MATCH (m) RETURN n, m");
  ast_generator.ParseQuery("OPTIONAL MATCH (n) OPTIONAL MATCH (m) RETURN n, m");
  ast_generator.ParseQuery("MATCH (n) OPTIONAL MATCH (m) RETURN n, m");

  ast_generator.ParseQuery("CREATE (n)");
  ASSERT_THROW(ast_generator.ParseQuery("SET n:x MATCH (n) RETURN n"),
               SemanticException);
  ast_generator.ParseQuery("REMOVE n.x SET n.x = 1");
  ast_generator.ParseQuery("REMOVE n:L RETURN n");
  ast_generator.ParseQuery("SET n.x = 1 WITH n AS m RETURN m");

  ASSERT_THROW(ast_generator.ParseQuery("MATCH (n)"), SemanticException);
  ast_generator.ParseQuery("MATCH (n) MATCH (n) RETURN n");
  ast_generator.ParseQuery("MATCH (n) SET n = m");
  ast_generator.ParseQuery("MATCH (n) RETURN n");
  ast_generator.ParseQuery("MATCH (n) WITH n AS m RETURN m");

  ASSERT_THROW(ast_generator.ParseQuery("WITH 1 AS n"), SemanticException);
  ast_generator.ParseQuery("WITH 1 AS n WITH n AS m RETURN m");
  ast_generator.ParseQuery("WITH 1 AS n RETURN n");
  ast_generator.ParseQuery("WITH 1 AS n SET n += m");
  ast_generator.ParseQuery("WITH 1 AS n MATCH (n) RETURN n");

  ASSERT_THROW(ast_generator.ParseQuery("UNWIND [1,2,3] AS x"),
               SemanticException);
  ASSERT_THROW(
      ast_generator.ParseQuery("CREATE (n) UNWIND [1,2,3] AS x RETURN x"),
      SemanticException);
  ast_generator.ParseQuery("UNWIND [1,2,3] AS x CREATE (n) RETURN x");
  ast_generator.ParseQuery("CREATE (n) WITH n UNWIND [1,2,3] AS x RETURN x");
}

TEST_P(CypherMainVisitorTest, Merge) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MERGE (a) -[:r]- (b) ON MATCH SET a.x = b.x "
                               "ON CREATE SET b :label ON MATCH SET b = a"));
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

TEST_P(CypherMainVisitorTest, Unwind) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("UNWIND [1,2,3] AS elem RETURN elem"));
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

TEST_P(CypherMainVisitorTest, UnwindWithoutAsError) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("UNWIND [1,2,3] RETURN 42"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, CreateIndex) {
  auto &ast_generator = *GetParam();
  auto *index_query = dynamic_cast<IndexQuery *>(
      ast_generator.ParseQuery("Create InDeX oN :mirko(slavko)"));
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::CREATE);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko")};
  EXPECT_EQ(index_query->properties_, expected_properties);
}

TEST_P(CypherMainVisitorTest, DropIndex) {
  auto &ast_generator = *GetParam();
  auto *index_query = dynamic_cast<IndexQuery *>(
      ast_generator.ParseQuery("dRoP InDeX oN :mirko(slavko)"));
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::DROP);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko")};
  EXPECT_EQ(index_query->properties_, expected_properties);
}

TEST_P(CypherMainVisitorTest, DropIndexWithoutProperties) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("dRoP InDeX oN :mirko()"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, DropIndexWithMultipleProperties) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("dRoP InDeX oN :mirko(slavko, pero)"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, ReturnAll) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_THROW(ast_generator.ParseQuery("RETURN all(x in [1,2,3])"),
                 SyntaxException);
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("RETURN all(x IN [1,2,3] WHERE x = 2)"));
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

TEST_P(CypherMainVisitorTest, ReturnSingle) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_THROW(ast_generator.ParseQuery("RETURN single(x in [1,2,3])"),
                 SyntaxException);
  }
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN single(x IN [1,2,3] WHERE x = 2)"));
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

TEST_P(CypherMainVisitorTest, ReturnReduce) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "RETURN reduce(sum = 0, x IN [1,2,3] | sum + x)"));
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

TEST_P(CypherMainVisitorTest, ReturnExtract) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN extract(x IN [1,2,3] | sum + x)"));
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

TEST_P(CypherMainVisitorTest, MatchBfsReturn) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "MATCH (n) -[r:type1|type2 *bfs..10 (e, n|e.prop = 42)]-> (m) RETURN r"));
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

TEST_P(CypherMainVisitorTest, MatchVariableLambdaSymbols) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH () -[*]- () RETURN *"));
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

TEST_P(CypherMainVisitorTest, MatchWShortestReturn) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "MATCH ()-[r:type1|type2 *wShortest 10 (we, wn | 42) total_weight "
      "(e, n | true)]->() RETURN r"));
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

TEST_P(CypherMainVisitorTest, MatchWShortestNoFilterReturn) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "MATCH ()-[r:type1|type2 *wShortest 10 (we, wn | 42)]->() "
      "RETURN r"));
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

TEST_P(CypherMainVisitorTest, SemanticExceptionOnWShortestLowerBound) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery(
                   "MATCH ()-[r *wShortest 10.. (e, n | 42)]-() RETURN r"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery(
                   "MATCH ()-[r *wShortest 10..20 (e, n | 42)]-() RETURN r"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, SemanticExceptionOnWShortestWithoutLambda) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("MATCH ()-[r *wShortest]-() RETURN r"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, SemanticExceptionOnUnionTypeMix) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery(
                   "RETURN 5 as X UNION ALL RETURN 6 AS X UNION RETURN 7 AS X"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery(
                   "RETURN 5 as X UNION RETURN 6 AS X UNION ALL RETURN 7 AS X"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, Union) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "RETURN 5 AS X, 6 AS Y UNION RETURN 6 AS X, 5 AS Y"));
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

TEST_P(CypherMainVisitorTest, UnionAll) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "RETURN 5 AS X UNION ALL RETURN 6 AS X UNION ALL RETURN 7 AS X"));
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

void check_auth_query(Base *ast_generator, std::string input,
                      AuthQuery::Action action, std::string user,
                      std::string role, std::string user_or_role,
                      std::optional<TypedValue> password,
                      std::vector<AuthQuery::Privilege> privileges) {
  auto *auth_query =
      dynamic_cast<AuthQuery *>(ast_generator->ParseQuery(input));
  ASSERT_TRUE(auth_query);
  EXPECT_EQ(auth_query->action_, action);
  EXPECT_EQ(auth_query->user_, user);
  EXPECT_EQ(auth_query->role_, role);
  EXPECT_EQ(auth_query->user_or_role_, user_or_role);
  ASSERT_EQ(static_cast<bool>(auth_query->password_),
            static_cast<bool>(password));
  if (password) {
    ast_generator->CheckLiteral(auth_query->password_, *password);
  }
  EXPECT_EQ(auth_query->privileges_, privileges);
}

TEST_P(CypherMainVisitorTest, UserOrRoleName) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ROLE `us|er`"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ROLE `us er`"),
               SyntaxException);
  check_auth_query(&ast_generator, "CREATE ROLE `user`",
                   AuthQuery::Action::CREATE_ROLE, "", "user", "", {}, {});
  check_auth_query(&ast_generator, "CREATE ROLE us___er",
                   AuthQuery::Action::CREATE_ROLE, "", "us___er", "", {}, {});
  check_auth_query(&ast_generator, "CREATE ROLE `us+er`",
                   AuthQuery::Action::CREATE_ROLE, "", "us+er", "", {}, {});
}

TEST_P(CypherMainVisitorTest, CreateRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ROLE"), SyntaxException);
  check_auth_query(&ast_generator, "CREATE ROLE rola",
                   AuthQuery::Action::CREATE_ROLE, "", "rola", "", {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ROLE lagano rolamo"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, DropRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("DROP ROLE"), SyntaxException);
  check_auth_query(&ast_generator, "DROP ROLE rola",
                   AuthQuery::Action::DROP_ROLE, "", "rola", "", {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("DROP ROLE lagano rolamo"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowRoles) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW ROLES ROLES"), SyntaxException);
  check_auth_query(&ast_generator, "SHOW ROLES", AuthQuery::Action::SHOW_ROLES,
                   "", "", "", {}, {});
}

TEST_P(CypherMainVisitorTest, CreateUser) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER 123"), SyntaxException);
  check_auth_query(&ast_generator, "CREATE USER user",
                   AuthQuery::Action::CREATE_USER, "user", "", "", {}, {});
  check_auth_query(&ast_generator, "CREATE USER user IDENTIFIED BY 'password'",
                   AuthQuery::Action::CREATE_USER, "user", "", "",
                   TypedValue("password"), {});
  check_auth_query(&ast_generator, "CREATE USER user IDENTIFIED BY ''",
                   AuthQuery::Action::CREATE_USER, "user", "", "",
                   TypedValue(""), {});
  check_auth_query(&ast_generator, "CREATE USER user IDENTIFIED BY null",
                   AuthQuery::Action::CREATE_USER, "user", "", "", TypedValue(),
                   {});
  ASSERT_THROW(
      ast_generator.ParseQuery("CRATE USER user IDENTIFIED BY password"),
      SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER user IDENTIFIED BY 5"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER user IDENTIFIED BY "),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, SetPassword) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SET PASSWORD FOR"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET PASSWORD FOR user "),
               SyntaxException);
  check_auth_query(&ast_generator, "SET PASSWORD FOR user TO null",
                   AuthQuery::Action::SET_PASSWORD, "user", "", "",
                   TypedValue(), {});
  check_auth_query(&ast_generator, "SET PASSWORD FOR user TO 'password'",
                   AuthQuery::Action::SET_PASSWORD, "user", "", "",
                   TypedValue("password"), {});
  ASSERT_THROW(ast_generator.ParseQuery("SET PASSWORD FOR user To 5"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, DropUser) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("DROP USER"), SyntaxException);
  check_auth_query(&ast_generator, "DROP USER user",
                   AuthQuery::Action::DROP_USER, "user", "", "", {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("DROP USER lagano rolamo"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowUsers) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW USERS ROLES"), SyntaxException);
  check_auth_query(&ast_generator, "SHOW USERS", AuthQuery::Action::SHOW_USERS,
                   "", "", "", {}, {});
}

TEST_P(CypherMainVisitorTest, SetRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE FOR user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE FOR user TO"),
               SyntaxException);
  check_auth_query(&ast_generator, "SET ROLE FOR user TO role",
                   AuthQuery::Action::SET_ROLE, "user", "role", "", {}, {});
  check_auth_query(&ast_generator, "SET ROLE FOR user TO null",
                   AuthQuery::Action::SET_ROLE, "user", "null", "", {}, {});
}

TEST_P(CypherMainVisitorTest, ClearRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CLEAR ROLE"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CLEAR ROLE user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CLEAR ROLE FOR user TO"),
               SyntaxException);
  check_auth_query(&ast_generator, "CLEAR ROLE FOR user",
                   AuthQuery::Action::CLEAR_ROLE, "user", "", "", {}, {});
}

TEST_P(CypherMainVisitorTest, GrantPrivilege) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("GRANT"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT BLABLA TO user"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT MATCH, TO user"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT MATCH, BLABLA TO user"),
               SyntaxException);
  check_auth_query(&ast_generator, "GRANT MATCH TO user",
                   AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH});
  check_auth_query(&ast_generator, "GRANT MATCH, AUTH TO user",
                   AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH});
}

TEST_P(CypherMainVisitorTest, DenyPrivilege) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("DENY"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY BLABLA TO user"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY MATCH, TO user"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY MATCH, BLABLA TO user"),
               SyntaxException);
  check_auth_query(&ast_generator, "DENY MATCH TO user",
                   AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH});
  check_auth_query(&ast_generator, "DENY MATCH, AUTH TO user",
                   AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH});
}

TEST_P(CypherMainVisitorTest, RevokePrivilege) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE FROM user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE BLABLA FROM user"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE MATCH, FROM user"),
               SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE MATCH, BLABLA FROM user"),
               SyntaxException);
  check_auth_query(&ast_generator, "REVOKE MATCH FROM user",
                   AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH});
  check_auth_query(&ast_generator, "REVOKE MATCH, AUTH FROM user",
                   AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH});
  check_auth_query(&ast_generator, "REVOKE ALL PRIVILEGES FROM user",
                   AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   kPrivilegesAll);
}

TEST_P(CypherMainVisitorTest, ShowPrivileges) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW PRIVILEGES FOR"),
               SyntaxException);
  check_auth_query(&ast_generator, "SHOW PRIVILEGES FOR user",
                   AuthQuery::Action::SHOW_PRIVILEGES, "", "", "user", {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("SHOW PRIVILEGES FOR user1, user2"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowRoleForUser) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW ROLE FOR "), SyntaxException);
  check_auth_query(&ast_generator, "SHOW ROLE FOR user",
                   AuthQuery::Action::SHOW_ROLE_FOR_USER, "user", "", "", {},
                   {});
  ASSERT_THROW(ast_generator.ParseQuery("SHOW ROLE FOR user1, user2"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowUsersForRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW USERS FOR "), SyntaxException);
  check_auth_query(&ast_generator, "SHOW USERS FOR role",
                   AuthQuery::Action::SHOW_USERS_FOR_ROLE, "", "role", "", {},
                   {});
  ASSERT_THROW(ast_generator.ParseQuery("SHOW USERS FOR role1, role2"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestExplainRegularQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_TRUE(dynamic_cast<ExplainQuery *>(
      ast_generator.ParseQuery("EXPLAIN RETURN n")));
}

TEST_P(CypherMainVisitorTest, TestExplainExplainQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("EXPLAIN EXPLAIN RETURN n"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestExplainAuthQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("EXPLAIN SHOW ROLES"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestProfileRegularQuery) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_TRUE(dynamic_cast<ProfileQuery *>(
        ast_generator.ParseQuery("PROFILE RETURN n")));
  }
}

TEST_P(CypherMainVisitorTest, TestProfileComplicatedQuery) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_TRUE(dynamic_cast<ProfileQuery *>(ast_generator.ParseQuery(
        "profile optional match (n) where n.hello = 5 "
        "return n union optional match (n) where n.there = 10 "
        "return n")));
  }
}

TEST_P(CypherMainVisitorTest, TestProfileProfileQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("PROFILE PROFILE RETURN n"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestProfileAuthQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("PROFILE SHOW ROLES"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestShowStorageInfo) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<InfoQuery *>(ast_generator.ParseQuery("SHOW STORAGE INFO"));
  ASSERT_TRUE(query);
  EXPECT_EQ(query->info_type_, InfoQuery::InfoType::STORAGE);
}

TEST_P(CypherMainVisitorTest, TestShowIndexInfo) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<InfoQuery *>(ast_generator.ParseQuery("SHOW INDEX INFO"));
  ASSERT_TRUE(query);
  EXPECT_EQ(query->info_type_, InfoQuery::InfoType::INDEX);
}

TEST_P(CypherMainVisitorTest, TestShowConstraintInfo) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<InfoQuery *>(
      ast_generator.ParseQuery("SHOW CONSTRAINT INFO"));
  ASSERT_TRUE(query);
  EXPECT_EQ(query->info_type_, InfoQuery::InfoType::CONSTRAINT);
}

TEST_P(CypherMainVisitorTest, CreateConstraintSyntaxError) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(
      ast_generator.ParseQuery("CREATE CONSTRAINT ON (:label) ASSERT EXISTS"),
      SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT () ASSERT EXISTS"),
               SyntaxException);
  EXPECT_THROW(
      ast_generator.ParseQuery("CREATE CONSTRAINT ON () ASSERT EXISTS(prop1)"),
      SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery(
                   "CREATE CONSTRAINT ON () ASSERT EXISTS (prop1, prop2)"),
               SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "EXISTS (n.prop1, missing.prop2)"),
               SemanticException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "EXISTS (m.prop1, m.prop2)"),
               SemanticException);

  EXPECT_THROW(ast_generator.ParseQuery(
                   "CREATE CONSTRAINT ON (:label) ASSERT IS UNIQUE"),
               SyntaxException);
  EXPECT_THROW(
      ast_generator.ParseQuery("CREATE CONSTRAINT () ASSERT IS UNIQUE"),
      SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery(
                   "CREATE CONSTRAINT ON () ASSERT prop1 IS UNIQUE"),
               SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery(
                   "CREATE CONSTRAINT ON () ASSERT prop1, prop2 IS UNIQUE"),
               SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "n.prop1, missing.prop2 IS UNIQUE"),
               SemanticException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "m.prop1, m.prop2 IS UNIQUE"),
               SemanticException);

  EXPECT_THROW(ast_generator.ParseQuery(
                   "CREATE CONSTRAINT ON (:label) ASSERT IS NODE KEY"),
               SyntaxException);
  EXPECT_THROW(
      ast_generator.ParseQuery("CREATE CONSTRAINT () ASSERT IS NODE KEY"),
      SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery(
                   "CREATE CONSTRAINT ON () ASSERT (prop1) IS NODE KEY"),
               SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery(
                   "CREATE CONSTRAINT ON () ASSERT (prop1, prop2) IS NODE KEY"),
               SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "(n.prop1, missing.prop2) IS NODE KEY"),
               SemanticException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "(m.prop1, m.prop2) IS NODE KEY"),
               SemanticException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "n.prop1, n.prop2 IS NODE KEY"),
               SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "exists(n.prop1, n.prop2) IS NODE KEY"),
               SyntaxException);
}

TEST_P(CypherMainVisitorTest, CreateConstraint) {
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "CREATE CONSTRAINT ON (n:label) ASSERT EXISTS(n.prop1)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "CREATE CONSTRAINT ON (n:label) ASSERT EXISTS (n.prop1, n.prop2)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"),
                                     ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "CREATE CONSTRAINT ON (n:label) ASSERT n.prop1 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "CREATE CONSTRAINT ON (n:label) ASSERT n.prop1, n.prop2 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"),
                                     ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "CREATE CONSTRAINT ON (n:label) ASSERT (n.prop1) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                 "(n.prop1, n.prop2) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"),
                                     ast_generator.Prop("prop2")));
  }
}

TEST_P(CypherMainVisitorTest, DropConstraint) {
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "DROP CONSTRAINT ON (n:label) ASSERT EXISTS(n.prop1)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "DROP CONSTRAINT ON (n:label) ASSERT EXISTS(n.prop1, n.prop2)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"),
                                     ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "DROP CONSTRAINT ON (n:label) ASSERT n.prop1 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "DROP CONSTRAINT ON (n:label) ASSERT n.prop1, n.prop2 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"),
                                     ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery(
        "DROP CONSTRAINT ON (n:label) ASSERT (n.prop1) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("DROP CONSTRAINT ON (n:label) ASSERT "
                                 "(n.prop1, n.prop2) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"),
                                     ast_generator.Prop("prop2")));
  }
}

TEST_P(CypherMainVisitorTest, RegexMatch) {
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
        "MATCH (n) WHERE n.name =~ \".*bla.*\" RETURN n.name"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 2U);
    auto *match_clause = dynamic_cast<Match *>(single_query->clauses_[0]);
    ASSERT_TRUE(match_clause);
    auto *regex_match =
        dynamic_cast<RegexMatch *>(match_clause->where_->expression_);
    ASSERT_TRUE(regex_match);
    ASSERT_TRUE(dynamic_cast<PropertyLookup *>(regex_match->string_expr_));
    ast_generator.CheckLiteral(regex_match->regex_, ".*bla.*");
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("RETURN \"text\" =~ \".*bla.*\""));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
    ASSERT_TRUE(return_clause);
    ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
    auto *named_expression = return_clause->body_.named_expressions[0];
    auto *regex_match =
        dynamic_cast<RegexMatch *>(named_expression->expression_);
    ASSERT_TRUE(regex_match);
    ast_generator.CheckLiteral(regex_match->string_expr_, "text");
    ast_generator.CheckLiteral(regex_match->regex_, ".*bla.*");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(CypherMainVisitorTest, DumpDatabase) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<DumpQuery *>(ast_generator.ParseQuery("DUMP DATABASE"));
  ASSERT_TRUE(query);
}

}  // namespace
