// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <climits>
#include <limits>
#include <optional>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

//////////////////////////////////////////////////////
// "json.hpp" should always come before "antlr4-runtime.h"
// "json.hpp" uses libc's EOF macro while
// "antlr4-runtime.h" contains a static variable of the
// same name, EOF.
// This hides the definition of the macro which causes
// the compilation to fail.
#include <json/json.hpp>
//////////////////////////////////////////////////////
#include <antlr4-runtime.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/frontend/stripped.hpp"
#include "query/procedure/cypher_types.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "query/typed_value.hpp"

#include "utils/string.hpp"
#include "utils/variant_helpers.hpp"

using namespace memgraph::query;
using namespace memgraph::query::frontend;
using memgraph::query::TypedValue;
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
      return TypedValue(parameters_.AtTokenPosition(param_lookup->token_position_));
    } else {
      auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
      return TypedValue(literal->value_);
    }
  }

  TypedValue GetLiteral(Expression *expression, const bool use_parameter_lookup,
                        const std::optional<int> &token_position = std::nullopt) const {
    if (use_parameter_lookup) {
      auto *param_lookup = dynamic_cast<ParameterLookup *>(expression);
      if (param_lookup == nullptr) {
        ADD_FAILURE();
        return {};
      }
      if (token_position) {
        EXPECT_EQ(param_lookup->token_position_, *token_position);
      }
      return TypedValue(parameters_.AtTokenPosition(param_lookup->token_position_));
    }

    auto *literal = dynamic_cast<PrimitiveLiteral *>(expression);
    if (literal == nullptr) {
      ADD_FAILURE();
      return {};
    }
    if (token_position) {
      EXPECT_EQ(literal->token_position_, *token_position);
    }
    return TypedValue(literal->value_);
  }

  template <class TValue>
  void CheckLiteral(Expression *expression, const TValue &expected,
                    const std::optional<int> &token_position = std::nullopt) const {
    TypedValue expected_tv(expected);
    const auto use_parameter_lookup = !expected_tv.IsNull() && context_.is_query_cached;
    TypedValue value = GetLiteral(expression, use_parameter_lookup, token_position);
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

  PropertyIx Prop(const std::string &prop_name) override { return ast_storage_.GetPropertyIx(prop_name); }

  LabelIx Label(const std::string &name) override { return ast_storage_.GetLabelIx(name); }

  EdgeTypeIx EdgeType(const std::string &name) override { return ast_storage_.GetEdgeTypeIx(name); }

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

  PropertyIx Prop(const std::string &prop_name) override { return ast_storage_.GetPropertyIx(prop_name); }

  LabelIx Label(const std::string &name) override { return ast_storage_.GetLabelIx(name); }

  EdgeTypeIx EdgeType(const std::string &name) override { return ast_storage_.GetEdgeTypeIx(name); }

  AstStorage ast_storage_;
};

// This generator strips ast, clones it and then plugs stripped out literals in
// the same way it is done in ast caching in interpreter.
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

  PropertyIx Prop(const std::string &prop_name) override { return ast_storage_.GetPropertyIx(prop_name); }

  LabelIx Label(const std::string &name) override { return ast_storage_.GetLabelIx(name); }

  EdgeTypeIx EdgeType(const std::string &name) override { return ast_storage_.GetEdgeTypeIx(name); }

  AstStorage ast_storage_;
};

class MockModule : public procedure::Module {
 public:
  MockModule(){};
  ~MockModule() override{};
  MockModule(const MockModule &) = delete;
  MockModule(MockModule &&) = delete;
  MockModule &operator=(const MockModule &) = delete;
  MockModule &operator=(MockModule &&) = delete;

  bool Close() override { return true; };

  const std::map<std::string, mgp_proc, std::less<>> *Procedures() const override { return &procedures; }

  const std::map<std::string, mgp_trans, std::less<>> *Transformations() const override { return &transformations; }

  const std::map<std::string, mgp_func, std::less<>> *Functions() const override { return &functions; }

  std::optional<std::filesystem::path> Path() const override { return std::nullopt; };

  std::map<std::string, mgp_proc, std::less<>> procedures{};
  std::map<std::string, mgp_trans, std::less<>> transformations{};
  std::map<std::string, mgp_func, std::less<>> functions{};
};

void DummyProcCallback(mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result * /*result*/, mgp_memory * /*memory*/){};
void DummyFuncCallback(mgp_list * /*args*/, mgp_func_context * /*func_ctx*/, mgp_func_result * /*result*/,
                       mgp_memory * /*memory*/){};

enum class ProcedureType { WRITE, READ };

std::string ToString(const ProcedureType type) { return type == ProcedureType::WRITE ? "write" : "read"; }

class CypherMainVisitorTest : public ::testing::TestWithParam<std::shared_ptr<Base>> {
 public:
  void SetUp() override {
    {
      auto mock_module_owner = std::make_unique<MockModule>();
      mock_module = mock_module_owner.get();
      procedure::gModuleRegistry.RegisterModule("mock_module", std::move(mock_module_owner));
    }
    {
      auto mock_module_with_dots_in_name_owner = std::make_unique<MockModule>();
      mock_module_with_dots_in_name = mock_module_with_dots_in_name_owner.get();
      procedure::gModuleRegistry.RegisterModule("mock_module.with.dots.in.name",
                                                std::move(mock_module_with_dots_in_name_owner));
    }
  }

  void TearDown() override {
    // To release any_type
    procedure::gModuleRegistry.UnloadAllModules();
  }

  static void AddProc(MockModule &module, const char *name, const std::vector<std::string_view> &args,
                      const std::vector<std::string_view> &results, const ProcedureType type) {
    memgraph::utils::MemoryResource *memory = memgraph::utils::NewDeleteResource();
    const bool is_write = type == ProcedureType::WRITE;
    mgp_proc proc(name, DummyProcCallback, memory, {.is_write = is_write});
    for (const auto arg : args) {
      proc.args.emplace_back(memgraph::utils::pmr::string{arg, memory}, &any_type);
    }
    for (const auto result : results) {
      proc.results.emplace(memgraph::utils::pmr::string{result, memory}, std::make_pair(&any_type, false));
    }
    module.procedures.emplace(name, std::move(proc));
  }

  static void AddFunc(MockModule &module, const char *name, const std::vector<std::string_view> &args) {
    memgraph::utils::MemoryResource *memory = memgraph::utils::NewDeleteResource();
    mgp_func func(name, DummyFuncCallback, memory);
    for (const auto arg : args) {
      func.args.emplace_back(memgraph::utils::pmr::string{arg, memory}, &any_type);
    }
    module.functions.emplace(name, std::move(func));
  }

  std::string CreateProcByType(const ProcedureType type, const std::vector<std::string_view> &args) {
    const auto proc_name = std::string{"proc_"} + ToString(type);
    SCOPED_TRACE(proc_name);
    AddProc(*mock_module, proc_name.c_str(), {}, args, type);
    return std::string{"mock_module."} + proc_name;
  }

  static const procedure::AnyType any_type;
  MockModule *mock_module{nullptr};
  MockModule *mock_module_with_dots_in_name{nullptr};
};

const procedure::AnyType CypherMainVisitorTest::any_type{};

std::shared_ptr<Base> gAstGeneratorTypes[] = {
    std::make_shared<AstGenerator>(),
    std::make_shared<OriginalAfterCloningAstGenerator>(),
    std::make_shared<ClonedAstGenerator>(),
    std::make_shared<CachedAstGenerator>(),
};

INSTANTIATE_TEST_CASE_P(AstGeneratorTypes, CypherMainVisitorTest, ::testing::ValuesIn(gAstGeneratorTypes));

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
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ()-[*1....2]-()"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, SyntaxExceptionOnTrailingText) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 2 + 2 mirko"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, PropertyLookup) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN n.x"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *property_lookup = dynamic_cast<PropertyLookup *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(property_lookup->expression_);
  auto identifier = dynamic_cast<Identifier *>(property_lookup->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_EQ(property_lookup->property_, ast_generator.Prop("x"));
}

TEST_P(CypherMainVisitorTest, LabelsTest) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN n:x:y"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(labels_test->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  ASSERT_TRUE(identifier);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_, ElementsAre(ast_generator.Label("x"), ast_generator.Label("y")));
}

TEST_P(CypherMainVisitorTest, EscapedLabel) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN n:`l-$\"'ab``e````l`"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(return_clause->body_.named_expressions[0]->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  ASSERT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_, ElementsAre(ast_generator.Label("l-$\"'ab`e``l")));
}

TEST_P(CypherMainVisitorTest, KeywordLabel) {
  for (const auto &label : {"DeLete", "UsER"}) {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(fmt::format("RETURN n:{}", label)));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
    auto *labels_test = dynamic_cast<LabelsTest *>(return_clause->body_.named_expressions[0]->expression_);
    auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
    ASSERT_EQ(identifier->name_, "n");
    ASSERT_THAT(labels_test->labels_, ElementsAre(ast_generator.Label(label)));
  }
}

TEST_P(CypherMainVisitorTest, HexLetterLabel) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN n:a"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *labels_test = dynamic_cast<LabelsTest *>(return_clause->body_.named_expressions[0]->expression_);
  auto identifier = dynamic_cast<Identifier *>(labels_test->expression_);
  EXPECT_EQ(identifier->name_, "n");
  ASSERT_THAT(labels_test->labels_, ElementsAre(ast_generator.Label("a")));
}

TEST_P(CypherMainVisitorTest, ReturnNoDistinctNoBagSemantics) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN DISTINCT x"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.distinct);
}

TEST_P(CypherMainVisitorTest, ReturnLimit) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x LIMIT 5"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x SKIP 5"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x, y, z ORDER BY z ASC, x, y DESC"));
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
  ASSERT_THAT(ordering,
              UnorderedElementsAre(Pair(Ordering::ASC, "z"), Pair(Ordering::ASC, "x"), Pair(Ordering::DESC, "y")));
}

TEST_P(CypherMainVisitorTest, ReturnNamedIdentifier) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN var AS var5"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN *"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(return_clause->body_.all_identifiers);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 0U);
}

TEST_P(CypherMainVisitorTest, IntegerLiteral) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 42"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, 42, 1);
}

TEST_P(CypherMainVisitorTest, IntegerLiteralTooLarge) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 10000000000000000000000000"), SemanticException);
}

TEST_P(CypherMainVisitorTest, BooleanLiteralTrue) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN TrUe"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, true, 1);
}

TEST_P(CypherMainVisitorTest, BooleanLiteralFalse) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN faLSE"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, false, 1);
}

TEST_P(CypherMainVisitorTest, NullLiteral) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN nULl"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, TypedValue(), 1);
}

TEST_P(CypherMainVisitorTest, ParenthesizedExpression) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN (2)"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, 2);
}

TEST_P(CypherMainVisitorTest, OrOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN true Or false oR n"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *or_operator2 = dynamic_cast<OrOperator *>(return_clause->body_.named_expressions[0]->expression_);
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN true xOr false"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *xor_operator = dynamic_cast<XorOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(xor_operator->expression1_, true);
  ast_generator.CheckLiteral(xor_operator->expression2_, false);
}

TEST_P(CypherMainVisitorTest, AndOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN true and false"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *and_operator = dynamic_cast<AndOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(and_operator->expression1_, true);
  ast_generator.CheckLiteral(and_operator->expression2_, false);
}

TEST_P(CypherMainVisitorTest, AdditionSubtractionOperators) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 1 - 2 + 3"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *addition_operator = dynamic_cast<AdditionOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(addition_operator);
  auto *subtraction_operator = dynamic_cast<SubtractionOperator *>(addition_operator->expression1_);
  ASSERT_TRUE(subtraction_operator);
  ast_generator.CheckLiteral(subtraction_operator->expression1_, 1);
  ast_generator.CheckLiteral(subtraction_operator->expression2_, 2);
  ast_generator.CheckLiteral(addition_operator->expression2_, 3);
}

TEST_P(CypherMainVisitorTest, MultiplicationOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 * 3"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *mult_operator = dynamic_cast<MultiplicationOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(mult_operator->expression1_, 2);
  ast_generator.CheckLiteral(mult_operator->expression2_, 3);
}

TEST_P(CypherMainVisitorTest, DivisionOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 / 3"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *div_operator = dynamic_cast<DivisionOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(div_operator->expression1_, 2);
  ast_generator.CheckLiteral(div_operator->expression2_, 3);
}

TEST_P(CypherMainVisitorTest, ModOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 % 3"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *mod_operator = dynamic_cast<ModOperator *>(return_clause->body_.named_expressions[0]->expression_);
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 = 3 != 4 <> 5 < 6 > 7 <= 8 >= 9"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  Expression *_operator = return_clause->body_.named_expressions[0]->expression_;
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN [1,2,3] [ 2 ]"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *list_index_op = dynamic_cast<SubscriptOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_index_op);
  auto *list = dynamic_cast<ListLiteral *>(list_index_op->expression1_);
  EXPECT_TRUE(list);
  ast_generator.CheckLiteral(list_index_op->expression2_, 2);
}

TEST_P(CypherMainVisitorTest, ListSlicingOperatorNoBounds) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN [1,2,3] [ .. ]"), SemanticException);
}

TEST_P(CypherMainVisitorTest, ListSlicingOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN [1,2,3] [ .. 2 ]"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *list_slicing_op = dynamic_cast<ListSlicingOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(list_slicing_op);
  auto *list = dynamic_cast<ListLiteral *>(list_slicing_op->list_);
  EXPECT_TRUE(list);
  EXPECT_FALSE(list_slicing_op->lower_bound_);
  ast_generator.CheckLiteral(list_slicing_op->upper_bound_, 2);
}

TEST_P(CypherMainVisitorTest, InListOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 5 IN [1,2]"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *in_list_operator = dynamic_cast<InListOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(in_list_operator);
  ast_generator.CheckLiteral(in_list_operator->expression1_, 5);
  auto *list = dynamic_cast<ListLiteral *>(in_list_operator->expression2_);
  ASSERT_TRUE(list);
}

TEST_P(CypherMainVisitorTest, InWithListIndexing) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 1 IN [[1,2]][0]"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *in_list_operator = dynamic_cast<InListOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(in_list_operator);
  ast_generator.CheckLiteral(in_list_operator->expression1_, 1);
  auto *list_indexing = dynamic_cast<SubscriptOperator *>(in_list_operator->expression2_);
  ASSERT_TRUE(list_indexing);
  auto *list = dynamic_cast<ListLiteral *>(list_indexing->expression1_);
  EXPECT_TRUE(list);
  ast_generator.CheckLiteral(list_indexing->expression2_, 0);
}

TEST_P(CypherMainVisitorTest, CaseGenericForm) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN CASE WHEN n < 10 THEN 1 WHEN n > 10 THEN 2 END"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(if_operator);
  auto *condition = dynamic_cast<LessOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  ast_generator.CheckLiteral(if_operator->then_expression_, 1);

  auto *if_operator2 = dynamic_cast<IfOperator *>(if_operator->else_expression_);
  ASSERT_TRUE(if_operator2);
  auto *condition2 = dynamic_cast<GreaterOperator *>(if_operator2->condition_);
  ASSERT_TRUE(condition2);
  ast_generator.CheckLiteral(if_operator2->then_expression_, 2);
  ast_generator.CheckLiteral(if_operator2->else_expression_, TypedValue());
}

TEST_P(CypherMainVisitorTest, CaseGenericFormElse) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN CASE WHEN n < 10 THEN 1 ELSE 2 END"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(return_clause->body_.named_expressions[0]->expression_);
  auto *condition = dynamic_cast<LessOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  ast_generator.CheckLiteral(if_operator->then_expression_, 1);
  ast_generator.CheckLiteral(if_operator->else_expression_, 2);
}

TEST_P(CypherMainVisitorTest, CaseSimpleForm) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN CASE 5 WHEN 10 THEN 1 END"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *if_operator = dynamic_cast<IfOperator *>(return_clause->body_.named_expressions[0]->expression_);
  auto *condition = dynamic_cast<EqualOperator *>(if_operator->condition_);
  ASSERT_TRUE(condition);
  ast_generator.CheckLiteral(condition->expression1_, 5);
  ast_generator.CheckLiteral(condition->expression2_, 10);
  ast_generator.CheckLiteral(if_operator->then_expression_, 1);
  ast_generator.CheckLiteral(if_operator->else_expression_, TypedValue());
}

TEST_P(CypherMainVisitorTest, IsNull) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 iS NulL"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *is_type_operator = dynamic_cast<IsNullOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(is_type_operator->expression_, 2);
}

TEST_P(CypherMainVisitorTest, IsNotNull) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 2 iS nOT NulL"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *not_operator = dynamic_cast<NotOperator *>(return_clause->body_.named_expressions[0]->expression_);
  auto *is_type_operator = dynamic_cast<IsNullOperator *>(not_operator->expression_);
  ast_generator.CheckLiteral(is_type_operator->expression_, 2);
}

TEST_P(CypherMainVisitorTest, NotOperator) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN not true"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *not_operator = dynamic_cast<NotOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ast_generator.CheckLiteral(not_operator->expression_, true);
}

TEST_P(CypherMainVisitorTest, UnaryMinusPlusOperators) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN -+5"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *unary_minus_operator =
      dynamic_cast<UnaryMinusOperator *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(unary_minus_operator);
  auto *unary_plus_operator = dynamic_cast<UnaryPlusOperator *>(unary_minus_operator->expression_);
  ASSERT_TRUE(unary_plus_operator);
  ast_generator.CheckLiteral(unary_plus_operator->expression_, 5);
}

TEST_P(CypherMainVisitorTest, Aggregation) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN COUNT(a), MIN(b), MAX(c), SUM(d), AVG(e), COLLECT(f), COUNT(*)"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 7U);
  Aggregation::Op ops[] = {Aggregation::Op::COUNT, Aggregation::Op::MIN, Aggregation::Op::MAX,
                           Aggregation::Op::SUM,   Aggregation::Op::AVG, Aggregation::Op::COLLECT_LIST};
  std::string ids[] = {"a", "b", "c", "d", "e", "f"};
  for (int i = 0; i < 6; ++i) {
    auto *aggregation = dynamic_cast<Aggregation *>(return_clause->body_.named_expressions[i]->expression_);
    ASSERT_TRUE(aggregation);
    ASSERT_EQ(aggregation->op_, ops[i]);
    auto *identifier = dynamic_cast<Identifier *>(aggregation->expression1_);
    ASSERT_TRUE(identifier);
    ASSERT_EQ(identifier->name_, ids[i]);
  }
  auto *aggregation = dynamic_cast<Aggregation *>(return_clause->body_.named_expressions[6]->expression_);
  ASSERT_TRUE(aggregation);
  ASSERT_EQ(aggregation->op_, Aggregation::Op::COUNT);
  ASSERT_FALSE(aggregation->expression1_);
}

TEST_P(CypherMainVisitorTest, UndefinedFunction) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN "
                                        "IHopeWeWillNeverHaveAwesomeMemgraphProcedureWithS"
                                        "uchALongAndAwesomeNameSinceThisTestWouldFail(1)"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, MissingFunction) {
  AddFunc(*mock_module, "get", {});
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN missing_function.get()"), SemanticException);
}

TEST_P(CypherMainVisitorTest, Function) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN abs(n, 2)"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1);
  auto *function = dynamic_cast<Function *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(function);
  ASSERT_TRUE(function->function_);
}

TEST_P(CypherMainVisitorTest, MagicFunction) {
  AddFunc(*mock_module, "get", {});
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN mock_module.get()"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_EQ(return_clause->body_.named_expressions.size(), 1);
  auto *function = dynamic_cast<Function *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(function);
  ASSERT_TRUE(function->function_);
}

TEST_P(CypherMainVisitorTest, StringLiteralDoubleQuotes) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN \"mi'rko\""));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, "mi'rko", 1);
}

TEST_P(CypherMainVisitorTest, StringLiteralSingleQuotes) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 'mi\"rko'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, "mi\"rko", 1);
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedChars) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN '\\\\\\'\\\"\\b\\B\\f\\F\\n\\N\\r\\R\\t\\T'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, "\\'\"\b\b\f\f\n\n\r\r\t\t", 1);
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedUtf16) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN '\\u221daaa\\u221daaa'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_,
                             "\xE2\x88\x9D"
                             "aaa"
                             "\xE2\x88\x9D"
                             "aaa",
                             1);  // u8"\u221daaa\u221daaa"
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedUtf16Error) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN '\\U221daaa'"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, StringLiteralEscapedUtf32) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN '\\U0001F600aaaa\\U0001F600aaaaaaaa'"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_,
                             "\xF0\x9F\x98\x80"
                             "aaaa"
                             "\xF0\x9F\x98\x80"
                             "aaaaaaaa",
                             1);  // u8"\U0001F600aaaa\U0001F600aaaaaaaa"
}

TEST_P(CypherMainVisitorTest, DoubleLiteral) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 3.5"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, 3.5, 1);
}

TEST_P(CypherMainVisitorTest, DoubleLiteralExponent) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 5e-1"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  ast_generator.CheckLiteral(return_clause->body_.named_expressions[0]->expression_, 0.5, 1);
}

TEST_P(CypherMainVisitorTest, ListLiteral) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN [3, [], 'johhny']"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *list_literal = dynamic_cast<ListLiteral *>(return_clause->body_.named_expressions[0]->expression_);
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN {a: 1, b: 'bla', c: [1, {a: 42}]}"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
  auto *map_literal = dynamic_cast<MapLiteral *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(map_literal);
  ASSERT_EQ(3, map_literal->elements_.size());
  ast_generator.CheckLiteral(map_literal->elements_[ast_generator.Prop("a")], 1);
  ast_generator.CheckLiteral(map_literal->elements_[ast_generator.Prop("b")], "bla");
  auto *elem_2 = dynamic_cast<ListLiteral *>(map_literal->elements_[ast_generator.Prop("c")]);
  ASSERT_TRUE(elem_2);
  EXPECT_EQ(2, elem_2->elements_.size());
  auto *elem_2_1 = dynamic_cast<MapLiteral *>(elem_2->elements_[1]);
  ASSERT_TRUE(elem_2_1);
  EXPECT_EQ(1, elem_2_1->elements_.size());
}

TEST_P(CypherMainVisitorTest, MapProjectionLiteral) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
      "WITH {name: \"Morgan\"} as actor, 85 as age RETURN actor {.name, .*, age, lastName: \"Freeman\"}"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[1]);
  auto *map_projection_literal =
      dynamic_cast<MapProjectionLiteral *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(map_projection_literal);
  ASSERT_EQ(4, map_projection_literal->elements_.size());

  ASSERT_EQ(std::string(map_projection_literal->elements_[ast_generator.Prop("name")]->GetTypeInfo().name),
            std::string("PropertyLookup"));
  ASSERT_EQ(std::string(map_projection_literal->elements_[ast_generator.Prop("*")]->GetTypeInfo().name),
            std::string("AllPropertiesLookup"));
  ASSERT_EQ(std::string(map_projection_literal->elements_[ast_generator.Prop("age")]->GetTypeInfo().name),
            std::string("Identifier"));
  ASSERT_EQ(std::string(map_projection_literal->elements_[ast_generator.Prop("lastName")]->GetTypeInfo().name),
            std::string(typeid(ast_generator).name()).ends_with("CachedAstGenerator")
                ? std::string("ParameterLookup")
                : std::string("PrimitiveLiteral"));
}

TEST_P(CypherMainVisitorTest, MapProjectionRepeatedKeySameTypeValue) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH {} as x RETURN x {a: 0, a: 1}"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[1]);
  auto *map_projection_literal =
      dynamic_cast<MapProjectionLiteral *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(map_projection_literal);
  // When multiple map properties have the same name, only one gets in
  ASSERT_EQ(1, map_projection_literal->elements_.size());
}

TEST_P(CypherMainVisitorTest, MapProjectionRepeatedKeyDifferentTypeValue) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH {a: 0} as x, 1 as a RETURN x {a: 2, .a, a}"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[1]);
  auto *map_projection_literal =
      dynamic_cast<MapProjectionLiteral *>(return_clause->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(map_projection_literal);
  // When multiple map properties have the same name, only one gets in
  ASSERT_EQ(1, map_projection_literal->elements_.size());
  // The last-given map property is the one that gets in
  ASSERT_EQ(std::string(map_projection_literal->elements_[ast_generator.Prop("a")]->GetTypeInfo().name),
            std::string("Identifier"));
}

TEST_P(CypherMainVisitorTest, NodePattern) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH (:label1:label2:label3 {a : 5, b : 10}) RETURN 1"));
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
  EXPECT_EQ(node->identifier_->name_, CypherMainVisitor::kAnonPrefix + std::to_string(1));
  EXPECT_FALSE(node->identifier_->user_declared_);
  EXPECT_THAT(node->labels_, UnorderedElementsAre(ast_generator.Label("label1"), ast_generator.Label("label2"),
                                                  ast_generator.Label("label3")));
  std::unordered_map<PropertyIx, int64_t> properties;
  for (auto x : std::get<0>(node->properties_)) {
    TypedValue value = ast_generator.LiteralValue(x.second);
    ASSERT_TRUE(value.type() == TypedValue::Type::Int);
    properties[x.first] = value.ValueInt();
  }
  EXPECT_THAT(properties, UnorderedElementsAre(Pair(ast_generator.Prop("a"), 5), Pair(ast_generator.Prop("b"), 10)));
}

TEST_P(CypherMainVisitorTest, PropertyMapSameKeyAppearsTwice) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("MATCH ({a : 1, a : 2})"), SemanticException);
}

TEST_P(CypherMainVisitorTest, NodePatternIdentifier) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH (var) RETURN 1"));
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
  EXPECT_THAT(std::get<0>(node->properties_), UnorderedElementsAre());
}

TEST_P(CypherMainVisitorTest, RelationshipPatternNoDetails) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()--() RETURN 1"));
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
      std::vector<std::string>({node1->identifier_->name_, edge->identifier_->name_, node2->identifier_->name_}),
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ((()--())) RETURN 1"));
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
      std::vector<std::string>({node1->identifier_->name_, edge->identifier_->name_, node2->identifier_->name_}),
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
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()<-[:type1|type2 {a : 5, b : 10}]-() RETURN 1"));
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
              UnorderedElementsAre(ast_generator.EdgeType("type1"), ast_generator.EdgeType("type2")));
  std::unordered_map<PropertyIx, int64_t> properties;
  for (auto x : std::get<0>(edge->properties_)) {
    TypedValue value = ast_generator.LiteralValue(x.second);
    ASSERT_TRUE(value.type() == TypedValue::Type::Int);
    properties[x.first] = value.ValueInt();
  }
  EXPECT_THAT(properties, UnorderedElementsAre(Pair(ast_generator.Prop("a"), 5), Pair(ast_generator.Prop("b"), 10)));
}

TEST_P(CypherMainVisitorTest, RelationshipPatternVariable) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[var]->() RETURN 1"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r*]->() RETURN r"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r*42..]->() RETURN r"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r*..42]->() RETURN r"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r*24..42]->() RETURN r"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r*42]->() RETURN r"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r*1...2]->() RETURN r"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r* {prop: 42}]->() RETURN r"));
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
  ast_generator.CheckLiteral(std::get<0>(edge->properties_)[ast_generator.Prop("prop")], 42);
}

TEST_P(CypherMainVisitorTest, RelationshipPatternDotsUnboundedWithEdgeTypeProperty) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r:edge_type*..{prop: 42}]->() RETURN r"));
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
  ast_generator.CheckLiteral(std::get<0>(edge->properties_)[ast_generator.Prop("prop")], 42);
  ASSERT_EQ(edge->edge_types_.size(), 1U);
  auto edge_type = ast_generator.EdgeType("edge_type");
  EXPECT_EQ(edge->edge_types_[0], edge_type);
}

TEST_P(CypherMainVisitorTest, RelationshipPatternUpperBoundedWithProperty) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r*..2{prop: 42}]->() RETURN r"));
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
  ast_generator.CheckLiteral(std::get<0>(edge->properties_)[ast_generator.Prop("prop")], 42);
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN var"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CREATE (n)"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("DELETE n, m"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("DETACH DELETE n"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("OPTIONAL MATCH (n) WHERE m RETURN 1"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("SET a.x = b, c = d, e += f, g : h : i "));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 4U);

  {
    auto *set_property = dynamic_cast<SetProperty *>(single_query->clauses_[0]);
    ASSERT_TRUE(set_property);
    ASSERT_TRUE(set_property->property_lookup_);
    auto *identifier1 = dynamic_cast<Identifier *>(set_property->property_lookup_->expression_);
    ASSERT_TRUE(identifier1);
    ASSERT_EQ(identifier1->name_, "a");
    ASSERT_EQ(set_property->property_lookup_->property_, ast_generator.Prop("x"));
    auto *identifier2 = dynamic_cast<Identifier *>(set_property->expression_);
    ASSERT_EQ(identifier2->name_, "b");
  }

  {
    auto *set_properties_assignment = dynamic_cast<SetProperties *>(single_query->clauses_[1]);
    ASSERT_TRUE(set_properties_assignment);
    ASSERT_FALSE(set_properties_assignment->update_);
    ASSERT_TRUE(set_properties_assignment->identifier_);
    ASSERT_EQ(set_properties_assignment->identifier_->name_, "c");
    auto *identifier = dynamic_cast<Identifier *>(set_properties_assignment->expression_);
    ASSERT_EQ(identifier->name_, "d");
  }

  {
    auto *set_properties_update = dynamic_cast<SetProperties *>(single_query->clauses_[2]);
    ASSERT_TRUE(set_properties_update);
    ASSERT_TRUE(set_properties_update->update_);
    ASSERT_TRUE(set_properties_update->identifier_);
    ASSERT_EQ(set_properties_update->identifier_->name_, "e");
    auto *identifier = dynamic_cast<Identifier *>(set_properties_update->expression_);
    ASSERT_EQ(identifier->name_, "f");
  }

  {
    auto *set_labels = dynamic_cast<SetLabels *>(single_query->clauses_[3]);
    ASSERT_TRUE(set_labels);
    ASSERT_TRUE(set_labels->identifier_);
    ASSERT_EQ(set_labels->identifier_->name_, "g");
    ASSERT_THAT(set_labels->labels_, UnorderedElementsAre(ast_generator.Label("h"), ast_generator.Label("i")));
  }
}

TEST_P(CypherMainVisitorTest, Remove) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("REMOVE a.x, g : h : i"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  {
    auto *remove_property = dynamic_cast<RemoveProperty *>(single_query->clauses_[0]);
    ASSERT_TRUE(remove_property);
    ASSERT_TRUE(remove_property->property_lookup_);
    auto *identifier1 = dynamic_cast<Identifier *>(remove_property->property_lookup_->expression_);
    ASSERT_TRUE(identifier1);
    ASSERT_EQ(identifier1->name_, "a");
    ASSERT_EQ(remove_property->property_lookup_->property_, ast_generator.Prop("x"));
  }
  {
    auto *remove_labels = dynamic_cast<RemoveLabels *>(single_query->clauses_[1]);
    ASSERT_TRUE(remove_labels);
    ASSERT_TRUE(remove_labels->identifier_);
    ASSERT_EQ(remove_labels->identifier_->name_, "g");
    ASSERT_THAT(remove_labels->labels_, UnorderedElementsAre(ast_generator.Label("h"), ast_generator.Label("i")));
  }
}

TEST_P(CypherMainVisitorTest, With) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH n AS m RETURN 1"));
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
  ASSERT_THROW(ast_generator.ParseQuery("WITH n.x RETURN 1"), SemanticException);
}

TEST_P(CypherMainVisitorTest, WithNonAliasedVariable) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH n RETURN 1"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH DISTINCT n AS m RETURN 1"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH n as m ORDER BY m SKIP 1 LIMIT 2 RETURN 1"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH n AS m WHERE k RETURN 1"));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("WITH 5 as anon1 MATCH () return *"));
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
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 RETURN 1"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 MATCH (n) RETURN n"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 DELETE n"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 MERGE (n)"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 WITH n AS m RETURN 1"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 1 AS n UNWIND n AS x RETURN x"), SemanticException);

  ASSERT_THROW(ast_generator.ParseQuery("OPTIONAL MATCH (n) MATCH (m) RETURN n, m"), SemanticException);
  ast_generator.ParseQuery("OPTIONAL MATCH (n) WITH n MATCH (m) RETURN n, m");
  ast_generator.ParseQuery("OPTIONAL MATCH (n) OPTIONAL MATCH (m) RETURN n, m");
  ast_generator.ParseQuery("MATCH (n) OPTIONAL MATCH (m) RETURN n, m");

  ast_generator.ParseQuery("CREATE (n)");
  ASSERT_THROW(ast_generator.ParseQuery("SET n:x MATCH (n) RETURN n"), SemanticException);
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

  ASSERT_THROW(ast_generator.ParseQuery("UNWIND [1,2,3] AS x"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE (n) UNWIND [1,2,3] AS x RETURN x"), SemanticException);
  ast_generator.ParseQuery("UNWIND [1,2,3] AS x CREATE (n) RETURN x");
  ast_generator.ParseQuery("CREATE (n) WITH n UNWIND [1,2,3] AS x RETURN x");
}

TEST_P(CypherMainVisitorTest, Merge) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MERGE (a) -[:r]- (b) ON MATCH SET a.x = b.x "
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("UNWIND [1,2,3] AS elem RETURN elem"));
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
  EXPECT_THROW(ast_generator.ParseQuery("UNWIND [1,2,3] RETURN 42"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, CreateIndex) {
  auto &ast_generator = *GetParam();
  auto *index_query = dynamic_cast<IndexQuery *>(ast_generator.ParseQuery("Create InDeX oN :mirko(slavko)"));
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::CREATE);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko")};
  EXPECT_EQ(index_query->properties_, expected_properties);
}

TEST_P(CypherMainVisitorTest, DropIndex) {
  auto &ast_generator = *GetParam();
  auto *index_query = dynamic_cast<IndexQuery *>(ast_generator.ParseQuery("dRoP InDeX oN :mirko(slavko)"));
  ASSERT_TRUE(index_query);
  EXPECT_EQ(index_query->action_, IndexQuery::Action::DROP);
  EXPECT_EQ(index_query->label_, ast_generator.Label("mirko"));
  std::vector<PropertyIx> expected_properties{ast_generator.Prop("slavko")};
  EXPECT_EQ(index_query->properties_, expected_properties);
}

TEST_P(CypherMainVisitorTest, DropIndexWithoutProperties) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("dRoP InDeX oN :mirko()"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, DropIndexWithMultipleProperties) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("dRoP InDeX oN :mirko(slavko, pero)"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, ReturnAll) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_THROW(ast_generator.ParseQuery("RETURN all(x in [1,2,3])"), SyntaxException);
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN all(x IN [1,2,3] WHERE x = 2)"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
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
}

TEST_P(CypherMainVisitorTest, ReturnSingle) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_THROW(ast_generator.ParseQuery("RETURN single(x in [1,2,3])"), SyntaxException);
  }
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN single(x IN [1,2,3] WHERE x = 2)"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(ret);
  ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
  auto *single = dynamic_cast<Single *>(ret->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(single);
  EXPECT_EQ(single->identifier_->name_, "x");
  auto *list_literal = dynamic_cast<ListLiteral *>(single->list_expression_);
  EXPECT_TRUE(list_literal);
  auto *eq = dynamic_cast<EqualOperator *>(single->where_->expression_);
  EXPECT_TRUE(eq);
}

TEST_P(CypherMainVisitorTest, ReturnReduce) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN reduce(sum = 0, x IN [1,2,3] | sum + x)"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(ret);
  ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
  auto *reduce = dynamic_cast<Reduce *>(ret->body_.named_expressions[0]->expression_);
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN extract(x IN [1,2,3] | sum + x)"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[0]);
  ASSERT_TRUE(ret);
  ASSERT_EQ(ret->body_.named_expressions.size(), 1U);
  auto *extract = dynamic_cast<Extract *>(ret->body_.named_expressions[0]->expression_);
  ASSERT_TRUE(extract);
  EXPECT_EQ(extract->identifier_->name_, "x");
  auto *list_literal = dynamic_cast<ListLiteral *>(extract->list_);
  EXPECT_TRUE(list_literal);
  auto *add = dynamic_cast<AdditionOperator *>(extract->expression_);
  EXPECT_TRUE(add);
}

TEST_P(CypherMainVisitorTest, MatchBfsReturn) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH (n) -[r:type1|type2 *bfs..10 (e, n|e.prop = 42)]-> (m) RETURN r"));
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
  EXPECT_THAT(bfs->edge_types_, UnorderedElementsAre(ast_generator.EdgeType("type1"), ast_generator.EdgeType("type2")));
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
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH () -[*]- () RETURN *"));
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
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("MATCH ()-[r:type1|type2 *wShortest 10 (we, wn | 42) total_weight "
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
              UnorderedElementsAre(ast_generator.EdgeType("type1"), ast_generator.EdgeType("type2")));
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
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH ()-[r:type1|type2 *wShortest 10 (we, wn | 42)]->() "
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
              UnorderedElementsAre(ast_generator.EdgeType("type1"), ast_generator.EdgeType("type2")));
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
  ASSERT_THROW(ast_generator.ParseQuery("MATCH ()-[r *wShortest 10.. (e, n | 42)]-() RETURN r"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("MATCH ()-[r *wShortest 10..20 (e, n | 42)]-() RETURN r"), SemanticException);
}

TEST_P(CypherMainVisitorTest, SemanticExceptionOnWShortestWithoutLambda) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("MATCH ()-[r *wShortest]-() RETURN r"), SemanticException);
}

TEST_P(CypherMainVisitorTest, SemanticExceptionOnUnionTypeMix) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 5 as X UNION ALL RETURN 6 AS X UNION RETURN 7 AS X"),
               SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 5 as X UNION RETURN 6 AS X UNION ALL RETURN 7 AS X"),
               SemanticException);
}

TEST_P(CypherMainVisitorTest, Union) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN 5 AS X, 6 AS Y UNION RETURN 6 AS X, 5 AS Y"));
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
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("RETURN 5 AS X UNION ALL RETURN 6 AS X UNION ALL RETURN 7 AS X"));
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

void check_auth_query(
    Base *ast_generator, std::string input, AuthQuery::Action action, std::string user, std::string role,
    std::string user_or_role, std::optional<TypedValue> password, std::vector<AuthQuery::Privilege> privileges,
    std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> label_privileges,
    std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges) {
  auto *auth_query = dynamic_cast<AuthQuery *>(ast_generator->ParseQuery(input));
  ASSERT_TRUE(auth_query);
  EXPECT_EQ(auth_query->action_, action);
  EXPECT_EQ(auth_query->user_, user);
  EXPECT_EQ(auth_query->role_, role);
  EXPECT_EQ(auth_query->user_or_role_, user_or_role);
  ASSERT_EQ(static_cast<bool>(auth_query->password_), static_cast<bool>(password));
  if (password) {
    ast_generator->CheckLiteral(auth_query->password_, *password);
  }
  EXPECT_EQ(auth_query->privileges_, privileges);
  EXPECT_EQ(auth_query->label_privileges_, label_privileges);
  EXPECT_EQ(auth_query->edge_type_privileges_, edge_type_privileges);
}

TEST_P(CypherMainVisitorTest, UserOrRoleName) {
  auto &ast_generator = *GetParam();
  check_auth_query(&ast_generator, "CREATE ROLE `user`", AuthQuery::Action::CREATE_ROLE, "", "user", "", {}, {}, {},
                   {});
  check_auth_query(&ast_generator, "CREATE ROLE us___er", AuthQuery::Action::CREATE_ROLE, "", "us___er", "", {}, {}, {},
                   {});
  check_auth_query(&ast_generator, "CREATE ROLE `us+er`", AuthQuery::Action::CREATE_ROLE, "", "us+er", "", {}, {}, {},
                   {});
  check_auth_query(&ast_generator, "CREATE ROLE `us|er`", AuthQuery::Action::CREATE_ROLE, "", "us|er", "", {}, {}, {},
                   {});
  check_auth_query(&ast_generator, "CREATE ROLE `us er`", AuthQuery::Action::CREATE_ROLE, "", "us er", "", {}, {}, {},
                   {});
}

TEST_P(CypherMainVisitorTest, CreateRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ROLE"), SyntaxException);
  check_auth_query(&ast_generator, "CREATE ROLE rola", AuthQuery::Action::CREATE_ROLE, "", "rola", "", {}, {}, {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("CREATE ROLE lagano rolamo"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, DropRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("DROP ROLE"), SyntaxException);
  check_auth_query(&ast_generator, "DROP ROLE rola", AuthQuery::Action::DROP_ROLE, "", "rola", "", {}, {}, {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("DROP ROLE lagano rolamo"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowRoles) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW ROLES ROLES"), SyntaxException);
  check_auth_query(&ast_generator, "SHOW ROLES", AuthQuery::Action::SHOW_ROLES, "", "", "", {}, {}, {}, {});
}

TEST_P(CypherMainVisitorTest, CreateUser) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER 123"), SyntaxException);
  check_auth_query(&ast_generator, "CREATE USER user", AuthQuery::Action::CREATE_USER, "user", "", "", {}, {}, {}, {});
  check_auth_query(&ast_generator, "CREATE USER user IDENTIFIED BY 'password'", AuthQuery::Action::CREATE_USER, "user",
                   "", "", TypedValue("password"), {}, {}, {});
  check_auth_query(&ast_generator, "CREATE USER user IDENTIFIED BY ''", AuthQuery::Action::CREATE_USER, "user", "", "",
                   TypedValue(""), {}, {}, {});
  check_auth_query(&ast_generator, "CREATE USER user IDENTIFIED BY null", AuthQuery::Action::CREATE_USER, "user", "",
                   "", TypedValue(), {}, {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("CRATE USER user IDENTIFIED BY password"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER user IDENTIFIED BY 5"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CREATE USER user IDENTIFIED BY "), SyntaxException);
}

TEST_P(CypherMainVisitorTest, SetPassword) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SET PASSWORD FOR"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET PASSWORD FOR user "), SyntaxException);
  check_auth_query(&ast_generator, "SET PASSWORD FOR user TO null", AuthQuery::Action::SET_PASSWORD, "user", "", "",
                   TypedValue(), {}, {}, {});
  check_auth_query(&ast_generator, "SET PASSWORD FOR user TO 'password'", AuthQuery::Action::SET_PASSWORD, "user", "",
                   "", TypedValue("password"), {}, {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("SET PASSWORD FOR user To 5"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, DropUser) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("DROP USER"), SyntaxException);
  check_auth_query(&ast_generator, "DROP USER user", AuthQuery::Action::DROP_USER, "user", "", "", {}, {}, {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("DROP USER lagano rolamo"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowUsers) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW USERS ROLES"), SyntaxException);
  check_auth_query(&ast_generator, "SHOW USERS", AuthQuery::Action::SHOW_USERS, "", "", "", {}, {}, {}, {});
}

TEST_P(CypherMainVisitorTest, SetRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE FOR user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("SET ROLE FOR user TO"), SyntaxException);
  check_auth_query(&ast_generator, "SET ROLE FOR user TO role", AuthQuery::Action::SET_ROLE, "user", "role", "", {}, {},
                   {}, {});
  check_auth_query(&ast_generator, "SET ROLE FOR user TO null", AuthQuery::Action::SET_ROLE, "user", "null", "", {}, {},
                   {}, {});
}

TEST_P(CypherMainVisitorTest, ClearRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CLEAR ROLE"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CLEAR ROLE user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CLEAR ROLE FOR user TO"), SyntaxException);
  check_auth_query(&ast_generator, "CLEAR ROLE FOR user", AuthQuery::Action::CLEAR_ROLE, "user", "", "", {}, {}, {},
                   {});
}

TEST_P(CypherMainVisitorTest, GrantPrivilege) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("GRANT"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT BLABLA TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT MATCH, TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("GRANT MATCH, BLABLA TO user"), SyntaxException);
  check_auth_query(&ast_generator, "GRANT MATCH TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH}, {}, {});
  check_auth_query(&ast_generator, "GRANT MATCH, AUTH TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH}, {}, {});
  // Verify that all privileges are correctly visited.
  check_auth_query(&ast_generator, "GRANT CREATE TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::CREATE}, {}, {});
  check_auth_query(&ast_generator, "GRANT DELETE TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::DELETE}, {}, {});
  check_auth_query(&ast_generator, "GRANT MERGE TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MERGE}, {}, {});
  check_auth_query(&ast_generator, "GRANT SET TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::SET}, {}, {});
  check_auth_query(&ast_generator, "GRANT REMOVE TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::REMOVE}, {}, {});
  check_auth_query(&ast_generator, "GRANT INDEX TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::INDEX}, {}, {});
  check_auth_query(&ast_generator, "GRANT STATS TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::STATS}, {}, {});
  check_auth_query(&ast_generator, "GRANT AUTH TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::AUTH}, {}, {});
  check_auth_query(&ast_generator, "GRANT CONSTRAINT TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::CONSTRAINT}, {}, {});
  check_auth_query(&ast_generator, "GRANT DUMP TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::DUMP}, {}, {});
  check_auth_query(&ast_generator, "GRANT REPLICATION TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::REPLICATION}, {}, {});
  check_auth_query(&ast_generator, "GRANT DURABILITY TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::DURABILITY}, {}, {});
  check_auth_query(&ast_generator, "GRANT READ_FILE TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::READ_FILE}, {}, {});
  check_auth_query(&ast_generator, "GRANT FREE_MEMORY TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::FREE_MEMORY}, {}, {});
  check_auth_query(&ast_generator, "GRANT TRIGGER TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::TRIGGER}, {}, {});
  check_auth_query(&ast_generator, "GRANT CONFIG TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::CONFIG}, {}, {});
  check_auth_query(&ast_generator, "GRANT STREAM TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::STREAM}, {}, {});
  check_auth_query(&ast_generator, "GRANT WEBSOCKET TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::WEBSOCKET}, {}, {});
  check_auth_query(&ast_generator, "GRANT MODULE_READ TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MODULE_READ}, {}, {});
  check_auth_query(&ast_generator, "GRANT MODULE_WRITE TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MODULE_WRITE}, {}, {});

  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> label_privileges{};
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges{};

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::READ}, {{"*"}}}});
  check_auth_query(&ast_generator, "GRANT READ ON LABELS * TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user",
                   {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::UPDATE}, {{"*"}}}});
  check_auth_query(&ast_generator, "GRANT UPDATE ON LABELS * TO user", AuthQuery::Action::GRANT_PRIVILEGE, "", "",
                   "user", {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::CREATE_DELETE}, {{"*"}}}});
  check_auth_query(&ast_generator, "GRANT CREATE_DELETE ON LABELS * TO user", AuthQuery::Action::GRANT_PRIVILEGE, "",
                   "", "user", {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::READ}, {{"Label1"}, {"Label2"}}}});
  check_auth_query(&ast_generator, "GRANT READ ON LABELS :Label1, :Label2 TO user", AuthQuery::Action::GRANT_PRIVILEGE,
                   "", "", "user", {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::UPDATE}, {{"Label1"}, {"Label2"}}}});
  check_auth_query(&ast_generator, "GRANT UPDATE ON LABELS :Label1, :Label2 TO user",
                   AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::CREATE_DELETE}, {{"Label1"}, {"Label2"}}}});
  check_auth_query(&ast_generator, "GRANT CREATE_DELETE ON LABELS :Label1, :Label2 TO user",
                   AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::READ}, {{"Label1"}, {"Label2"}}},
                              {{AuthQuery::FineGrainedPrivilege::UPDATE}, {{"Label3"}}}});
  check_auth_query(&ast_generator, "GRANT READ ON LABELS :Label1, :Label2, UPDATE ON LABELS :Label3 TO user",
                   AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::READ}, {{"Label1"}, {"Label2"}}}});
  edge_type_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::READ}, {{"Edge1"}, {"Edge2"}, {"Edge3"}}}});
  check_auth_query(&ast_generator,
                   "GRANT READ ON LABELS :Label1, :Label2, READ ON EDGE_TYPES :Edge1, :Edge2, :Edge3 TO user",
                   AuthQuery::Action::GRANT_PRIVILEGE, "", "", "user", {}, {}, label_privileges, edge_type_privileges);
  label_privileges.clear();
  edge_type_privileges.clear();
}

TEST_P(CypherMainVisitorTest, DenyPrivilege) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("DENY"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY BLABLA TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY MATCH, TO user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("DENY MATCH, BLABLA TO user"), SyntaxException);
  check_auth_query(&ast_generator, "DENY MATCH TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH}, {}, {});
  check_auth_query(&ast_generator, "DENY MATCH, AUTH TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH}, {}, {});
  // Verify that all privileges are correctly visited.
  check_auth_query(&ast_generator, "DENY CREATE TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::CREATE}, {}, {});
  check_auth_query(&ast_generator, "DENY DELETE TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::DELETE}, {}, {});
  check_auth_query(&ast_generator, "DENY MERGE TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MERGE}, {}, {});
  check_auth_query(&ast_generator, "DENY SET TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::SET}, {}, {});
  check_auth_query(&ast_generator, "DENY REMOVE TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::REMOVE}, {}, {});
  check_auth_query(&ast_generator, "DENY INDEX TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::INDEX}, {}, {});
  check_auth_query(&ast_generator, "DENY STATS TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::STATS}, {}, {});
  check_auth_query(&ast_generator, "DENY AUTH TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::AUTH}, {}, {});
  check_auth_query(&ast_generator, "DENY CONSTRAINT TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::CONSTRAINT}, {}, {});
  check_auth_query(&ast_generator, "DENY DUMP TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::DUMP}, {}, {});
  check_auth_query(&ast_generator, "DENY WEBSOCKET TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::WEBSOCKET}, {}, {});
  check_auth_query(&ast_generator, "DENY MODULE_READ TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MODULE_READ}, {}, {});
  check_auth_query(&ast_generator, "DENY MODULE_WRITE TO user", AuthQuery::Action::DENY_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MODULE_WRITE}, {}, {});
}

TEST_P(CypherMainVisitorTest, RevokePrivilege) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE FROM user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE BLABLA FROM user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE MATCH, FROM user"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("REVOKE MATCH, BLABLA FROM user"), SyntaxException);
  check_auth_query(&ast_generator, "REVOKE MATCH FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MATCH}, {}, {});
  check_auth_query(&ast_generator, "REVOKE MATCH, AUTH FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user",
                   {}, {AuthQuery::Privilege::MATCH, AuthQuery::Privilege::AUTH}, {}, {});
  check_auth_query(&ast_generator, "REVOKE ALL PRIVILEGES FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "",
                   "user", {}, kPrivilegesAll, {}, {});
  // Verify that all privileges are correctly visited.
  check_auth_query(&ast_generator, "REVOKE CREATE FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::CREATE}, {}, {});
  check_auth_query(&ast_generator, "REVOKE DELETE FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::DELETE}, {}, {});
  check_auth_query(&ast_generator, "REVOKE MERGE FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::MERGE}, {}, {});
  check_auth_query(&ast_generator, "REVOKE SET FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::SET}, {}, {});
  check_auth_query(&ast_generator, "REVOKE REMOVE FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::REMOVE}, {}, {});
  check_auth_query(&ast_generator, "REVOKE INDEX FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::INDEX}, {}, {});
  check_auth_query(&ast_generator, "REVOKE STATS FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::STATS}, {}, {});
  check_auth_query(&ast_generator, "REVOKE AUTH FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::AUTH}, {}, {});
  check_auth_query(&ast_generator, "REVOKE CONSTRAINT FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user",
                   {}, {AuthQuery::Privilege::CONSTRAINT}, {}, {});
  check_auth_query(&ast_generator, "REVOKE DUMP FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {AuthQuery::Privilege::DUMP}, {}, {});
  check_auth_query(&ast_generator, "REVOKE WEBSOCKET FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user",
                   {}, {AuthQuery::Privilege::WEBSOCKET}, {}, {});
  check_auth_query(&ast_generator, "REVOKE MODULE_READ FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user",
                   {}, {AuthQuery::Privilege::MODULE_READ}, {}, {});
  check_auth_query(&ast_generator, "REVOKE MODULE_WRITE FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user",
                   {}, {AuthQuery::Privilege::MODULE_WRITE}, {}, {});

  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> label_privileges{};
  std::vector<std::unordered_map<AuthQuery::FineGrainedPrivilege, std::vector<std::string>>> edge_type_privileges{};

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::CREATE_DELETE}, {{"*"}}}});
  check_auth_query(&ast_generator, "REVOKE LABELS * FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {},
                   {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::CREATE_DELETE}, {{"Label1"}, {"Label2"}}}});
  check_auth_query(&ast_generator, "REVOKE LABELS :Label1, :Label2 FROM user", AuthQuery::Action::REVOKE_PRIVILEGE, "",
                   "", "user", {}, {}, label_privileges, {});
  label_privileges.clear();

  label_privileges.push_back({{{AuthQuery::FineGrainedPrivilege::CREATE_DELETE}, {{"Label1"}, {"Label2"}}}});
  edge_type_privileges.push_back(
      {{{AuthQuery::FineGrainedPrivilege::CREATE_DELETE}, {{"Edge1"}, {"Edge2"}, {"Edge3"}}}});
  check_auth_query(&ast_generator, "REVOKE LABELS :Label1, :Label2, EDGE_TYPES :Edge1, :Edge2, :Edge3 FROM user",
                   AuthQuery::Action::REVOKE_PRIVILEGE, "", "", "user", {}, {}, label_privileges, edge_type_privileges);

  label_privileges.clear();
  edge_type_privileges.clear();
}

TEST_P(CypherMainVisitorTest, ShowPrivileges) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW PRIVILEGES FOR"), SyntaxException);
  check_auth_query(&ast_generator, "SHOW PRIVILEGES FOR user", AuthQuery::Action::SHOW_PRIVILEGES, "", "", "user", {},
                   {}, {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("SHOW PRIVILEGES FOR user1, user2"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowRoleForUser) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW ROLE FOR "), SyntaxException);
  check_auth_query(&ast_generator, "SHOW ROLE FOR user", AuthQuery::Action::SHOW_ROLE_FOR_USER, "user", "", "", {}, {},
                   {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("SHOW ROLE FOR user1, user2"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, ShowUsersForRole) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("SHOW USERS FOR "), SyntaxException);
  check_auth_query(&ast_generator, "SHOW USERS FOR role", AuthQuery::Action::SHOW_USERS_FOR_ROLE, "", "role", "", {},
                   {}, {}, {});
  ASSERT_THROW(ast_generator.ParseQuery("SHOW USERS FOR role1, role2"), SyntaxException);
}

void check_replication_query(Base *ast_generator, const ReplicationQuery *query, const std::string name,
                             const std::optional<TypedValue> socket_address, const ReplicationQuery::SyncMode sync_mode,
                             const std::optional<TypedValue> port = {}) {
  EXPECT_EQ(query->replica_name_, name);
  EXPECT_EQ(query->sync_mode_, sync_mode);
  ASSERT_EQ(static_cast<bool>(query->socket_address_), static_cast<bool>(socket_address));
  if (socket_address) {
    ast_generator->CheckLiteral(query->socket_address_, *socket_address);
  }
  ASSERT_EQ(static_cast<bool>(query->port_), static_cast<bool>(port));
  if (port) {
    ast_generator->CheckLiteral(query->port_, *port);
  }
}

TEST_P(CypherMainVisitorTest, TestShowReplicationMode) {
  auto &ast_generator = *GetParam();
  const std::string raw_query = "SHOW REPLICATION ROLE";
  auto *parsed_query = dynamic_cast<ReplicationQuery *>(ast_generator.ParseQuery(raw_query));
  EXPECT_EQ(parsed_query->action_, ReplicationQuery::Action::SHOW_REPLICATION_ROLE);
}

TEST_P(CypherMainVisitorTest, TestShowReplicasQuery) {
  auto &ast_generator = *GetParam();
  const std::string raw_query = "SHOW REPLICAS";
  auto *parsed_query = dynamic_cast<ReplicationQuery *>(ast_generator.ParseQuery(raw_query));
  EXPECT_EQ(parsed_query->action_, ReplicationQuery::Action::SHOW_REPLICAS);
}

TEST_P(CypherMainVisitorTest, TestSetReplicationMode) {
  auto &ast_generator = *GetParam();

  {
    const std::string query = "SET REPLICATION ROLE";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = "SET REPLICATION ROLE TO BUTTERY";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = "SET REPLICATION ROLE TO MAIN";
    auto *parsed_query = dynamic_cast<ReplicationQuery *>(ast_generator.ParseQuery(query));
    EXPECT_EQ(parsed_query->action_, ReplicationQuery::Action::SET_REPLICATION_ROLE);
    EXPECT_EQ(parsed_query->role_, ReplicationQuery::ReplicationRole::MAIN);
  }

  {
    const std::string query = "SET REPLICATION ROLE TO MAIN WITH PORT 10000";
    ASSERT_THROW(ast_generator.ParseQuery(query), SemanticException);
  }

  {
    const std::string query = "SET REPLICATION ROLE TO REPLICA WITH PORT 10000";
    auto *parsed_query = dynamic_cast<ReplicationQuery *>(ast_generator.ParseQuery(query));
    EXPECT_EQ(parsed_query->action_, ReplicationQuery::Action::SET_REPLICATION_ROLE);
    EXPECT_EQ(parsed_query->role_, ReplicationQuery::ReplicationRole::REPLICA);
    ast_generator.CheckLiteral(parsed_query->port_, TypedValue(10000));
  }
}

TEST_P(CypherMainVisitorTest, TestRegisterReplicationQuery) {
  auto &ast_generator = *GetParam();

  const std::string faulty_query = "REGISTER REPLICA TO";
  ASSERT_THROW(ast_generator.ParseQuery(faulty_query), SyntaxException);

  const std::string faulty_query_with_timeout = R"(REGISTER REPLICA replica1 SYNC WITH TIMEOUT 1.0 TO "127.0.0.1")";
  ASSERT_THROW(ast_generator.ParseQuery(faulty_query_with_timeout), SyntaxException);

  const std::string correct_query = R"(REGISTER REPLICA replica1 SYNC TO "127.0.0.1")";
  auto *correct_query_parsed = dynamic_cast<ReplicationQuery *>(ast_generator.ParseQuery(correct_query));
  check_replication_query(&ast_generator, correct_query_parsed, "replica1", TypedValue("127.0.0.1"),
                          ReplicationQuery::SyncMode::SYNC);

  std::string full_query = R"(REGISTER REPLICA replica2 SYNC TO "1.1.1.1:10000")";
  auto *full_query_parsed = dynamic_cast<ReplicationQuery *>(ast_generator.ParseQuery(full_query));
  ASSERT_TRUE(full_query_parsed);
  check_replication_query(&ast_generator, full_query_parsed, "replica2", TypedValue("1.1.1.1:10000"),
                          ReplicationQuery::SyncMode::SYNC);
}

TEST_P(CypherMainVisitorTest, TestDeleteReplica) {
  auto &ast_generator = *GetParam();

  std::string missing_name_query = "DROP REPLICA";
  ASSERT_THROW(ast_generator.ParseQuery(missing_name_query), SyntaxException);

  std::string correct_query = "DROP REPLICA replica1";
  auto *correct_query_parsed = dynamic_cast<ReplicationQuery *>(ast_generator.ParseQuery(correct_query));
  ASSERT_TRUE(correct_query_parsed);
  EXPECT_EQ(correct_query_parsed->replica_name_, "replica1");
}

TEST_P(CypherMainVisitorTest, TestExplainRegularQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_TRUE(dynamic_cast<ExplainQuery *>(ast_generator.ParseQuery("EXPLAIN RETURN n")));
}

TEST_P(CypherMainVisitorTest, TestExplainExplainQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("EXPLAIN EXPLAIN RETURN n"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestExplainAuthQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("EXPLAIN SHOW ROLES"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestProfileRegularQuery) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_TRUE(dynamic_cast<ProfileQuery *>(ast_generator.ParseQuery("PROFILE RETURN n")));
  }
}

TEST_P(CypherMainVisitorTest, TestProfileComplicatedQuery) {
  {
    auto &ast_generator = *GetParam();
    EXPECT_TRUE(
        dynamic_cast<ProfileQuery *>(ast_generator.ParseQuery("profile optional match (n) where n.hello = 5 "
                                                              "return n union optional match (n) where n.there = 10 "
                                                              "return n")));
  }
}

TEST_P(CypherMainVisitorTest, TestProfileProfileQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("PROFILE PROFILE RETURN n"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestProfileAuthQuery) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("PROFILE SHOW ROLES"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestShowStorageInfo) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<InfoQuery *>(ast_generator.ParseQuery("SHOW STORAGE INFO"));
  ASSERT_TRUE(query);
  EXPECT_EQ(query->info_type_, InfoQuery::InfoType::STORAGE);
}

TEST_P(CypherMainVisitorTest, TestShowIndexInfo) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<InfoQuery *>(ast_generator.ParseQuery("SHOW INDEX INFO"));
  ASSERT_TRUE(query);
  EXPECT_EQ(query->info_type_, InfoQuery::InfoType::INDEX);
}

TEST_P(CypherMainVisitorTest, TestShowConstraintInfo) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<InfoQuery *>(ast_generator.ParseQuery("SHOW CONSTRAINT INFO"));
  ASSERT_TRUE(query);
  EXPECT_EQ(query->info_type_, InfoQuery::InfoType::CONSTRAINT);
}

TEST_P(CypherMainVisitorTest, CreateConstraintSyntaxError) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (:label) ASSERT EXISTS"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT () ASSERT EXISTS"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON () ASSERT EXISTS(prop1)"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON () ASSERT EXISTS (prop1, prop2)"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "EXISTS (n.prop1, missing.prop2)"),
               SemanticException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "EXISTS (m.prop1, m.prop2)"),
               SemanticException);

  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (:label) ASSERT IS UNIQUE"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT () ASSERT IS UNIQUE"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON () ASSERT prop1 IS UNIQUE"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON () ASSERT prop1, prop2 IS UNIQUE"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "n.prop1, missing.prop2 IS UNIQUE"),
               SemanticException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                        "m.prop1, m.prop2 IS UNIQUE"),
               SemanticException);

  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON (:label) ASSERT IS NODE KEY"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT () ASSERT IS NODE KEY"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON () ASSERT (prop1) IS NODE KEY"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("CREATE CONSTRAINT ON () ASSERT (prop1, prop2) IS NODE KEY"), SyntaxException);
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
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT EXISTS(n.prop1)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties, UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT EXISTS (n.prop1, n.prop2)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"), ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT n.prop1 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties, UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT n.prop1, n.prop2 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"), ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT (n.prop1) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties, UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query =
        dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery("CREATE CONSTRAINT ON (n:label) ASSERT "
                                                                 "(n.prop1, n.prop2) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::CREATE);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"), ast_generator.Prop("prop2")));
  }
}

TEST_P(CypherMainVisitorTest, DropConstraint) {
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("DROP CONSTRAINT ON (n:label) ASSERT EXISTS(n.prop1)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties, UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("DROP CONSTRAINT ON (n:label) ASSERT EXISTS(n.prop1, n.prop2)"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::EXISTS);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"), ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("DROP CONSTRAINT ON (n:label) ASSERT n.prop1 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties, UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("DROP CONSTRAINT ON (n:label) ASSERT n.prop1, n.prop2 IS UNIQUE"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::UNIQUE);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"), ast_generator.Prop("prop2")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<ConstraintQuery *>(
        ast_generator.ParseQuery("DROP CONSTRAINT ON (n:label) ASSERT (n.prop1) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties, UnorderedElementsAre(ast_generator.Prop("prop1")));
  }
  {
    auto &ast_generator = *GetParam();
    auto *query =
        dynamic_cast<ConstraintQuery *>(ast_generator.ParseQuery("DROP CONSTRAINT ON (n:label) ASSERT "
                                                                 "(n.prop1, n.prop2) IS NODE KEY"));
    ASSERT_TRUE(query);
    EXPECT_EQ(query->action_type_, ConstraintQuery::ActionType::DROP);
    EXPECT_EQ(query->constraint_.type, Constraint::Type::NODE_KEY);
    EXPECT_EQ(query->constraint_.label, ast_generator.Label("label"));
    EXPECT_THAT(query->constraint_.properties,
                UnorderedElementsAre(ast_generator.Prop("prop1"), ast_generator.Prop("prop2")));
  }
}

TEST_P(CypherMainVisitorTest, RegexMatch) {
  {
    auto &ast_generator = *GetParam();
    auto *query =
        dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH (n) WHERE n.name =~ \".*bla.*\" RETURN n.name"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 2U);
    auto *match_clause = dynamic_cast<Match *>(single_query->clauses_[0]);
    ASSERT_TRUE(match_clause);
    auto *regex_match = dynamic_cast<RegexMatch *>(match_clause->where_->expression_);
    ASSERT_TRUE(regex_match);
    ASSERT_TRUE(dynamic_cast<PropertyLookup *>(regex_match->string_expr_));
    ast_generator.CheckLiteral(regex_match->regex_, ".*bla.*");
  }
  {
    auto &ast_generator = *GetParam();
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN \"text\" =~ \".*bla.*\""));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *return_clause = dynamic_cast<Return *>(single_query->clauses_[0]);
    ASSERT_TRUE(return_clause);
    ASSERT_EQ(return_clause->body_.named_expressions.size(), 1U);
    auto *named_expression = return_clause->body_.named_expressions[0];
    auto *regex_match = dynamic_cast<RegexMatch *>(named_expression->expression_);
    ASSERT_TRUE(regex_match);
    ast_generator.CheckLiteral(regex_match->string_expr_, "text");
    ast_generator.CheckLiteral(regex_match->regex_, ".*bla.*");
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_P(CypherMainVisitorTest, DumpDatabase) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<DumpQuery *>(ast_generator.ParseQuery("DUMP DATABASE"));
  ASSERT_TRUE(query);
}

namespace {
template <class TAst>
void CheckCallProcedureDefaultMemoryLimit(const TAst &ast, const CallProcedure &call_proc) {
  // Should be 100 MB
  auto *literal = dynamic_cast<PrimitiveLiteral *>(call_proc.memory_limit_);
  ASSERT_TRUE(literal);
  TypedValue value(literal->value_);
  ASSERT_TRUE(TypedValue::BoolEqual{}(value, TypedValue(100)));
  ASSERT_EQ(call_proc.memory_scale_, 1024 * 1024);
}
}  // namespace

TEST_P(CypherMainVisitorTest, CallProcedureWithDotsInName) {
  AddProc(*mock_module_with_dots_in_name, "proc", {}, {"res"}, ProcedureType::WRITE);
  auto &ast_generator = *GetParam();

  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mock_module.with.dots.in.name.proc() YIELD res"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mock_module.with.dots.in.name.proc");
  ASSERT_TRUE(call_proc->arguments_.empty());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  std::vector<std::string> expected_names{"res"};
  ASSERT_EQ(identifier_names, expected_names);
  ASSERT_EQ(identifier_names, call_proc->result_fields_);
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithDashesInName) {
  AddProc(*mock_module, "proc-with-dashes", {}, {"res"}, ProcedureType::READ);
  auto &ast_generator = *GetParam();

  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL `mock_module.proc-with-dashes`() YIELD res"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mock_module.proc-with-dashes");
  ASSERT_TRUE(call_proc->arguments_.empty());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  std::vector<std::string> expected_names{"res"};
  ASSERT_EQ(identifier_names, expected_names);
  ASSERT_EQ(identifier_names, call_proc->result_fields_);
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithYieldSomeFields) {
  auto &ast_generator = *GetParam();
  auto check_proc = [this, &ast_generator](const ProcedureType type) {
    const auto proc_name = std::string{"proc_"} + ToString(type);
    SCOPED_TRACE(proc_name);
    const auto fully_qualified_proc_name = std::string{"mock_module."} + proc_name;
    AddProc(*mock_module, proc_name.c_str(), {}, {"fst", "field-with-dashes", "last_field"}, type);
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
        fmt::format("CALL {}() YIELD fst, `field-with-dashes`, last_field", fully_qualified_proc_name)));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
    ASSERT_TRUE(call_proc);
    ASSERT_EQ(call_proc->is_write_, type == ProcedureType::WRITE);
    ASSERT_EQ(call_proc->procedure_name_, fully_qualified_proc_name);
    ASSERT_TRUE(call_proc->arguments_.empty());
    ASSERT_EQ(call_proc->result_fields_.size(), 3U);
    ASSERT_EQ(call_proc->result_identifiers_.size(), call_proc->result_fields_.size());
    std::vector<std::string> identifier_names;
    identifier_names.reserve(call_proc->result_identifiers_.size());
    for (const auto *identifier : call_proc->result_identifiers_) {
      ASSERT_TRUE(identifier->user_declared_);
      identifier_names.push_back(identifier->name_);
    }
    std::vector<std::string> expected_names{"fst", "field-with-dashes", "last_field"};
    ASSERT_EQ(identifier_names, expected_names);
    ASSERT_EQ(identifier_names, call_proc->result_fields_);
    CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
  };
  check_proc(ProcedureType::READ);
  check_proc(ProcedureType::WRITE);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithYieldAliasedFields) {
  AddProc(*mock_module, "proc", {}, {"fst", "snd", "thrd"}, ProcedureType::READ);
  auto &ast_generator = *GetParam();

  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mock_module.proc() YIELD fst AS res1, snd AS "
                                                           "`result-with-dashes`, thrd AS last_result"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mock_module.proc");
  ASSERT_TRUE(call_proc->arguments_.empty());
  ASSERT_EQ(call_proc->result_fields_.size(), 3U);
  ASSERT_EQ(call_proc->result_identifiers_.size(), call_proc->result_fields_.size());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  std::vector<std::string> aliased_names{"res1", "result-with-dashes", "last_result"};
  ASSERT_EQ(identifier_names, aliased_names);
  std::vector<std::string> field_names{"fst", "snd", "thrd"};
  ASSERT_EQ(call_proc->result_fields_, field_names);
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithArguments) {
  AddProc(*mock_module, "proc", {"arg1", "arg2", "arg3"}, {"res"}, ProcedureType::READ);
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mock_module.proc(0, 1, 2) YIELD res"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mock_module.proc");
  ASSERT_EQ(call_proc->arguments_.size(), 3U);
  for (int64_t i = 0; i < 3; ++i) {
    ast_generator.CheckLiteral(call_proc->arguments_[i], i);
  }
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  std::vector<std::string> expected_names{"res"};
  ASSERT_EQ(identifier_names, expected_names);
  ASSERT_EQ(identifier_names, call_proc->result_fields_);
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
}

TEST_P(CypherMainVisitorTest, CallProcedureYieldAsterisk) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.procedures() YIELD *"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mg.procedures");
  ASSERT_TRUE(call_proc->arguments_.empty());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  ASSERT_THAT(identifier_names, UnorderedElementsAre("name", "signature", "is_write", "path", "is_editable"));
  ASSERT_EQ(identifier_names, call_proc->result_fields_);
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
}

TEST_P(CypherMainVisitorTest, CallProcedureYieldAsteriskReturnAsterisk) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.procedures() YIELD * RETURN *"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 2U);
  auto *ret = dynamic_cast<Return *>(single_query->clauses_[1]);
  ASSERT_TRUE(ret);
  ASSERT_TRUE(ret->body_.all_identifiers);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mg.procedures");
  ASSERT_TRUE(call_proc->arguments_.empty());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  ASSERT_THAT(identifier_names, UnorderedElementsAre("name", "signature", "is_write", "path", "is_editable"));
  ASSERT_EQ(identifier_names, call_proc->result_fields_);
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithoutYield) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.load_all()"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mg.load_all");
  ASSERT_TRUE(call_proc->arguments_.empty());
  ASSERT_TRUE(call_proc->result_fields_.empty());
  ASSERT_TRUE(call_proc->result_identifiers_.empty());
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithMemoryLimitWithoutYield) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.load_all() PROCEDURE MEMORY LIMIT 32 KB"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mg.load_all");
  ASSERT_TRUE(call_proc->arguments_.empty());
  ASSERT_TRUE(call_proc->result_fields_.empty());
  ASSERT_TRUE(call_proc->result_identifiers_.empty());
  ast_generator.CheckLiteral(call_proc->memory_limit_, 32);
  ASSERT_EQ(call_proc->memory_scale_, 1024);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithMemoryUnlimitedWithoutYield) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.load_all() PROCEDURE MEMORY UNLIMITED"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mg.load_all");
  ASSERT_TRUE(call_proc->arguments_.empty());
  ASSERT_TRUE(call_proc->result_fields_.empty());
  ASSERT_TRUE(call_proc->result_identifiers_.empty());
  ASSERT_FALSE(call_proc->memory_limit_);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithMemoryLimit) {
  auto &ast_generator = *GetParam();
  auto *query = dynamic_cast<CypherQuery *>(
      ast_generator.ParseQuery("CALL mg.load_all() PROCEDURE MEMORY LIMIT 32 MB YIELD res"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mg.load_all");
  ASSERT_TRUE(call_proc->arguments_.empty());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  std::vector<std::string> expected_names{"res"};
  ASSERT_EQ(identifier_names, expected_names);
  ASSERT_EQ(identifier_names, call_proc->result_fields_);
  ast_generator.CheckLiteral(call_proc->memory_limit_, 32);
  ASSERT_EQ(call_proc->memory_scale_, 1024 * 1024);
}

TEST_P(CypherMainVisitorTest, CallProcedureWithMemoryUnlimited) {
  auto &ast_generator = *GetParam();
  auto *query =
      dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.load_all() PROCEDURE MEMORY UNLIMITED YIELD res"));
  ASSERT_TRUE(query);
  ASSERT_TRUE(query->single_query_);
  auto *single_query = query->single_query_;
  ASSERT_EQ(single_query->clauses_.size(), 1U);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
  ASSERT_TRUE(call_proc);
  ASSERT_EQ(call_proc->procedure_name_, "mg.load_all");
  ASSERT_TRUE(call_proc->arguments_.empty());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    ASSERT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  std::vector<std::string> expected_names{"res"};
  ASSERT_EQ(identifier_names, expected_names);
  ASSERT_EQ(identifier_names, call_proc->result_fields_);
  ASSERT_FALSE(call_proc->memory_limit_);
}

namespace {
template <typename TException = SyntaxException>
void TestInvalidQuery(const auto &query, Base &ast_generator) {
  SCOPED_TRACE(query);
  EXPECT_THROW(ast_generator.ParseQuery(query), TException) << query;
}

template <typename TException = SyntaxException>
void TestInvalidQueryWithMessage(const auto &query, Base &ast_generator, const std::string_view message) {
  bool exception_is_thrown = false;
  try {
    ast_generator.ParseQuery(query);
  } catch (const TException &se) {
    EXPECT_EQ(std::string_view{se.what()}, message);
    exception_is_thrown = true;
  } catch (...) {
    FAIL() << "Unexpected exception";
  }
  EXPECT_TRUE(exception_is_thrown);
}

void CheckParsedCallProcedure(const CypherQuery &query, Base &ast_generator,
                              const std::string_view fully_qualified_proc_name,
                              const std::vector<std::string_view> &args, const ProcedureType type,
                              const size_t clauses_size, const size_t call_procedure_index) {
  ASSERT_NE(query.single_query_, nullptr);
  auto *single_query = query.single_query_;
  EXPECT_EQ(single_query->clauses_.size(), clauses_size);
  ASSERT_FALSE(single_query->clauses_.empty());
  ASSERT_LT(call_procedure_index, clauses_size);
  auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[call_procedure_index]);
  ASSERT_NE(call_proc, nullptr);
  EXPECT_EQ(call_proc->procedure_name_, fully_qualified_proc_name);
  EXPECT_TRUE(call_proc->arguments_.empty());
  EXPECT_EQ(call_proc->result_fields_.size(), 2U);
  EXPECT_EQ(call_proc->result_identifiers_.size(), call_proc->result_fields_.size());
  std::vector<std::string> identifier_names;
  identifier_names.reserve(call_proc->result_identifiers_.size());
  for (const auto *identifier : call_proc->result_identifiers_) {
    EXPECT_TRUE(identifier->user_declared_);
    identifier_names.push_back(identifier->name_);
  }
  std::vector<std::string> args_as_str{};
  std::transform(args.begin(), args.end(), std::back_inserter(args_as_str),
                 [](const std::string_view arg) { return std::string{arg}; });
  EXPECT_EQ(identifier_names, args_as_str);
  EXPECT_EQ(identifier_names, call_proc->result_fields_);
  ASSERT_EQ(call_proc->is_write_, type == ProcedureType::WRITE);
  CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
};
}  // namespace

TEST_P(CypherMainVisitorTest, CallProcedureMultipleQueryPartsAfter) {
  auto &ast_generator = *GetParam();
  static constexpr std::string_view fst{"fst"};
  static constexpr std::string_view snd{"snd"};
  const std::vector args{fst, snd};

  const auto read_proc = CreateProcByType(ProcedureType::READ, args);
  const auto write_proc = CreateProcByType(ProcedureType::WRITE, args);

  const auto check_parsed_call_proc = [&ast_generator, &args](const CypherQuery &query,
                                                              const std::string_view fully_qualified_proc_name,
                                                              const ProcedureType type, const size_t clause_size) {
    CheckParsedCallProcedure(query, ast_generator, fully_qualified_proc_name, args, type, clause_size, 0);
  };
  {
    SCOPED_TRACE("Read query part");
    {
      SCOPED_TRACE("With WITH");
      static constexpr std::string_view kQueryWithWith{"CALL {}() YIELD {},{} WITH {},{} UNWIND {} as u RETURN u"};
      static constexpr size_t kQueryParts{4};
      {
        SCOPED_TRACE("Write proc");
        const auto query_str = fmt::format(kQueryWithWith, write_proc, fst, snd, fst, snd, fst);
        TestInvalidQueryWithMessage<SemanticException>(
            query_str, ast_generator,
            "WITH can't be put after calling a writeable procedure, only RETURN clause can be put after.");
      }
      {
        SCOPED_TRACE("Read proc");
        const auto query_str = fmt::format(kQueryWithWith, read_proc, fst, snd, fst, snd, fst);
        const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
        ASSERT_NE(query, nullptr);
        check_parsed_call_proc(*query, read_proc, ProcedureType::READ, kQueryParts);
      }
    }
    {
      SCOPED_TRACE("Without WITH");
      static constexpr std::string_view kQueryWithoutWith{"CALL {}() YIELD {},{} UNWIND {} as u RETURN u"};
      static constexpr size_t kQueryParts{3};
      {
        SCOPED_TRACE("Write proc");
        const auto query_str = fmt::format(kQueryWithoutWith, write_proc, fst, snd, fst);
        TestInvalidQueryWithMessage<SemanticException>(
            query_str, ast_generator,
            "UNWIND can't be put after calling a writeable procedure, only RETURN clause can be put after.");
      }
      {
        SCOPED_TRACE("Read proc");
        const auto query_str = fmt::format(kQueryWithoutWith, read_proc, fst, snd, fst);
        const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
        ASSERT_NE(query, nullptr);
        check_parsed_call_proc(*query, read_proc, ProcedureType::READ, kQueryParts);
      }
    }
  }
  {
    SCOPED_TRACE("Write query part");
    {
      SCOPED_TRACE("With WITH");
      static constexpr std::string_view kQueryWithWith{
          "CALL {}() YIELD {},{} WITH {},{} CREATE(n {{prop : {}}}) RETURN n"};
      static constexpr size_t kQueryParts{4};
      {
        SCOPED_TRACE("Write proc");
        const auto query_str = fmt::format(kQueryWithWith, write_proc, fst, snd, fst, snd, fst);
        TestInvalidQueryWithMessage<SemanticException>(
            query_str, ast_generator,
            "WITH can't be put after calling a writeable procedure, only RETURN clause can be put after.");
      }
      {
        SCOPED_TRACE("Read proc");
        const auto query_str = fmt::format(kQueryWithWith, read_proc, fst, snd, fst, snd, fst);
        const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
        ASSERT_NE(query, nullptr);
        check_parsed_call_proc(*query, read_proc, ProcedureType::READ, kQueryParts);
      }
    }
    {
      SCOPED_TRACE("Without WITH");
      static constexpr std::string_view kQueryWithoutWith{"CALL {}() YIELD {},{} CREATE(n {{prop : {}}}) RETURN n"};
      static constexpr size_t kQueryParts{3};
      {
        SCOPED_TRACE("Write proc");
        const auto query_str = fmt::format(kQueryWithoutWith, write_proc, fst, snd, fst);
        TestInvalidQueryWithMessage<SemanticException>(
            query_str, ast_generator,
            "Update clause can't be put after calling a writeable procedure, only RETURN clause can be put after.");
      }
      {
        SCOPED_TRACE("Read proc");
        const auto query_str = fmt::format(kQueryWithoutWith, read_proc, fst, snd, fst, snd, fst);
        const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
        ASSERT_NE(query, nullptr);
        check_parsed_call_proc(*query, read_proc, ProcedureType::READ, kQueryParts);
      }
    }
  }
}

TEST_P(CypherMainVisitorTest, CallProcedureMultipleQueryPartsBefore) {
  auto &ast_generator = *GetParam();
  static constexpr std::string_view fst{"fst"};
  static constexpr std::string_view snd{"snd"};
  const std::vector args{fst, snd};

  const auto read_proc = CreateProcByType(ProcedureType::READ, args);
  const auto write_proc = CreateProcByType(ProcedureType::WRITE, args);

  const auto check_parsed_call_proc = [&ast_generator, &args](const CypherQuery &query,
                                                              const std::string_view fully_qualified_proc_name,
                                                              const ProcedureType type, const size_t clause_size) {
    CheckParsedCallProcedure(query, ast_generator, fully_qualified_proc_name, args, type, clause_size, clause_size - 2);
  };
  {
    SCOPED_TRACE("Read query part");
    static constexpr std::string_view kQueryWithReadQueryPart{"MATCH (n) CALL {}() YIELD * RETURN *"};
    static constexpr size_t kQueryParts{3};
    {
      SCOPED_TRACE("Write proc");
      const auto query_str = fmt::format(kQueryWithReadQueryPart, write_proc);
      const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
      ASSERT_NE(query, nullptr);
      check_parsed_call_proc(*query, write_proc, ProcedureType::WRITE, kQueryParts);
    }
    {
      SCOPED_TRACE("Read proc");
      const auto query_str = fmt::format(kQueryWithReadQueryPart, read_proc);
      const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
      ASSERT_NE(query, nullptr);
      check_parsed_call_proc(*query, read_proc, ProcedureType::READ, kQueryParts);
    }
  }
  {
    SCOPED_TRACE("Write query part");
    static constexpr std::string_view kQueryWithWriteQueryPart{"CREATE (n) WITH n CALL {}() YIELD * RETURN *"};
    static constexpr size_t kQueryParts{4};
    {
      SCOPED_TRACE("Write proc");
      const auto query_str = fmt::format(kQueryWithWriteQueryPart, write_proc, fst, snd, fst);
      TestInvalidQueryWithMessage<SemanticException>(
          query_str, ast_generator, "Write procedures cannot be used in queries that contains any update clauses!");
    }
    {
      SCOPED_TRACE("Read proc");
      const auto query_str = fmt::format(kQueryWithWriteQueryPart, read_proc, fst, snd, fst, snd, fst);
      const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
      ASSERT_NE(query, nullptr);
      check_parsed_call_proc(*query, read_proc, ProcedureType::READ, kQueryParts);
    }
  }
}

TEST_P(CypherMainVisitorTest, CallProcedureMultipleProcedures) {
  auto &ast_generator = *GetParam();
  static constexpr std::string_view fst{"fst"};
  static constexpr std::string_view snd{"snd"};
  const std::vector args{fst, snd};

  const auto read_proc = CreateProcByType(ProcedureType::READ, args);
  const auto write_proc = CreateProcByType(ProcedureType::WRITE, args);

  {
    SCOPED_TRACE("Read then write");
    const auto query_str = fmt::format("CALL {}() YIELD * CALL {}() YIELD * RETURN *", read_proc, write_proc);
    static constexpr size_t kQueryParts{3};
    const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query_str));
    ASSERT_NE(query, nullptr);

    CheckParsedCallProcedure(*query, ast_generator, read_proc, args, ProcedureType::READ, kQueryParts, 0);
    CheckParsedCallProcedure(*query, ast_generator, write_proc, args, ProcedureType::WRITE, kQueryParts, 1);
  }
  {
    SCOPED_TRACE("Write then read");
    const auto query_str = fmt::format("CALL {}() YIELD * CALL {}() YIELD * RETURN *", write_proc, read_proc);
    TestInvalidQueryWithMessage<SemanticException>(
        query_str, ast_generator,
        "CALL can't be put after calling a writeable procedure, only RETURN clause can be put after.");
  }
  {
    SCOPED_TRACE("Write twice");
    const auto query_str = fmt::format("CALL {}() YIELD * CALL {}() YIELD * RETURN *", write_proc, write_proc);
    TestInvalidQueryWithMessage<SemanticException>(
        query_str, ast_generator,
        "CALL can't be put after calling a writeable procedure, only RETURN clause can be put after.");
  }
}

TEST_P(CypherMainVisitorTest, IncorrectCallProcedure) {
  auto &ast_generator = *GetParam();
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc-with-dashes()"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc() yield field-with-dashes"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc() yield field.with.dots"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc() yield res AS result-with-dashes"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc() yield res AS result.with.dots"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("WITH 42 AS x CALL not_standalone(x)"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL procedure() YIELD"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 42, CALL procedure() YIELD"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 42, CALL procedure() YIELD res"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN 42 AS x CALL procedure() YIELD res"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc.with.dots() MEMORY YIELD res"), SyntaxException);
  // mg.procedures returns something, so it needs to have a YIELD.
  ASSERT_THROW(ast_generator.ParseQuery("CALL mg.procedures()"), SemanticException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL mg.procedures() PROCEDURE MEMORY UNLIMITED"), SemanticException);
  // TODO: Implement support for the following syntax. These are defined in
  // Neo4j and accepted in openCypher CIP.
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc RETURN 42"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc() YIELD res WHERE res > 42"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("CALL proc() YIELD res WHERE res > 42 RETURN *"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, TestLockPathQuery) {
  auto &ast_generator = *GetParam();

  const auto test_lock_path_query = [&](const std::string_view command, const LockPathQuery::Action action) {
    ASSERT_THROW(ast_generator.ParseQuery(command.data()), SyntaxException);

    {
      const std::string query = fmt::format("{} ME", command);
      ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
    }

    {
      const std::string query = fmt::format("{} DATA", command);
      ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
    }

    {
      const std::string query = fmt::format("{} DATA STUFF", command);
      ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
    }

    {
      const std::string query = fmt::format("{} DATA DIRECTORY", command);
      auto *parsed_query = dynamic_cast<LockPathQuery *>(ast_generator.ParseQuery(query));
      ASSERT_TRUE(parsed_query);
      EXPECT_EQ(parsed_query->action_, action);
    }

    {
      const std::string query = fmt::format("{} DATA DIRECTORY LOCK STATUS", command);
      ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
    }

    {
      const std::string query = fmt::format("{} DATA DIRECTORY STATUS", command);
      ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
    }
  };

  test_lock_path_query("LOCK", LockPathQuery::Action::LOCK_PATH);
  test_lock_path_query("UNLOCK", LockPathQuery::Action::UNLOCK_PATH);

  // Status test
  {
    const std::string query = "DATA DIRECTORY LOCK";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = "DATA LOCK STATUS";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = "DIRECTORY LOCK STATUS";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = "DATA DIRECTORY LOCK STATUS";
    auto *parsed_query = dynamic_cast<LockPathQuery *>(ast_generator.ParseQuery(query));
    ASSERT_TRUE(parsed_query);
    EXPECT_EQ(parsed_query->action_, LockPathQuery::Action::STATUS);
  }
}

TEST_P(CypherMainVisitorTest, TestLoadCsvClause) {
  auto &ast_generator = *GetParam();

  {
    const std::string query = R"(LOAD CSV FROM "file.csv")";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH)";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH HEADER)";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH HEADER DELIMITER ";")";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH HEADER DELIMITER ";" QUOTE "'")";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH HEADER DELIMITER ";" QUOTE "'" AS)";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = R"(LOAD CSV FROM file WITH HEADER IGNORE BAD DELIMITER ";" QUOTE "'" AS x)";
    ASSERT_THROW(ast_generator.ParseQuery(query), SyntaxException);
  }

  {
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH HEADER IGNORE BAD DELIMITER 0 QUOTE "'" AS x)";
    ASSERT_THROW(ast_generator.ParseQuery(query), SemanticException);
  }

  {
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH HEADER IGNORE BAD DELIMITER ";" QUOTE 0 AS x)";
    ASSERT_THROW(ast_generator.ParseQuery(query), SemanticException);
  }

  {
    // can't be a standalone clause
    const std::string query = R"(LOAD CSV FROM "file.csv" WITH HEADER IGNORE BAD DELIMITER ";" QUOTE "'" AS x)";
    ASSERT_THROW(ast_generator.ParseQuery(query), SemanticException);
  }

  {
    const std::string query =
        R"(LOAD CSV FROM "file.csv" WITH HEADER IGNORE BAD DELIMITER ";" QUOTE "'" AS x RETURN x)";
    auto *parsed_query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(query));
    ASSERT_TRUE(parsed_query);
    auto *load_csv_clause = dynamic_cast<LoadCsv *>(parsed_query->single_query_->clauses_[0]);
    ASSERT_TRUE(load_csv_clause);
    ASSERT_TRUE(load_csv_clause->with_header_);
    ASSERT_TRUE(load_csv_clause->ignore_bad_);
  }
}

TEST_P(CypherMainVisitorTest, MemoryLimit) {
  auto &ast_generator = *GetParam();

  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUE"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUERY"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUERY MEM"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUERY MEMORY"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUERY MEMORY LIM"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUERY MEMORY LIMIT"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUERY MEMORY LIMIT KB"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("RETURN x QUERY MEMORY LIMIT 12GB"), SyntaxException);
  ASSERT_THROW(ast_generator.ParseQuery("QUERY MEMORY LIMIT 12KB RETURN x"), SyntaxException);

  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x"));
    ASSERT_TRUE(query);
    ASSERT_FALSE(query->memory_limit_);
  }

  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x QUERY MEMORY LIMIT 12KB"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->memory_limit_);
    ast_generator.CheckLiteral(query->memory_limit_, 12);
    ASSERT_EQ(query->memory_scale_, 1024U);
  }

  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("RETURN x QUERY MEMORY LIMIT 12MB"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->memory_limit_);
    ast_generator.CheckLiteral(query->memory_limit_, 12);
    ASSERT_EQ(query->memory_scale_, 1024U * 1024U);
  }

  {
    auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("CALL mg.procedures() YIELD x RETURN x QUERY MEMORY LIMIT 12MB"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->memory_limit_);
    ast_generator.CheckLiteral(query->memory_limit_, 12);
    ASSERT_EQ(query->memory_scale_, 1024U * 1024U);

    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 2U);
    auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
    CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
  }

  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
        "CALL mg.procedures() PROCEDURE MEMORY LIMIT 3KB YIELD x RETURN x QUERY MEMORY LIMIT 12MB"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->memory_limit_);
    ast_generator.CheckLiteral(query->memory_limit_, 12);
    ASSERT_EQ(query->memory_scale_, 1024U * 1024U);

    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 2U);
    auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
    ASSERT_TRUE(call_proc->memory_limit_);
    ast_generator.CheckLiteral(call_proc->memory_limit_, 3);
    ASSERT_EQ(call_proc->memory_scale_, 1024U);
  }

  {
    auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("CALL mg.procedures() PROCEDURE MEMORY LIMIT 3KB YIELD x RETURN x"));
    ASSERT_TRUE(query);
    ASSERT_FALSE(query->memory_limit_);

    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 2U);
    auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
    ASSERT_TRUE(call_proc->memory_limit_);
    ast_generator.CheckLiteral(call_proc->memory_limit_, 3);
    ASSERT_EQ(call_proc->memory_scale_, 1024U);
  }

  {
    auto *query =
        dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.load_all() PROCEDURE MEMORY LIMIT 3KB"));
    ASSERT_TRUE(query);
    ASSERT_FALSE(query->memory_limit_);

    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
    ASSERT_TRUE(call_proc->memory_limit_);
    ast_generator.CheckLiteral(call_proc->memory_limit_, 3);
    ASSERT_EQ(call_proc->memory_scale_, 1024U);
  }

  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("CALL mg.load_all() QUERY MEMORY LIMIT 3KB"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->memory_limit_);
    ast_generator.CheckLiteral(query->memory_limit_, 3);
    ASSERT_EQ(query->memory_scale_, 1024U);

    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *call_proc = dynamic_cast<CallProcedure *>(single_query->clauses_[0]);
    CheckCallProcedureDefaultMemoryLimit(ast_generator, *call_proc);
  }
}

TEST_P(CypherMainVisitorTest, DropTrigger) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("DROP TR", ast_generator);
  TestInvalidQuery("DROP TRIGGER", ast_generator);

  auto *parsed_query = dynamic_cast<TriggerQuery *>(ast_generator.ParseQuery("DROP TRIGGER trigger"));
  EXPECT_EQ(parsed_query->action_, TriggerQuery::Action::DROP_TRIGGER);
  EXPECT_EQ(parsed_query->trigger_name_, "trigger");
}

TEST_P(CypherMainVisitorTest, ShowTriggers) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("SHOW TR", ast_generator);
  TestInvalidQuery("SHOW TRIGGER", ast_generator);

  auto *parsed_query = dynamic_cast<TriggerQuery *>(ast_generator.ParseQuery("SHOW TRIGGERS"));
  EXPECT_EQ(parsed_query->action_, TriggerQuery::Action::SHOW_TRIGGERS);
}

namespace {
void ValidateCreateQuery(Base &ast_generator, const auto &query, const auto &trigger_name,
                         const memgraph::query::TriggerQuery::EventType event_type, const auto &phase,
                         const auto &statement) {
  auto *parsed_query = dynamic_cast<TriggerQuery *>(ast_generator.ParseQuery(query));
  EXPECT_EQ(parsed_query->action_, TriggerQuery::Action::CREATE_TRIGGER);
  EXPECT_EQ(parsed_query->trigger_name_, trigger_name);
  EXPECT_EQ(parsed_query->event_type_, event_type);
  EXPECT_EQ(parsed_query->before_commit_, phase == "BEFORE");
  EXPECT_EQ(parsed_query->statement_, statement);
}
}  // namespace

TEST_P(CypherMainVisitorTest, CreateTriggers) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("CREATE TRIGGER", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON ", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON ()", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON -->", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON CREATE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON () CREATE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON --> CREATE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON DELETE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON () DELETE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON --> DELETE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON UPDATE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON () UPDATE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON --> UPDATE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON CREATE BEFORE", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON CREATE BEFORE COMMIT", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON CREATE AFTER", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON CREATE AFTER COMMIT", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON -> CREATE AFTER COMMIT EXECUTE a", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON ) CREATE AFTER COMMIT EXECUTE a", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON ( CREATE AFTER COMMIT EXECUTE a", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON CRETE AFTER COMMIT EXECUTE a", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON DELET AFTER COMMIT EXECUTE a", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON UPDTE AFTER COMMIT EXECUTE a", ast_generator);
  TestInvalidQuery("CREATE TRIGGER trigger ON UPDATE COMMIT EXECUTE a", ast_generator);

  static constexpr std::string_view query_template = "CREATE TRIGGER trigger {} {} COMMIT EXECUTE {}";

  static constexpr std::array events{std::pair{"", memgraph::query::TriggerQuery::EventType::ANY},
                                     std::pair{"ON CREATE", memgraph::query::TriggerQuery::EventType::CREATE},
                                     std::pair{"ON () CREATE", memgraph::query::TriggerQuery::EventType::VERTEX_CREATE},
                                     std::pair{"ON --> CREATE", memgraph::query::TriggerQuery::EventType::EDGE_CREATE},
                                     std::pair{"ON DELETE", memgraph::query::TriggerQuery::EventType::DELETE},
                                     std::pair{"ON () DELETE", memgraph::query::TriggerQuery::EventType::VERTEX_DELETE},
                                     std::pair{"ON --> DELETE", memgraph::query::TriggerQuery::EventType::EDGE_DELETE},
                                     std::pair{"ON UPDATE", memgraph::query::TriggerQuery::EventType::UPDATE},
                                     std::pair{"ON () UPDATE", memgraph::query::TriggerQuery::EventType::VERTEX_UPDATE},
                                     std::pair{"ON --> UPDATE", memgraph::query::TriggerQuery::EventType::EDGE_UPDATE}};

  static constexpr std::array phases{"BEFORE", "AFTER"};

  static constexpr std::array statements{
      "", "SOME SUPER\nSTATEMENT", "Statement with 12312321 3     ", "        Statement with 12312321 3     "

  };

  for (const auto &[event_string, event_type] : events) {
    for (const auto &phase : phases) {
      for (const auto &statement : statements) {
        ValidateCreateQuery(ast_generator, fmt::format(query_template, event_string, phase, statement), "trigger",
                            event_type, phase, memgraph::utils::Trim(statement));
      }
    }
  }
}

namespace {
void ValidateSetIsolationLevelQuery(Base &ast_generator, const auto &query, const auto scope,
                                    const auto isolation_level) {
  auto *parsed_query = dynamic_cast<IsolationLevelQuery *>(ast_generator.ParseQuery(query));
  EXPECT_EQ(parsed_query->isolation_level_scope_, scope);
  EXPECT_EQ(parsed_query->isolation_level_, isolation_level);
}
}  // namespace

TEST_P(CypherMainVisitorTest, SetIsolationLevelQuery) {
  auto &ast_generator = *GetParam();
  TestInvalidQuery("SET ISO", ast_generator);
  TestInvalidQuery("SET TRANSACTION ISOLATION", ast_generator);
  TestInvalidQuery("SET TRANSACTION ISOLATION LEVEL", ast_generator);
  TestInvalidQuery("SET TRANSACTION ISOLATION LEVEL READ COMMITTED", ast_generator);
  TestInvalidQuery("SET NEXT TRANSACTION ISOLATION LEVEL", ast_generator);
  TestInvalidQuery("SET ISOLATION LEVEL READ COMMITTED", ast_generator);
  TestInvalidQuery("SET GLOBAL ISOLATION LEVEL READ COMMITTED", ast_generator);
  TestInvalidQuery("SET GLOBAL TRANSACTION ISOLATION LEVEL READ COMITTED", ast_generator);
  TestInvalidQuery("SET GLOBAL TRANSACTION ISOLATION LEVEL READ_COMITTED", ast_generator);
  TestInvalidQuery("SET SESSION TRANSACTION ISOLATION LEVEL READCOMITTED", ast_generator);

  static constexpr std::array scopes{
      std::pair{"GLOBAL", memgraph::query::IsolationLevelQuery::IsolationLevelScope::GLOBAL},
      std::pair{"SESSION", memgraph::query::IsolationLevelQuery::IsolationLevelScope::SESSION},
      std::pair{"NEXT", memgraph::query::IsolationLevelQuery::IsolationLevelScope::NEXT}};
  static constexpr std::array isolation_levels{
      std::pair{"READ UNCOMMITTED", memgraph::query::IsolationLevelQuery::IsolationLevel::READ_UNCOMMITTED},
      std::pair{"READ COMMITTED", memgraph::query::IsolationLevelQuery::IsolationLevel::READ_COMMITTED},
      std::pair{"SNAPSHOT ISOLATION", memgraph::query::IsolationLevelQuery::IsolationLevel::SNAPSHOT_ISOLATION}};

  static constexpr const auto *query_template = "SET {} TRANSACTION ISOLATION LEVEL {}";

  for (const auto &[scope_string, scope] : scopes) {
    for (const auto &[isolation_level_string, isolation_level] : isolation_levels) {
      ValidateSetIsolationLevelQuery(ast_generator, fmt::format(query_template, scope_string, isolation_level_string),
                                     scope, isolation_level);
    }
  }
}

TEST_P(CypherMainVisitorTest, CreateSnapshotQuery) {
  auto &ast_generator = *GetParam();
  ASSERT_TRUE(dynamic_cast<CreateSnapshotQuery *>(ast_generator.ParseQuery("CREATE SNAPSHOT")));
}

void CheckOptionalExpression(Base &ast_generator, Expression *expression, const std::optional<TypedValue> &expected) {
  EXPECT_EQ(expression != nullptr, expected.has_value());
  if (expected.has_value()) {
    EXPECT_NO_FATAL_FAILURE(ast_generator.CheckLiteral(expression, *expected));
  }
};

void ValidateMostlyEmptyStreamQuery(Base &ast_generator, const std::string &query_string,
                                    const StreamQuery::Action action, const std::string_view stream_name,
                                    const std::optional<TypedValue> &batch_limit = std::nullopt,
                                    const std::optional<TypedValue> &timeout = std::nullopt) {
  auto *parsed_query = dynamic_cast<StreamQuery *>(ast_generator.ParseQuery(query_string));
  ASSERT_NE(parsed_query, nullptr);
  EXPECT_EQ(parsed_query->action_, action);
  EXPECT_EQ(parsed_query->stream_name_, stream_name);
  auto topic_names = std::get_if<Expression *>(&parsed_query->topic_names_);
  EXPECT_NE(topic_names, nullptr);
  EXPECT_EQ(*topic_names, nullptr);
  EXPECT_TRUE(topic_names);
  EXPECT_FALSE(*topic_names);
  EXPECT_TRUE(parsed_query->transform_name_.empty());
  EXPECT_TRUE(parsed_query->consumer_group_.empty());
  EXPECT_EQ(parsed_query->batch_interval_, nullptr);
  EXPECT_EQ(parsed_query->batch_size_, nullptr);
  EXPECT_EQ(parsed_query->service_url_, nullptr);
  EXPECT_EQ(parsed_query->bootstrap_servers_, nullptr);
  EXPECT_NO_FATAL_FAILURE(CheckOptionalExpression(ast_generator, parsed_query->batch_limit_, batch_limit));
  EXPECT_NO_FATAL_FAILURE(CheckOptionalExpression(ast_generator, parsed_query->timeout_, timeout));
  EXPECT_TRUE(parsed_query->configs_.empty());
  EXPECT_TRUE(parsed_query->credentials_.empty());
}

TEST_P(CypherMainVisitorTest, DropStream) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("DROP ST", ast_generator);
  TestInvalidQuery("DROP STREAM", ast_generator);
  TestInvalidQuery("DROP STREAMS", ast_generator);

  ValidateMostlyEmptyStreamQuery(ast_generator, "DrOP STREAm droppedStream", StreamQuery::Action::DROP_STREAM,
                                 "droppedStream");
}

TEST_P(CypherMainVisitorTest, StartStream) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("START ST", ast_generator);
  TestInvalidQuery("START STREAM", ast_generator);
  TestInvalidQuery("START STREAMS", ast_generator);

  ValidateMostlyEmptyStreamQuery(ast_generator, "START STREAM startedStream", StreamQuery::Action::START_STREAM,
                                 "startedStream");
}

TEST_P(CypherMainVisitorTest, StartAllStreams) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("START ALL", ast_generator);
  TestInvalidQuery("START ALL STREAM", ast_generator);
  TestInvalidQuery("START STREAMS ALL", ast_generator);

  ValidateMostlyEmptyStreamQuery(ast_generator, "StARt AlL StrEAMS", StreamQuery::Action::START_ALL_STREAMS, "");
}

TEST_P(CypherMainVisitorTest, ShowStreams) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("SHOW ALL", ast_generator);
  TestInvalidQuery("SHOW STREAM", ast_generator);
  TestInvalidQuery("SHOW STREAMS ALL", ast_generator);

  ValidateMostlyEmptyStreamQuery(ast_generator, "SHOW STREAMS", StreamQuery::Action::SHOW_STREAMS, "");
}

TEST_P(CypherMainVisitorTest, StopStream) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("STOP ST", ast_generator);
  TestInvalidQuery("STOP STREAM", ast_generator);
  TestInvalidQuery("STOP STREAMS", ast_generator);
  TestInvalidQuery("STOP STREAM invalid stream name", ast_generator);

  ValidateMostlyEmptyStreamQuery(ast_generator, "STOP stREAM stoppedStream", StreamQuery::Action::STOP_STREAM,
                                 "stoppedStream");
}

TEST_P(CypherMainVisitorTest, StopAllStreams) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("STOP ALL", ast_generator);
  TestInvalidQuery("STOP ALL STREAM", ast_generator);
  TestInvalidQuery("STOP STREAMS ALL", ast_generator);

  ValidateMostlyEmptyStreamQuery(ast_generator, "SToP ALL STReaMS", StreamQuery::Action::STOP_ALL_STREAMS, "");
}

void ValidateTopicNames(const auto &topic_names, const std::vector<std::string> &expected_topic_names,
                        Base &ast_generator) {
  std::visit(memgraph::utils::Overloaded{
                 [&](Expression *expression) {
                   ast_generator.CheckLiteral(expression, memgraph::utils::Join(expected_topic_names, ","));
                 },
                 [&](const std::vector<std::string> &topic_names) { EXPECT_EQ(topic_names, expected_topic_names); }},
             topic_names);
}

void ValidateCreateKafkaStreamQuery(Base &ast_generator, const std::string &query_string,
                                    const std::string_view stream_name, const std::vector<std::string> &topic_names,
                                    const std::string_view transform_name, const std::string_view consumer_group,
                                    const std::optional<TypedValue> &batch_interval,
                                    const std::optional<TypedValue> &batch_size,
                                    const std::string_view bootstrap_servers,
                                    const std::unordered_map<std::string, std::string> &configs,
                                    const std::unordered_map<std::string, std::string> &credentials) {
  SCOPED_TRACE(query_string);
  StreamQuery *parsed_query{nullptr};
  ASSERT_NO_THROW(parsed_query = dynamic_cast<StreamQuery *>(ast_generator.ParseQuery(query_string))) << query_string;
  ASSERT_NE(parsed_query, nullptr);
  EXPECT_EQ(parsed_query->stream_name_, stream_name);
  ValidateTopicNames(parsed_query->topic_names_, topic_names, ast_generator);
  EXPECT_EQ(parsed_query->transform_name_, transform_name);
  EXPECT_EQ(parsed_query->consumer_group_, consumer_group);
  EXPECT_NO_FATAL_FAILURE(CheckOptionalExpression(ast_generator, parsed_query->batch_interval_, batch_interval));
  EXPECT_NO_FATAL_FAILURE(CheckOptionalExpression(ast_generator, parsed_query->batch_size_, batch_size));
  EXPECT_EQ(parsed_query->batch_limit_, nullptr);
  if (bootstrap_servers.empty()) {
    EXPECT_EQ(parsed_query->bootstrap_servers_, nullptr);
  } else {
    EXPECT_NE(parsed_query->bootstrap_servers_, nullptr);
  }

  const auto evaluate_config_map = [&ast_generator](const std::unordered_map<Expression *, Expression *> &config_map) {
    std::unordered_map<std::string, std::string> evaluated_config_map;
    const auto expr_to_str = [&ast_generator](Expression *expression) {
      return std::string{ast_generator.GetLiteral(expression, ast_generator.context_.is_query_cached).ValueString()};
    };
    std::transform(config_map.begin(), config_map.end(),
                   std::inserter(evaluated_config_map, evaluated_config_map.end()),
                   [&expr_to_str](const auto expr_pair) {
                     return std::pair{expr_to_str(expr_pair.first), expr_to_str(expr_pair.second)};
                   });
    return evaluated_config_map;
  };

  using testing::UnorderedElementsAreArray;
  EXPECT_THAT(evaluate_config_map(parsed_query->configs_), UnorderedElementsAreArray(configs.begin(), configs.end()));
  EXPECT_THAT(evaluate_config_map(parsed_query->credentials_),
              UnorderedElementsAreArray(credentials.begin(), credentials.end()));
}

TEST_P(CypherMainVisitorTest, CreateKafkaStream) {
  auto &ast_generator = *GetParam();
  TestInvalidQuery("CREATE KAFKA STREAM", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM invalid stream name TOPICS topic1 TRANSFORM transform", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS invalid topic name TRANSFORM transform", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM invalid transformation name", ast_generator);
  // required configs are missing
  TestInvalidQuery<SemanticException>("CREATE KAFKA STREAM stream TRANSFORM transform", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS TRANSFORM transform", ast_generator);
  // required configs are missing
  TestInvalidQuery<SemanticException>("CREATE KAFKA STREAM stream TOPICS topic1", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform CONSUMER_GROUP", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform CONSUMER_GROUP invalid consumer group",
                   ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform BATCH_INTERVAL", ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform BATCH_INTERVAL 'invalid interval'", ast_generator);
  TestInvalidQuery<SemanticException>("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform TOPICS topic2",
                                      ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform BATCH_SIZE", ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform BATCH_SIZE 'invalid size'", ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1, TRANSFORM transform BATCH_SIZE 2 CONSUMER_GROUP Gru",
                   ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform BOOTSTRAP_SERVERS localhost:9092",
                   ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform BOOTSTRAP_SERVERS", ast_generator);
  // the keys must be string literals
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform CONFIGS { symbolicname : 'string' }",
                   ast_generator);
  TestInvalidQuery(
      "CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform CREDENTIALS { symbolicname : 'string' }",
      ast_generator);
  TestInvalidQuery("CREATE KAFKA STREAM stream TOPICS topic1 TRANSFORM transform CREDENTIALS 2", ast_generator);

  const std::vector<std::string> topic_names{"topic1_name.with_dot", "topic1_name.with_multiple.dots",
                                             "topic-name.with-multiple.dots-and-dashes"};

  static constexpr std::string_view kStreamName{"SomeSuperStream"};
  static constexpr std::string_view kTransformName{"moreAwesomeTransform"};

  auto check_topic_names = [&](const std::vector<std::string> &topic_names) {
    static constexpr std::string_view kConsumerGroup{"ConsumerGru"};
    static constexpr int kBatchInterval = 324;
    const TypedValue batch_interval_value{kBatchInterval};
    static constexpr int kBatchSize = 1;
    const TypedValue batch_size_value{kBatchSize};

    const auto topic_names_as_str = memgraph::utils::Join(topic_names, ",");

    ValidateCreateKafkaStreamQuery(
        ast_generator,
        fmt::format("CREATE KAFKA STREAM {} TOPICS {} TRANSFORM {}", kStreamName, topic_names_as_str, kTransformName),
        kStreamName, topic_names, kTransformName, "", std::nullopt, std::nullopt, {}, {}, {});

    ValidateCreateKafkaStreamQuery(ast_generator,
                                   fmt::format("CREATE KAFKA STREAM {} TOPICS {} TRANSFORM {} CONSUMER_GROUP {} ",
                                               kStreamName, topic_names_as_str, kTransformName, kConsumerGroup),
                                   kStreamName, topic_names, kTransformName, kConsumerGroup, std::nullopt, std::nullopt,
                                   {}, {}, {});

    ValidateCreateKafkaStreamQuery(ast_generator,
                                   fmt::format("CREATE KAFKA STREAM {} TRANSFORM {} TOPICS {} BATCH_INTERVAL {}",
                                               kStreamName, kTransformName, topic_names_as_str, kBatchInterval),
                                   kStreamName, topic_names, kTransformName, "", batch_interval_value, std::nullopt, {},
                                   {}, {});

    ValidateCreateKafkaStreamQuery(ast_generator,
                                   fmt::format("CREATE KAFKA STREAM {} BATCH_SIZE {} TOPICS {} TRANSFORM {}",
                                               kStreamName, kBatchSize, topic_names_as_str, kTransformName),
                                   kStreamName, topic_names, kTransformName, "", std::nullopt, batch_size_value, {}, {},
                                   {});

    ValidateCreateKafkaStreamQuery(ast_generator,
                                   fmt::format("CREATE KAFKA STREAM {} TOPICS '{}' BATCH_SIZE {} TRANSFORM {}",
                                               kStreamName, topic_names_as_str, kBatchSize, kTransformName),
                                   kStreamName, topic_names, kTransformName, "", std::nullopt, batch_size_value, {}, {},
                                   {});

    ValidateCreateKafkaStreamQuery(
        ast_generator,
        fmt::format("CREATE KAFKA STREAM {} TOPICS {} TRANSFORM {} CONSUMER_GROUP {} BATCH_INTERVAL {} BATCH_SIZE {}",
                    kStreamName, topic_names_as_str, kTransformName, kConsumerGroup, kBatchInterval, kBatchSize),
        kStreamName, topic_names, kTransformName, kConsumerGroup, batch_interval_value, batch_size_value, {}, {}, {});
    using namespace std::string_literals;
    const auto host1 = "localhost:9094"s;
    ValidateCreateKafkaStreamQuery(
        ast_generator,
        fmt::format("CREATE KAFKA STREAM {} TOPICS {} CONSUMER_GROUP {} BATCH_SIZE {} BATCH_INTERVAL {} TRANSFORM {} "
                    "BOOTSTRAP_SERVERS '{}'",
                    kStreamName, topic_names_as_str, kConsumerGroup, kBatchSize, kBatchInterval, kTransformName, host1),
        kStreamName, topic_names, kTransformName, kConsumerGroup, batch_interval_value, batch_size_value, host1, {},
        {});

    ValidateCreateKafkaStreamQuery(
        ast_generator,
        fmt::format("CREATE KAFKA STREAM {} CONSUMER_GROUP {} TOPICS {} BATCH_INTERVAL {} TRANSFORM {} BATCH_SIZE {} "
                    "BOOTSTRAP_SERVERS '{}'",
                    kStreamName, kConsumerGroup, topic_names_as_str, kBatchInterval, kTransformName, kBatchSize, host1),
        kStreamName, topic_names, kTransformName, kConsumerGroup, batch_interval_value, batch_size_value, host1, {},
        {});

    const auto host2 = "localhost:9094,localhost:1994,168.1.1.256:345"s;
    ValidateCreateKafkaStreamQuery(
        ast_generator,
        fmt::format("CREATE KAFKA STREAM {} TOPICS {} BOOTSTRAP_SERVERS '{}' CONSUMER_GROUP {} TRANSFORM {} "
                    "BATCH_INTERVAL {} BATCH_SIZE {}",
                    kStreamName, topic_names_as_str, host2, kConsumerGroup, kTransformName, kBatchInterval, kBatchSize),
        kStreamName, topic_names, kTransformName, kConsumerGroup, batch_interval_value, batch_size_value, host2, {},
        {});
  };

  for (const auto &topic_name : topic_names) {
    EXPECT_NO_FATAL_FAILURE(check_topic_names({topic_name}));
  }

  EXPECT_NO_FATAL_FAILURE(check_topic_names(topic_names));

  auto check_consumer_group = [&](const std::string_view consumer_group) {
    const std::string kTopicName{"topic1"};
    ValidateCreateKafkaStreamQuery(ast_generator,
                                   fmt::format("CREATE KAFKA STREAM {} TOPICS {} TRANSFORM {} CONSUMER_GROUP {}",
                                               kStreamName, kTopicName, kTransformName, consumer_group),
                                   kStreamName, {kTopicName}, kTransformName, consumer_group, std::nullopt,
                                   std::nullopt, {}, {}, {});
  };

  using namespace std::literals;
  static constexpr std::array consumer_groups{"consumergru"sv, "consumer-group-with-dash"sv,
                                              "consumer_group.with.dot"sv, "consumer-group.With-Dot-and.dash"sv};

  for (const auto consumer_group : consumer_groups) {
    EXPECT_NO_FATAL_FAILURE(check_consumer_group(consumer_group));
  }

  auto check_config_map = [&](const std::unordered_map<std::string, std::string> &config_map) {
    const std::string kTopicName{"topic1"};

    const auto map_as_str = std::invoke([&config_map] {
      std::stringstream buffer;
      buffer << '{';
      if (!config_map.empty()) {
        auto it = config_map.begin();
        buffer << fmt::format("'{}': '{}'", it->first, it->second);
        for (; it != config_map.end(); ++it) {
          buffer << fmt::format(", '{}': '{}'", it->first, it->second);
        }
      }
      buffer << '}';
      return std::move(buffer).str();
    });

    ValidateCreateKafkaStreamQuery(ast_generator,
                                   fmt::format("CREATE KAFKA STREAM {} TOPICS {} TRANSFORM {} CONFIGS {}", kStreamName,
                                               kTopicName, kTransformName, map_as_str),
                                   kStreamName, {kTopicName}, kTransformName, "", std::nullopt, std::nullopt, {},
                                   config_map, {});

    ValidateCreateKafkaStreamQuery(ast_generator,
                                   fmt::format("CREATE KAFKA STREAM {} TOPICS {} TRANSFORM {} CREDENTIALS {}",
                                               kStreamName, kTopicName, kTransformName, map_as_str),
                                   kStreamName, {kTopicName}, kTransformName, "", std::nullopt, std::nullopt, {}, {},
                                   config_map);
  };

  const std::array config_maps = {std::unordered_map<std::string, std::string>{},
                                  std::unordered_map<std::string, std::string>{{"key", "value"}},
                                  std::unordered_map<std::string, std::string>{{"key.with.dot", "value.with.doth"},
                                                                               {"key with space", "value with space"}}};
  for (const auto &map_to_test : config_maps) {
    EXPECT_NO_FATAL_FAILURE(check_config_map(map_to_test));
  }
}

void ValidateCreatePulsarStreamQuery(Base &ast_generator, const std::string &query_string,
                                     const std::string_view stream_name, const std::vector<std::string> &topic_names,
                                     const std::string_view transform_name,
                                     const std::optional<TypedValue> &batch_interval,
                                     const std::optional<TypedValue> &batch_size, const std::string_view service_url) {
  SCOPED_TRACE(query_string);

  StreamQuery *parsed_query{nullptr};
  ASSERT_NO_THROW(parsed_query = dynamic_cast<StreamQuery *>(ast_generator.ParseQuery(query_string))) << query_string;
  ASSERT_NE(parsed_query, nullptr);
  EXPECT_EQ(parsed_query->stream_name_, stream_name);
  ValidateTopicNames(parsed_query->topic_names_, topic_names, ast_generator);
  EXPECT_EQ(parsed_query->transform_name_, transform_name);
  EXPECT_NO_FATAL_FAILURE(CheckOptionalExpression(ast_generator, parsed_query->batch_interval_, batch_interval));
  EXPECT_NO_FATAL_FAILURE(CheckOptionalExpression(ast_generator, parsed_query->batch_size_, batch_size));
  EXPECT_EQ(parsed_query->batch_limit_, nullptr);
  if (service_url.empty()) {
    EXPECT_EQ(parsed_query->service_url_, nullptr);
    return;
  }
  EXPECT_NE(parsed_query->service_url_, nullptr);
}

TEST_P(CypherMainVisitorTest, CreatePulsarStream) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("CREATE PULSAR STREAM", ast_generator);
  TestInvalidQuery<SemanticException>("CREATE PULSAR STREAM stream", ast_generator);
  TestInvalidQuery("CREATE PULSAR STREAM stream TOPICS", ast_generator);
  TestInvalidQuery<SemanticException>("CREATE PULSAR STREAM stream TOPICS topic_name", ast_generator);
  TestInvalidQuery("CREATE PULSAR STREAM stream TOPICS topic_name TRANSFORM", ast_generator);
  TestInvalidQuery("CREATE PULSAR STREAM stream TOPICS topic_name TRANSFORM transform.name SERVICE_URL", ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE PULSAR STREAM stream TOPICS topic_name TRANSFORM transform.name SERVICE_URL 1", ast_generator);
  TestInvalidQuery(
      "CREATE PULSAR STREAM stream TOPICS topic_name TRANSFORM transform.name BOOTSTRAP_SERVERS 'bootstrap'",
      ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE PULSAR STREAM stream TOPICS topic_name TRANSFORM transform.name SERVICE_URL 'test' TOPICS topic_name",
      ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE PULSAR STREAM stream TRANSFORM transform.name TOPICS topic_name TRANSFORM transform.name SERVICE_URL "
      "'test'",
      ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE PULSAR STREAM stream BATCH_INTERVAL 1 TOPICS topic_name TRANSFORM transform.name SERVICE_URL 'test' "
      "BATCH_INTERVAL 1000",
      ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE PULSAR STREAM stream BATCH_INTERVAL 'a' TOPICS topic_name TRANSFORM transform.name SERVICE_URL 'test'",
      ast_generator);
  TestInvalidQuery<SemanticException>(
      "CREATE PULSAR STREAM stream BATCH_SIZE 'a' TOPICS topic_name TRANSFORM transform.name SERVICE_URL 'test'",
      ast_generator);

  const std::vector<std::string> topic_names{"topic1", "topic2"};
  const std::string topic_names_str = memgraph::utils::Join(topic_names, ",");
  static constexpr std::string_view kStreamName{"PulsarStream"};
  static constexpr std::string_view kTransformName{"boringTransformation"};
  static constexpr std::string_view kServiceUrl{"localhost"};
  static constexpr int kBatchSize{1000};
  static constexpr int kBatchInterval{231321};

  {
    SCOPED_TRACE("single topic");
    ValidateCreatePulsarStreamQuery(
        ast_generator,
        fmt::format("CREATE PULSAR STREAM {} TOPICS {} TRANSFORM {}", kStreamName, topic_names[0], kTransformName),
        kStreamName, {topic_names[0]}, kTransformName, std::nullopt, std::nullopt, "");
  }
  {
    SCOPED_TRACE("multiple topics");
    ValidateCreatePulsarStreamQuery(
        ast_generator,
        fmt::format("CREATE PULSAR STREAM {} TRANSFORM {} TOPICS {}", kStreamName, kTransformName, topic_names_str),
        kStreamName, topic_names, kTransformName, std::nullopt, std::nullopt, "");
  }
  {
    SCOPED_TRACE("topic name in string");
    ValidateCreatePulsarStreamQuery(
        ast_generator,
        fmt::format("CREATE PULSAR STREAM {} TRANSFORM {} TOPICS '{}'", kStreamName, kTransformName, topic_names_str),
        kStreamName, topic_names, kTransformName, std::nullopt, std::nullopt, "");
  }
  {
    SCOPED_TRACE("service url");
    ValidateCreatePulsarStreamQuery(ast_generator,
                                    fmt::format("CREATE PULSAR STREAM {} SERVICE_URL '{}' TRANSFORM {} TOPICS {}",
                                                kStreamName, kServiceUrl, kTransformName, topic_names_str),
                                    kStreamName, topic_names, kTransformName, std::nullopt, std::nullopt, kServiceUrl);
    ValidateCreatePulsarStreamQuery(ast_generator,
                                    fmt::format("CREATE PULSAR STREAM {} TRANSFORM {} SERVICE_URL '{}' TOPICS {}",
                                                kStreamName, kTransformName, kServiceUrl, topic_names_str),
                                    kStreamName, topic_names, kTransformName, std::nullopt, std::nullopt, kServiceUrl);
  }
  {
    SCOPED_TRACE("batch size");
    ValidateCreatePulsarStreamQuery(
        ast_generator,
        fmt::format("CREATE PULSAR STREAM {} SERVICE_URL '{}' BATCH_SIZE {} TRANSFORM {} TOPICS {}", kStreamName,
                    kServiceUrl, kBatchSize, kTransformName, topic_names_str),
        kStreamName, topic_names, kTransformName, std::nullopt, TypedValue(kBatchSize), kServiceUrl);
    ValidateCreatePulsarStreamQuery(
        ast_generator,
        fmt::format("CREATE PULSAR STREAM {} TRANSFORM {} SERVICE_URL '{}' TOPICS {} BATCH_SIZE {}", kStreamName,
                    kTransformName, kServiceUrl, topic_names_str, kBatchSize),
        kStreamName, topic_names, kTransformName, std::nullopt, TypedValue(kBatchSize), kServiceUrl);
  }
  {
    SCOPED_TRACE("batch interval");
    ValidateCreatePulsarStreamQuery(
        ast_generator,
        fmt::format("CREATE PULSAR STREAM {} BATCH_INTERVAL {} SERVICE_URL '{}' BATCH_SIZE {} TRANSFORM {} TOPICS {}",
                    kStreamName, kBatchInterval, kServiceUrl, kBatchSize, kTransformName, topic_names_str),
        kStreamName, topic_names, kTransformName, TypedValue(kBatchInterval), TypedValue(kBatchSize), kServiceUrl);
    ValidateCreatePulsarStreamQuery(
        ast_generator,
        fmt::format("CREATE PULSAR STREAM {} TRANSFORM {} SERVICE_URL '{}' BATCH_INTERVAL {} TOPICS {} BATCH_SIZE {}",
                    kStreamName, kTransformName, kServiceUrl, kBatchInterval, topic_names_str, kBatchSize),
        kStreamName, topic_names, kTransformName, TypedValue(kBatchInterval), TypedValue(kBatchSize), kServiceUrl);
  }
}

TEST_P(CypherMainVisitorTest, CheckStream) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("CHECK STREAM", ast_generator);
  TestInvalidQuery("CHECK STREAMS", ast_generator);
  TestInvalidQuery("CHECK STREAMS something", ast_generator);
  TestInvalidQuery("CHECK STREAM something,something", ast_generator);
  TestInvalidQuery("CHECK STREAM something BATCH LIMIT 1", ast_generator);
  TestInvalidQuery("CHECK STREAM something BATCH_LIMIT", ast_generator);
  TestInvalidQuery("CHECK STREAM something TIMEOUT", ast_generator);
  TestInvalidQuery("CHECK STREAM something BATCH_LIMIT 1 TIMEOUT", ast_generator);
  TestInvalidQuery<SemanticException>("CHECK STREAM something BATCH_LIMIT 'it should be an integer'", ast_generator);
  TestInvalidQuery<SemanticException>("CHECK STREAM something BATCH_LIMIT 2.5", ast_generator);
  TestInvalidQuery<SemanticException>("CHECK STREAM something TIMEOUT 'it should be an integer'", ast_generator);

  ValidateMostlyEmptyStreamQuery(ast_generator, "CHECK STREAM checkedStream", StreamQuery::Action::CHECK_STREAM,
                                 "checkedStream");
  ValidateMostlyEmptyStreamQuery(ast_generator, "CHECK STREAM checkedStream bAtCH_LIMIT 42",
                                 StreamQuery::Action::CHECK_STREAM, "checkedStream", TypedValue(42));
  ValidateMostlyEmptyStreamQuery(ast_generator, "CHECK STREAM checkedStream TimEOuT 666",
                                 StreamQuery::Action::CHECK_STREAM, "checkedStream", std::nullopt, TypedValue(666));
  ValidateMostlyEmptyStreamQuery(ast_generator, "CHECK STREAM checkedStream BATCH_LIMIT 30 TIMEOUT 444",
                                 StreamQuery::Action::CHECK_STREAM, "checkedStream", TypedValue(30), TypedValue(444));
}

TEST_P(CypherMainVisitorTest, SettingQuery) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("SHOW DB SETTINGS", ast_generator);
  TestInvalidQuery("SHOW SETTINGS", ast_generator);
  TestInvalidQuery("SHOW DATABASE SETTING", ast_generator);
  TestInvalidQuery("SHOW DB SETTING 'setting'", ast_generator);
  TestInvalidQuery("SHOW SETTING 'setting'", ast_generator);
  TestInvalidQuery<SemanticException>("SHOW DATABASE SETTING 1", ast_generator);
  TestInvalidQuery("SET SETTING 'setting' TO 'value'", ast_generator);
  TestInvalidQuery("SET DB SETTING 'setting' TO 'value'", ast_generator);
  TestInvalidQuery<SemanticException>("SET DATABASE SETTING 1 TO 'value'", ast_generator);
  TestInvalidQuery<SemanticException>("SET DATABASE SETTING 'setting' TO 2", ast_generator);

  const auto validate_setting_query = [&](const auto &query, const auto action,
                                          const std::optional<TypedValue> &expected_setting_name,
                                          const std::optional<TypedValue> &expected_setting_value) {
    auto *parsed_query = dynamic_cast<SettingQuery *>(ast_generator.ParseQuery(query));
    EXPECT_EQ(parsed_query->action_, action) << query;
    EXPECT_NO_FATAL_FAILURE(CheckOptionalExpression(ast_generator, parsed_query->setting_name_, expected_setting_name));
    EXPECT_NO_FATAL_FAILURE(
        CheckOptionalExpression(ast_generator, parsed_query->setting_value_, expected_setting_value));
  };

  validate_setting_query("SHOW DATABASE SETTINGS", SettingQuery::Action::SHOW_ALL_SETTINGS, std::nullopt, std::nullopt);
  validate_setting_query("SHOW DATABASE SETTING 'setting'", SettingQuery::Action::SHOW_SETTING, TypedValue{"setting"},
                         std::nullopt);
  validate_setting_query("SET DATABASE SETTING 'setting' TO 'value'", SettingQuery::Action::SET_SETTING,
                         TypedValue{"setting"}, TypedValue{"value"});
}

TEST_P(CypherMainVisitorTest, VersionQuery) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("SHOW VERION", ast_generator);
  TestInvalidQuery("SHOW VER", ast_generator);
  TestInvalidQuery("SHOW VERSIONS", ast_generator);
  ASSERT_NO_THROW(ast_generator.ParseQuery("SHOW VERSION"));
}

TEST_P(CypherMainVisitorTest, ConfigQuery) {
  auto &ast_generator = *GetParam();

  TestInvalidQuery("SHOW CF", ast_generator);
  TestInvalidQuery("SHOW CFG", ast_generator);
  TestInvalidQuery("SHOW CFGS", ast_generator);
  TestInvalidQuery("SHOW CONF", ast_generator);
  TestInvalidQuery("SHOW CONFIGS", ast_generator);
  TestInvalidQuery("SHOW CONFIGURATION", ast_generator);
  TestInvalidQuery("SHOW CONFIGURATIONS", ast_generator);

  Query *query = ast_generator.ParseQuery("SHOW CONFIG");
  auto *ptr = dynamic_cast<ShowConfigQuery *>(query);
  ASSERT_TRUE(ptr != nullptr);

  ASSERT_NO_THROW(ast_generator.ParseQuery("SHOW CONFIG"));
}

TEST_P(CypherMainVisitorTest, ForeachThrow) {
  auto &ast_generator = *GetParam();
  EXPECT_THROW(ast_generator.ParseQuery("FOREACH(i IN [1, 2] | UNWIND [1,2,3] AS j CREATE (n))"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("FOREACH(i IN [1, 2] CREATE (:Foo {prop : i}))"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("FOREACH(i IN [1, 2] | MATCH (n)"), SyntaxException);
  EXPECT_THROW(ast_generator.ParseQuery("FOREACH(i IN x | MATCH (n)"), SyntaxException);
}

TEST_P(CypherMainVisitorTest, Foreach) {
  auto &ast_generator = *GetParam();
  // CREATE
  {
    auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("FOREACH (age IN [1, 2, 3] | CREATE (m:Age {amount: age}))"));
    ASSERT_TRUE(query);
    ASSERT_TRUE(query->single_query_);
    auto *single_query = query->single_query_;
    ASSERT_EQ(single_query->clauses_.size(), 1U);
    auto *foreach = dynamic_cast<Foreach *>(single_query->clauses_[0]);
    ASSERT_TRUE(foreach);
    ASSERT_TRUE(foreach->named_expression_);
    EXPECT_EQ(foreach->named_expression_->name_, "age");
    auto *expr = foreach->named_expression_->expression_;
    ASSERT_TRUE(expr);
    ASSERT_TRUE(dynamic_cast<ListLiteral *>(expr));
    const auto &clauses = foreach->clauses_;
    ASSERT_TRUE(clauses.size() == 1);
    ASSERT_TRUE(dynamic_cast<Create *>(clauses.front()));
  }
  // SET
  {
    auto *query =
        dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("FOREACH (i IN nodes(path) | SET i.checkpoint = true)"));
    auto *foreach = dynamic_cast<Foreach *>(query->single_query_->clauses_[0]);
    const auto &clauses = foreach->clauses_;
    ASSERT_TRUE(clauses.size() == 1);
    ASSERT_TRUE(dynamic_cast<SetProperty *>(clauses.front()));
  }
  // REMOVE
  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("FOREACH (i IN nodes(path) | REMOVE i.prop)"));
    auto *foreach = dynamic_cast<Foreach *>(query->single_query_->clauses_[0]);
    const auto &clauses = foreach->clauses_;
    ASSERT_TRUE(clauses.size() == 1);
    ASSERT_TRUE(dynamic_cast<RemoveProperty *>(clauses.front()));
  }
  // MERGE
  {
    // merge works as create here
    auto *query =
        dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("FOREACH (i IN [1, 2, 3] | MERGE (n {no : i}))"));
    auto *foreach = dynamic_cast<Foreach *>(query->single_query_->clauses_[0]);
    const auto &clauses = foreach->clauses_;
    ASSERT_TRUE(clauses.size() == 1);
    ASSERT_TRUE(dynamic_cast<Merge *>(clauses.front()));
  }
  // CYPHER DELETE
  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("FOREACH (i IN nodes(path) | DETACH DELETE i)"));
    auto *foreach = dynamic_cast<Foreach *>(query->single_query_->clauses_[0]);
    const auto &clauses = foreach->clauses_;
    ASSERT_TRUE(clauses.size() == 1);
    ASSERT_TRUE(dynamic_cast<Delete *>(clauses.front()));
  }
  // nested FOREACH
  {
    auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery(
        "FOREACH (i IN nodes(path) | FOREACH (age IN i.list | CREATE (m:Age {amount: age})))"));

    auto *foreach = dynamic_cast<Foreach *>(query->single_query_->clauses_[0]);
    const auto &clauses = foreach->clauses_;
    ASSERT_TRUE(clauses.size() == 1);
    ASSERT_TRUE(dynamic_cast<Foreach *>(clauses.front()));
  }
  // Multiple update clauses
  {
    auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("FOREACH (i IN nodes(path) | SET i.checkpoint = true REMOVE i.prop)"));
    auto *foreach = dynamic_cast<Foreach *>(query->single_query_->clauses_[0]);
    const auto &clauses = foreach->clauses_;
    ASSERT_TRUE(clauses.size() == 2);
    ASSERT_TRUE(dynamic_cast<SetProperty *>(clauses.front()));
    ASSERT_TRUE(dynamic_cast<RemoveProperty *>(*++clauses.begin()));
  }
}

TEST_P(CypherMainVisitorTest, ExistsThrow) {
  auto &ast_generator = *GetParam();

  TestInvalidQueryWithMessage<SyntaxException>("MATCH (n) WHERE exists(p=(n)-[]->()) RETURN n;", ast_generator,
                                               "Identifiers are not supported in exists(...).");
}

TEST_P(CypherMainVisitorTest, Exists) {
  auto &ast_generator = *GetParam();
  {
    const auto *query =
        dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH (n) WHERE exists((n)-[]->()) RETURN n;"));
    const auto *match = dynamic_cast<Match *>(query->single_query_->clauses_[0]);

    const auto *exists = dynamic_cast<Exists *>(match->where_->expression_);

    ASSERT_TRUE(exists);

    const auto pattern = exists->pattern_;
    ASSERT_TRUE(pattern->atoms_.size() == 3);

    const auto *node1 = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
    const auto *edge = dynamic_cast<EdgeAtom *>(pattern->atoms_[1]);
    const auto *node2 = dynamic_cast<NodeAtom *>(pattern->atoms_[2]);

    ASSERT_TRUE(node1);
    ASSERT_TRUE(edge);
    ASSERT_TRUE(node2);
  }

  {
    const auto *query =
        dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH (n) WHERE exists((n)-[]->()-[]->()) RETURN n;"));
    const auto *match = dynamic_cast<Match *>(query->single_query_->clauses_[0]);

    const auto *exists = dynamic_cast<Exists *>(match->where_->expression_);

    ASSERT_TRUE(exists);

    const auto pattern = exists->pattern_;
    ASSERT_TRUE(pattern->atoms_.size() == 5);

    const auto *node1 = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
    const auto *edge = dynamic_cast<EdgeAtom *>(pattern->atoms_[1]);
    const auto *node2 = dynamic_cast<NodeAtom *>(pattern->atoms_[2]);
    const auto *edge2 = dynamic_cast<EdgeAtom *>(pattern->atoms_[3]);
    const auto *node3 = dynamic_cast<NodeAtom *>(pattern->atoms_[4]);

    ASSERT_TRUE(node1);
    ASSERT_TRUE(edge);
    ASSERT_TRUE(node2);
    ASSERT_TRUE(edge2);
    ASSERT_TRUE(node3);
  }

  {
    const auto *query = dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH (n) WHERE exists((n)) RETURN n;"));
    const auto *match = dynamic_cast<Match *>(query->single_query_->clauses_[0]);

    const auto *exists = dynamic_cast<Exists *>(match->where_->expression_);

    ASSERT_TRUE(exists);

    const auto pattern = exists->pattern_;
    ASSERT_TRUE(pattern->atoms_.size() == 1);

    const auto *node = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);

    ASSERT_TRUE(node);
  }
}

TEST_P(CypherMainVisitorTest, CallSubqueryThrow) {
  auto &ast_generator = *GetParam();

  TestInvalidQueryWithMessage<SyntaxException>("MATCH (n) CALL { MATCH (m) RETURN m QUERY MEMORY UNLIMITED } RETURN n",
                                               ast_generator, "Memory limit cannot be set on subqueries!");
}

TEST_P(CypherMainVisitorTest, CallSubquery) {
  auto &ast_generator = *GetParam();

  {
    const auto *query =
        dynamic_cast<CypherQuery *>(ast_generator.ParseQuery("MATCH (n) CALL { MATCH (m) RETURN m } RETURN n, m"));
    const auto *call_subquery = dynamic_cast<CallSubquery *>(query->single_query_->clauses_[1]);

    const auto *subquery = dynamic_cast<CypherQuery *>(call_subquery->cypher_query_);
    ASSERT_TRUE(subquery);

    const auto *match = dynamic_cast<Match *>(subquery->single_query_->clauses_[0]);
    ASSERT_TRUE(match);
  }

  {
    const auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("MATCH (n) CALL { MATCH (m) RETURN (m) UNION MATCH (m) RETURN m } RETURN n, m"));
    const auto *call_subquery = dynamic_cast<CallSubquery *>(query->single_query_->clauses_[1]);

    const auto *subquery = dynamic_cast<CypherQuery *>(call_subquery->cypher_query_);
    ASSERT_TRUE(subquery);

    const auto *match = dynamic_cast<Match *>(subquery->single_query_->clauses_[0]);
    ASSERT_TRUE(match);

    const auto unions = subquery->cypher_unions_;
    ASSERT_TRUE(unions.size() == 1);
  }

  {
    const auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("MATCH (n) CALL { MATCH (m) RETURN (m) UNION ALL MATCH (m) RETURN m } RETURN n, m"));
    const auto *call_subquery = dynamic_cast<CallSubquery *>(query->single_query_->clauses_[1]);

    const auto *subquery = dynamic_cast<CypherQuery *>(call_subquery->cypher_query_);
    ASSERT_TRUE(subquery);

    const auto *match = dynamic_cast<Match *>(subquery->single_query_->clauses_[0]);
    ASSERT_TRUE(match);

    const auto unions = subquery->cypher_unions_;
    ASSERT_TRUE(unions.size() == 1);
  }

  {
    const auto *query = dynamic_cast<CypherQuery *>(
        ast_generator.ParseQuery("MATCH (n) CALL { MATCH (m) CALL { MATCH (o) RETURN o} RETURN m, o } RETURN n, m, o"));
    const auto *call_subquery = dynamic_cast<CallSubquery *>(query->single_query_->clauses_[1]);

    const auto *subquery = dynamic_cast<CypherQuery *>(call_subquery->cypher_query_);
    ASSERT_TRUE(subquery);

    const auto *match = dynamic_cast<Match *>(subquery->single_query_->clauses_[0]);
    ASSERT_TRUE(match);

    const auto *nested_subquery = dynamic_cast<CallSubquery *>(subquery->single_query_->clauses_[1]);
    ASSERT_TRUE(nested_subquery);

    const auto *nested_cypher = dynamic_cast<CypherQuery *>(nested_subquery->cypher_query_);
    ASSERT_TRUE(nested_cypher);

    const auto *nested_match = dynamic_cast<Match *>(nested_cypher->single_query_->clauses_[0]);
    ASSERT_TRUE(nested_match);
  }
}
