#pragma once

#include <string>
#include <unordered_set>

#include "antlr4-runtime.h"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/opencypher/generated/CypherBaseVisitor.h"

namespace query {
namespace frontend {

using query::Context;
using antlropencypher::CypherParser;

class CypherMainVisitor : public antlropencypher::CypherBaseVisitor {
 public:
  CypherMainVisitor(Context &ctx) : ctx_(ctx) {}

 private:
  //  template <typename TExpression>
  //  antlrcpp::Any
  //  LeftAssociativeOperatorExpression(std::vector<TExpression *> children,
  //                                    std::vector<Function> ops) {
  //    assert(children.size());
  //    std::vector<std::string> children_ids;
  //
  //    for (auto *child : children) {
  //      children_ids.push_back(child->accept(this).template
  //      as<std::string>());
  //    }
  //
  //    std::string first_operand = children_ids[0];
  //    for (int i = 0; i < (int)ops.size(); ++i) {
  //      auto lhs_id = new_id();
  //      symbol_table_[lhs_id] =
  //          SimpleExpression{ops[i], {first_operand, children_ids[i + 1]}};
  //      first_operand = lhs_id;
  //    }
  //    return first_operand;
  //  }
  //
  //  template <typename TExpression>
  //  antlrcpp::Any
  //  LeftAssociativeOperatorExpression(std::vector<TExpression *> children,
  //                                    Function op) {
  //    return LeftAssociativeOperatorExpression(
  //        children, std::vector<Function>((int)children.size() - 1, op));
  //  }

  /**
   * @return Query*
   */
  antlrcpp::Any visitSingleQuery(
      CypherParser::SingleQueryContext *ctx) override;

  /**
   * @return Clause*
   */
  antlrcpp::Any visitClause(CypherParser::ClauseContext *ctx) override;

  /**
   * @return Match*
   */
  antlrcpp::Any visitCypherMatch(
      CypherParser::CypherMatchContext *ctx) override;

  /**
   * @return Create*
   */
  antlrcpp::Any visitCreate(CypherParser::CreateContext *ctx) override;

  /**
   * @return Return*
   */
  antlrcpp::Any visitCypherReturn(
      CypherParser::CypherReturnContext *ctx) override;

  /**
   * @return Return*
   */
  antlrcpp::Any visitReturnBody(CypherParser::ReturnBodyContext *ctx) override;

  /**
   * @return Return*
   */
  antlrcpp::Any visitReturnItems(
      CypherParser::ReturnItemsContext *ctx) override;

  /**
   * @return NamedExpression*
   */
  antlrcpp::Any visitReturnItem(CypherParser::ReturnItemContext *ctx) override;

  /**
   * @return NodeAtom*
   */
  antlrcpp::Any visitNodePattern(
      CypherParser::NodePatternContext *ctx) override;

  /**
   * @return vector<GraphDb::Label>
   */
  antlrcpp::Any visitNodeLabels(CypherParser::NodeLabelsContext *ctx) override;

  /**
   * @return unordered_map<GraphDb::Property, Expression*>
   */
  antlrcpp::Any visitProperties(CypherParser::PropertiesContext *ctx) override;

  /**
   * @return unordered_map<GraphDb::Property, Expression*>
   */
  antlrcpp::Any visitMapLiteral(CypherParser::MapLiteralContext *ctx) override;

  /**
   * @return GraphDb::Property
   */
  antlrcpp::Any visitPropertyKeyName(
      CypherParser::PropertyKeyNameContext *ctx) override;

  /**
   * @return string
   */
  antlrcpp::Any visitSymbolicName(
      CypherParser::SymbolicNameContext *ctx) override;

  /**
   * @return vector<Pattern*>
   */
  antlrcpp::Any visitPattern(CypherParser::PatternContext *ctx) override;

  /**
   * @return Pattern*
   */
  antlrcpp::Any visitPatternPart(
      CypherParser::PatternPartContext *ctx) override;

  /**
   * @return Pattern*
   */
  antlrcpp::Any visitPatternElement(
      CypherParser::PatternElementContext *ctx) override;

  /**
   * @return vector<pair<EdgeAtom*, NodeAtom*>>
   */
  antlrcpp::Any visitPatternElementChain(
      CypherParser::PatternElementChainContext *ctx) override;

  /**
   *@return EdgeAtom*
   */
  antlrcpp::Any visitRelationshipPattern(
      CypherParser::RelationshipPatternContext *ctx) override;

  /**
  * This should never be called. Everything is done directly in
  * visitRelationshipPattern.
  */
  antlrcpp::Any visitRelationshipDetail(
      CypherParser::RelationshipDetailContext *ctx) override;

  /**
   * @return vector<GraphDb::EdgeType>
   */
  antlrcpp::Any visitRelationshipTypes(
      CypherParser::RelationshipTypesContext *ctx) override;

  /**
  * @return pair<int64_t, int64_t>.
  */
  antlrcpp::Any visitRangeLiteral(
      CypherParser::RangeLiteralContext *ctx) override;

  /**
   * Top level expression, does nothing.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression(CypherParser::ExpressionContext *ctx) override;

  ///**
  //* OR.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression12(CypherParser::Expression12Context *ctx) override;

  ///**
  //* XOR.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression11(CypherParser::Expression11Context *ctx) override;

  ///**
  //* AND.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression10(CypherParser::Expression10Context *ctx) override;

  ///**
  //* NOT.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression9(CypherParser::Expression9Context *ctx) override;

  ///**
  //* Comparisons.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression8(CypherParser::Expression8Context *ctx) override;

  ///**
  //* Never call this. Everything related to generating code for comparison
  //* operators should be done in visitExpression8.
  //*/
  // antlrcpp::Any visitPartialComparisonExpression(
  //    CypherParser::PartialComparisonExpressionContext *ctx) override;

  ///**
  //* Addition and subtraction.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression7(CypherParser::Expression7Context *ctx) override;

  ///**
  //* Multiplication, division, modding.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression6(CypherParser::Expression6Context *ctx) override;

  ///**
  //* Power.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression5(CypherParser::Expression5Context *ctx) override;

  ///**
  //* Unary minus and plus.
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression4(CypherParser::Expression4Context *ctx) override;

  ///**
  //* Element of a list, range of a list...
  //*
  //* @return string - expression id.
  //*/
  // antlrcpp::Any
  // visitExpression3(CypherParser::Expression3Context *ctx) override;

  /**
  * Property lookup, test for node labels existence...
  *
  * @return Expression*
  */
  antlrcpp::Any visitExpression2(
      CypherParser::Expression2Context *ctx) override;

  /**
  * Literals, params, list comprehension...
  *
  * @return Expression*
  */
  antlrcpp::Any visitAtom(CypherParser::AtomContext *ctx) override;

  //  antlrcpp::Any visitLiteral(CypherParser::LiteralContext *ctx) override {
  //    return visitChildren(ctx);
  //  }
  //
  //  antlrcpp::Any visitBooleanLiteral(
  //      CypherParser::BooleanLiteralContext *ctx) override {
  //    return visitChildren(ctx);
  //  }
  //
  //  antlrcpp::Any visitListLiteral(
  //      CypherParser::ListLiteralContext *ctx) override {
  //    return visitChildren(ctx);
  //  }
  //
  //  antlrcpp::Any visitParenthesizedExpression(
  //      CypherParser::ParenthesizedExpressionContext *ctx) override {
  //    return visitChildren(ctx);
  //  }

  /**
  * @return int64_t.
  */
  antlrcpp::Any visitIntegerLiteral(
      CypherParser::IntegerLiteralContext *ctx) override;

 public:
  Query *query() { return query_; }
  const static std::string kAnonPrefix;

 private:
  Context &ctx_;
  // Set of identifiers from queries.
  std::unordered_set<std::string> users_identifiers;
  // Identifiers that user didn't name.
  std::vector<Identifier **> anonymous_identifiers;
  AstTreeStorage storage_;
  Query *query_ = nullptr;
};
}
}
