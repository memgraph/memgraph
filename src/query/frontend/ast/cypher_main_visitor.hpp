#pragma once

#include <string>
#include <unordered_set>
#include <utility>

#include "antlr4-runtime.h"
#include "query/context.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/named_antlr_tokens.hpp"
#include "query/frontend/opencypher/generated/CypherBaseVisitor.h"
#include "utils/exceptions.hpp"

namespace query {
namespace frontend {

using query::Context;
using antlropencypher::CypherParser;

class CypherMainVisitor : public antlropencypher::CypherBaseVisitor {
 public:
  CypherMainVisitor(Context &ctx) : ctx_(ctx) {}

 private:
  Expression *CreateBinaryOperatorByToken(size_t token, Expression *e1,
                                          Expression *e2) {
    switch (token) {
      case CypherParser::OR:
        return storage_.Create<OrOperator>(e1, e2);
      case CypherParser::XOR:
        return storage_.Create<XorOperator>(e1, e2);
      case CypherParser::AND:
        return storage_.Create<AndOperator>(e1, e2);
      case kPlusTokenId:
        return storage_.Create<AdditionOperator>(e1, e2);
      case kMinusTokenId:
        return storage_.Create<SubtractionOperator>(e1, e2);
      case kMultTokenId:
        return storage_.Create<MultiplicationOperator>(e1, e2);
      case kDivTokenId:
        return storage_.Create<DivisionOperator>(e1, e2);
      case kModTokenId:
        return storage_.Create<ModOperator>(e1, e2);
      case kEqTokenId:
        return storage_.Create<EqualOperator>(e1, e2);
      case kNeTokenId1:
      case kNeTokenId2:
        return storage_.Create<NotEqualOperator>(e1, e2);
      case kLtTokenId:
        return storage_.Create<LessOperator>(e1, e2);
      case kGtTokenId:
        return storage_.Create<GreaterOperator>(e1, e2);
      case kLeTokenId:
        return storage_.Create<LessEqualOperator>(e1, e2);
      case kGeTokenId:
        return storage_.Create<GreaterEqualOperator>(e1, e2);
      default:
        throw utils::NotYetImplemented();
    }
  }

  Expression *CreateUnaryOperatorByToken(size_t token, Expression *e) {
    switch (token) {
      case CypherParser::NOT:
        return storage_.Create<NotOperator>(e);
      case kUnaryPlusTokenId:
        return storage_.Create<UnaryPlusOperator>(e);
      case kUnaryMinusTokenId:
        return storage_.Create<UnaryMinusOperator>(e);
      default:
        throw utils::NotYetImplemented();
    }
  }

  auto ExtractOperators(std::vector<antlr4::tree::ParseTree *> &all_children,
                        const std::vector<size_t> &allowed_operators) {
    std::vector<size_t> operators;
    for (auto *child : all_children) {
      antlr4::tree::TerminalNode *operator_node = nullptr;
      if ((operator_node = dynamic_cast<antlr4::tree::TerminalNode *>(child))) {
        if (std::find(allowed_operators.begin(), allowed_operators.end(),
                      operator_node->getSymbol()->getType()) !=
            allowed_operators.end()) {
          operators.push_back(operator_node->getSymbol()->getType());
        }
      }
    }
    return operators;
  }

  /**
   * Convert opencypher's n-ary production to ast binary operators.
   *
   * @param _expressions Subexpressions of child for which we construct ast
   * operators, for example expression6 if we want to create ast nodes for
   * expression7.
   */
  template <typename TExpression>
  Expression *LeftAssociativeOperatorExpression(
      std::vector<TExpression *> _expressions,
      std::vector<antlr4::tree::ParseTree *> all_children,
      const std::vector<size_t> &allowed_operators) {
    debug_assert(_expressions.size(), "can't happen");
    std::vector<Expression *> expressions;
    auto operators = ExtractOperators(all_children, allowed_operators);

    for (auto *expression : _expressions) {
      expressions.push_back(expression->accept(this));
    }

    Expression *first_operand = expressions[0];
    for (int i = 1; i < (int)expressions.size(); ++i) {
      first_operand = CreateBinaryOperatorByToken(
          operators[i - 1], first_operand, expressions[i]);
    }
    return first_operand;
  }

  template <typename TExpression>
  Expression *PrefixUnaryOperator(
      TExpression *_expression,
      std::vector<antlr4::tree::ParseTree *> all_children,
      const std::vector<size_t> &allowed_operators) {
    debug_assert(_expression, "can't happen");
    auto operators = ExtractOperators(all_children, allowed_operators);

    Expression *expression = _expression->accept(this);
    for (int i = (int)operators.size() - 1; i >= 0; --i) {
      expression = CreateUnaryOperatorByToken(operators[i], expression);
    }
    return expression;
  }

  /**
   * @return Query*
   */
  antlrcpp::Any visitSingleQuery(
      CypherParser::SingleQueryContext *ctx) override;

  /**
   * @return Clause* or vector<Clause*>!!!
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
   * @return vector<NamedExpression*>
   */
  antlrcpp::Any visitReturnItems(
      CypherParser::ReturnItemsContext *ctx) override;

  /**
   * @return vector<NamedExpression*>
   */
  antlrcpp::Any visitReturnItem(CypherParser::ReturnItemContext *ctx) override;

  /**
   * @return vector<pair<Ordering, Expression*>>
   */
  antlrcpp::Any visitOrder(CypherParser::OrderContext *ctx) override;

  /**
   * @return pair<Ordering, Expression*>
   */
  antlrcpp::Any visitSortItem(CypherParser::SortItemContext *ctx) override;

  /**
   * @return NodeAtom*
   */
  antlrcpp::Any visitNodePattern(
      CypherParser::NodePatternContext *ctx) override;

  /**
   * @return vector<GraphDbTypes::Label>
   */
  antlrcpp::Any visitNodeLabels(CypherParser::NodeLabelsContext *ctx) override;

  /**
   * @return unordered_map<GraphDbTypes::Property, Expression*>
   */
  antlrcpp::Any visitProperties(CypherParser::PropertiesContext *ctx) override;

  /**
   * @return unordered_map<GraphDbTypes::Property, Expression*>
   */
  antlrcpp::Any visitMapLiteral(CypherParser::MapLiteralContext *ctx) override;

  /**
   * @return vector<Expression*>
   */
  antlrcpp::Any visitListLiteral(
      CypherParser::ListLiteralContext *ctx) override;

  /**
   * @return GraphDbTypes::Property
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
   * @return vector<GraphDbTypes::EdgeType>
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

  /**
   * OR.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression12(
      CypherParser::Expression12Context *ctx) override;

  /**
   * XOR.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression11(
      CypherParser::Expression11Context *ctx) override;

  /**
   * AND.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression10(
      CypherParser::Expression10Context *ctx) override;

  /**
   * NOT.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression9(
      CypherParser::Expression9Context *ctx) override;

  /**
   * Comparisons.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression8(
      CypherParser::Expression8Context *ctx) override;

  /**
   * Never call this. Everything related to generating code for comparison
   * operators should be done in visitExpression8.
   */
  antlrcpp::Any visitPartialComparisonExpression(
      CypherParser::PartialComparisonExpressionContext *ctx) override;

  /**
   * Addition and subtraction.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression7(
      CypherParser::Expression7Context *ctx) override;

  /**
   * Multiplication, division, modding.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression6(
      CypherParser::Expression6Context *ctx) override;

  /**
   * Power.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression5(
      CypherParser::Expression5Context *ctx) override;

  /**
   * Unary minus and plus.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression4(
      CypherParser::Expression4Context *ctx) override;

  /**
   * IS NULL, IS NOT NULL, ...
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression3a(
      CypherParser::Expression3aContext *ctx) override;

  /**
   * List indexing and slicing.
   *
   * @return Expression*
   */
  antlrcpp::Any visitExpression3b(
      CypherParser::Expression3bContext *ctx) override;

  /**
   * Does nothing, everything is done in visitExpression3b.
   */
  antlrcpp::Any visitListIndexingOrSlicing(
      CypherParser::ListIndexingOrSlicingContext *ctx) override;

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

  /**
   * @return Expression*
   */
  antlrcpp::Any visitParenthesizedExpression(
      CypherParser::ParenthesizedExpressionContext *ctx) override;

  /**
   * @return Expression*
   */
  antlrcpp::Any visitFunctionInvocation(
      CypherParser::FunctionInvocationContext *ctx) override;

  /**
   * @return string - uppercased
   */
  antlrcpp::Any visitFunctionName(
      CypherParser::FunctionNameContext *ctx) override;

  /**
   * @return BaseLiteral*
   */
  antlrcpp::Any visitLiteral(CypherParser::LiteralContext *ctx) override;

  /**
   * Convert escaped string from a query to unescaped utf8 string.
   *
   * @return string
   */
  antlrcpp::Any visitStringLiteral(const std::string &escaped);

  /**
   * @return bool
   */
  antlrcpp::Any visitBooleanLiteral(
      CypherParser::BooleanLiteralContext *ctx) override;

  /**
   * @return TypedValue with either double or int
   */
  antlrcpp::Any visitNumberLiteral(
      CypherParser::NumberLiteralContext *ctx) override;

  /**
   * @return int64_t
   */
  antlrcpp::Any visitIntegerLiteral(
      CypherParser::IntegerLiteralContext *ctx) override;

  /**
   * @return double
   */
  antlrcpp::Any visitDoubleLiteral(
      CypherParser::DoubleLiteralContext *ctx) override;

  /**
   * @return Delete*
   */
  antlrcpp::Any visitCypherDelete(
      CypherParser::CypherDeleteContext *ctx) override;

  /**
   * @return Where*
   */
  antlrcpp::Any visitWhere(CypherParser::WhereContext *ctx) override;

  /**
   * return vector<Clause*>
   */
  antlrcpp::Any visitSet(CypherParser::SetContext *ctx) override;

  /**
   * @return Clause*
   */
  antlrcpp::Any visitSetItem(CypherParser::SetItemContext *ctx) override;

  /**
   * return vector<Clause*>
   */
  antlrcpp::Any visitRemove(CypherParser::RemoveContext *ctx) override;

  /**
   * @return Clause*
   */
  antlrcpp::Any visitRemoveItem(CypherParser::RemoveItemContext *ctx) override;

  /**
   * @return PropertyLookup*
   */
  antlrcpp::Any visitPropertyExpression(
      CypherParser::PropertyExpressionContext *ctx) override;

  /**
   * @return With*
   */
  antlrcpp::Any visitWith(CypherParser::WithContext *ctx) override;

  /**
   * @return Merge*
   */
  antlrcpp::Any visitMerge(CypherParser::MergeContext *ctx) override;

  /**
   * @return Unwind*
   */
  antlrcpp::Any visitUnwind(CypherParser::UnwindContext *ctx) override;

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
  // All return items which are not variables must be aliased in with.
  // We use this variable in visitReturnItem to check if we are in with or
  // return.
  bool in_with_ = false;
};
}
}
