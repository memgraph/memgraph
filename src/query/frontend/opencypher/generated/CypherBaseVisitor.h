
// Generated from /home/buda/Workspace/code/memgraph/memgraph/src/query/frontend/opencypher/grammar/Cypher.g4 by ANTLR 4.6

#pragma once


#include "antlr4-runtime.h"
#include "CypherVisitor.h"


namespace antlrcpptest {

/**
 * This class provides an empty implementation of CypherVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  CypherBaseVisitor : public CypherVisitor {
public:

  virtual antlrcpp::Any visitCypher(CypherParser::CypherContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitStatement(CypherParser::StatementContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQuery(CypherParser::QueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRegularQuery(CypherParser::RegularQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSingleQuery(CypherParser::SingleQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCypherUnion(CypherParser::CypherUnionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitClause(CypherParser::ClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCypherMatch(CypherParser::CypherMatchContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnwind(CypherParser::UnwindContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMerge(CypherParser::MergeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMergeAction(CypherParser::MergeActionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreate(CypherParser::CreateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSet(CypherParser::SetContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSetItem(CypherParser::SetItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCypherDelete(CypherParser::CypherDeleteContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRemove(CypherParser::RemoveContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRemoveItem(CypherParser::RemoveItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWith(CypherParser::WithContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCypherReturn(CypherParser::CypherReturnContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitReturnBody(CypherParser::ReturnBodyContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitReturnItems(CypherParser::ReturnItemsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitReturnItem(CypherParser::ReturnItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOrder(CypherParser::OrderContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSkip(CypherParser::SkipContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLimit(CypherParser::LimitContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSortItem(CypherParser::SortItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWhere(CypherParser::WhereContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPattern(CypherParser::PatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPatternPart(CypherParser::PatternPartContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAnonymousPatternPart(CypherParser::AnonymousPatternPartContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPatternElement(CypherParser::PatternElementContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNodePattern(CypherParser::NodePatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPatternElementChain(CypherParser::PatternElementChainContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRelationshipPattern(CypherParser::RelationshipPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRelationshipDetail(CypherParser::RelationshipDetailContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitProperties(CypherParser::PropertiesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRelationshipTypes(CypherParser::RelationshipTypesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNodeLabels(CypherParser::NodeLabelsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNodeLabel(CypherParser::NodeLabelContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRangeLiteral(CypherParser::RangeLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLabelName(CypherParser::LabelNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRelTypeName(CypherParser::RelTypeNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression(CypherParser::ExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression12(CypherParser::Expression12Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression11(CypherParser::Expression11Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression10(CypherParser::Expression10Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression9(CypherParser::Expression9Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression8(CypherParser::Expression8Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression7(CypherParser::Expression7Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression6(CypherParser::Expression6Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression5(CypherParser::Expression5Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression4(CypherParser::Expression4Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression3(CypherParser::Expression3Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExpression2(CypherParser::Expression2Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAtom(CypherParser::AtomContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLiteral(CypherParser::LiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBooleanLiteral(CypherParser::BooleanLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitListLiteral(CypherParser::ListLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPartialComparisonExpression(CypherParser::PartialComparisonExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitParenthesizedExpression(CypherParser::ParenthesizedExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRelationshipsPattern(CypherParser::RelationshipsPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFilterExpression(CypherParser::FilterExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIdInColl(CypherParser::IdInCollContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFunctionInvocation(CypherParser::FunctionInvocationContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFunctionName(CypherParser::FunctionNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitListComprehension(CypherParser::ListComprehensionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPropertyLookup(CypherParser::PropertyLookupContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitVariable(CypherParser::VariableContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumberLiteral(CypherParser::NumberLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitMapLiteral(CypherParser::MapLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitParameter(CypherParser::ParameterContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPropertyExpression(CypherParser::PropertyExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPropertyKeyName(CypherParser::PropertyKeyNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIntegerLiteral(CypherParser::IntegerLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDoubleLiteral(CypherParser::DoubleLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSymbolicName(CypherParser::SymbolicNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLeftArrowHead(CypherParser::LeftArrowHeadContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRightArrowHead(CypherParser::RightArrowHeadContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDash(CypherParser::DashContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace antlrcpptest
