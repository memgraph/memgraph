
// Generated from /home/mislav/code/memgraph/memgraph/src/query/frontend/opencypher/grammar/Cypher.g4 by ANTLR 4.6

#pragma once


#include "antlr4-runtime.h"
#include "CypherParser.h"


namespace antlropencypher {

/**
 * This interface defines an abstract listener for a parse tree produced by CypherParser.
 */
class  CypherListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterCypher(CypherParser::CypherContext *ctx) = 0;
  virtual void exitCypher(CypherParser::CypherContext *ctx) = 0;

  virtual void enterStatement(CypherParser::StatementContext *ctx) = 0;
  virtual void exitStatement(CypherParser::StatementContext *ctx) = 0;

  virtual void enterQuery(CypherParser::QueryContext *ctx) = 0;
  virtual void exitQuery(CypherParser::QueryContext *ctx) = 0;

  virtual void enterRegularQuery(CypherParser::RegularQueryContext *ctx) = 0;
  virtual void exitRegularQuery(CypherParser::RegularQueryContext *ctx) = 0;

  virtual void enterSingleQuery(CypherParser::SingleQueryContext *ctx) = 0;
  virtual void exitSingleQuery(CypherParser::SingleQueryContext *ctx) = 0;

  virtual void enterCypherUnion(CypherParser::CypherUnionContext *ctx) = 0;
  virtual void exitCypherUnion(CypherParser::CypherUnionContext *ctx) = 0;

  virtual void enterClause(CypherParser::ClauseContext *ctx) = 0;
  virtual void exitClause(CypherParser::ClauseContext *ctx) = 0;

  virtual void enterCypherMatch(CypherParser::CypherMatchContext *ctx) = 0;
  virtual void exitCypherMatch(CypherParser::CypherMatchContext *ctx) = 0;

  virtual void enterUnwind(CypherParser::UnwindContext *ctx) = 0;
  virtual void exitUnwind(CypherParser::UnwindContext *ctx) = 0;

  virtual void enterMerge(CypherParser::MergeContext *ctx) = 0;
  virtual void exitMerge(CypherParser::MergeContext *ctx) = 0;

  virtual void enterMergeAction(CypherParser::MergeActionContext *ctx) = 0;
  virtual void exitMergeAction(CypherParser::MergeActionContext *ctx) = 0;

  virtual void enterCreate(CypherParser::CreateContext *ctx) = 0;
  virtual void exitCreate(CypherParser::CreateContext *ctx) = 0;

  virtual void enterSet(CypherParser::SetContext *ctx) = 0;
  virtual void exitSet(CypherParser::SetContext *ctx) = 0;

  virtual void enterSetItem(CypherParser::SetItemContext *ctx) = 0;
  virtual void exitSetItem(CypherParser::SetItemContext *ctx) = 0;

  virtual void enterCypherDelete(CypherParser::CypherDeleteContext *ctx) = 0;
  virtual void exitCypherDelete(CypherParser::CypherDeleteContext *ctx) = 0;

  virtual void enterRemove(CypherParser::RemoveContext *ctx) = 0;
  virtual void exitRemove(CypherParser::RemoveContext *ctx) = 0;

  virtual void enterRemoveItem(CypherParser::RemoveItemContext *ctx) = 0;
  virtual void exitRemoveItem(CypherParser::RemoveItemContext *ctx) = 0;

  virtual void enterWith(CypherParser::WithContext *ctx) = 0;
  virtual void exitWith(CypherParser::WithContext *ctx) = 0;

  virtual void enterCypherReturn(CypherParser::CypherReturnContext *ctx) = 0;
  virtual void exitCypherReturn(CypherParser::CypherReturnContext *ctx) = 0;

  virtual void enterReturnBody(CypherParser::ReturnBodyContext *ctx) = 0;
  virtual void exitReturnBody(CypherParser::ReturnBodyContext *ctx) = 0;

  virtual void enterReturnItems(CypherParser::ReturnItemsContext *ctx) = 0;
  virtual void exitReturnItems(CypherParser::ReturnItemsContext *ctx) = 0;

  virtual void enterReturnItem(CypherParser::ReturnItemContext *ctx) = 0;
  virtual void exitReturnItem(CypherParser::ReturnItemContext *ctx) = 0;

  virtual void enterOrder(CypherParser::OrderContext *ctx) = 0;
  virtual void exitOrder(CypherParser::OrderContext *ctx) = 0;

  virtual void enterSkip(CypherParser::SkipContext *ctx) = 0;
  virtual void exitSkip(CypherParser::SkipContext *ctx) = 0;

  virtual void enterLimit(CypherParser::LimitContext *ctx) = 0;
  virtual void exitLimit(CypherParser::LimitContext *ctx) = 0;

  virtual void enterSortItem(CypherParser::SortItemContext *ctx) = 0;
  virtual void exitSortItem(CypherParser::SortItemContext *ctx) = 0;

  virtual void enterWhere(CypherParser::WhereContext *ctx) = 0;
  virtual void exitWhere(CypherParser::WhereContext *ctx) = 0;

  virtual void enterPattern(CypherParser::PatternContext *ctx) = 0;
  virtual void exitPattern(CypherParser::PatternContext *ctx) = 0;

  virtual void enterPatternPart(CypherParser::PatternPartContext *ctx) = 0;
  virtual void exitPatternPart(CypherParser::PatternPartContext *ctx) = 0;

  virtual void enterAnonymousPatternPart(CypherParser::AnonymousPatternPartContext *ctx) = 0;
  virtual void exitAnonymousPatternPart(CypherParser::AnonymousPatternPartContext *ctx) = 0;

  virtual void enterPatternElement(CypherParser::PatternElementContext *ctx) = 0;
  virtual void exitPatternElement(CypherParser::PatternElementContext *ctx) = 0;

  virtual void enterNodePattern(CypherParser::NodePatternContext *ctx) = 0;
  virtual void exitNodePattern(CypherParser::NodePatternContext *ctx) = 0;

  virtual void enterPatternElementChain(CypherParser::PatternElementChainContext *ctx) = 0;
  virtual void exitPatternElementChain(CypherParser::PatternElementChainContext *ctx) = 0;

  virtual void enterRelationshipPattern(CypherParser::RelationshipPatternContext *ctx) = 0;
  virtual void exitRelationshipPattern(CypherParser::RelationshipPatternContext *ctx) = 0;

  virtual void enterRelationshipDetail(CypherParser::RelationshipDetailContext *ctx) = 0;
  virtual void exitRelationshipDetail(CypherParser::RelationshipDetailContext *ctx) = 0;

  virtual void enterProperties(CypherParser::PropertiesContext *ctx) = 0;
  virtual void exitProperties(CypherParser::PropertiesContext *ctx) = 0;

  virtual void enterRelationshipTypes(CypherParser::RelationshipTypesContext *ctx) = 0;
  virtual void exitRelationshipTypes(CypherParser::RelationshipTypesContext *ctx) = 0;

  virtual void enterNodeLabels(CypherParser::NodeLabelsContext *ctx) = 0;
  virtual void exitNodeLabels(CypherParser::NodeLabelsContext *ctx) = 0;

  virtual void enterNodeLabel(CypherParser::NodeLabelContext *ctx) = 0;
  virtual void exitNodeLabel(CypherParser::NodeLabelContext *ctx) = 0;

  virtual void enterRangeLiteral(CypherParser::RangeLiteralContext *ctx) = 0;
  virtual void exitRangeLiteral(CypherParser::RangeLiteralContext *ctx) = 0;

  virtual void enterLabelName(CypherParser::LabelNameContext *ctx) = 0;
  virtual void exitLabelName(CypherParser::LabelNameContext *ctx) = 0;

  virtual void enterRelTypeName(CypherParser::RelTypeNameContext *ctx) = 0;
  virtual void exitRelTypeName(CypherParser::RelTypeNameContext *ctx) = 0;

  virtual void enterExpression(CypherParser::ExpressionContext *ctx) = 0;
  virtual void exitExpression(CypherParser::ExpressionContext *ctx) = 0;

  virtual void enterExpression12(CypherParser::Expression12Context *ctx) = 0;
  virtual void exitExpression12(CypherParser::Expression12Context *ctx) = 0;

  virtual void enterExpression11(CypherParser::Expression11Context *ctx) = 0;
  virtual void exitExpression11(CypherParser::Expression11Context *ctx) = 0;

  virtual void enterExpression10(CypherParser::Expression10Context *ctx) = 0;
  virtual void exitExpression10(CypherParser::Expression10Context *ctx) = 0;

  virtual void enterExpression9(CypherParser::Expression9Context *ctx) = 0;
  virtual void exitExpression9(CypherParser::Expression9Context *ctx) = 0;

  virtual void enterExpression8(CypherParser::Expression8Context *ctx) = 0;
  virtual void exitExpression8(CypherParser::Expression8Context *ctx) = 0;

  virtual void enterExpression7(CypherParser::Expression7Context *ctx) = 0;
  virtual void exitExpression7(CypherParser::Expression7Context *ctx) = 0;

  virtual void enterExpression6(CypherParser::Expression6Context *ctx) = 0;
  virtual void exitExpression6(CypherParser::Expression6Context *ctx) = 0;

  virtual void enterExpression5(CypherParser::Expression5Context *ctx) = 0;
  virtual void exitExpression5(CypherParser::Expression5Context *ctx) = 0;

  virtual void enterExpression4(CypherParser::Expression4Context *ctx) = 0;
  virtual void exitExpression4(CypherParser::Expression4Context *ctx) = 0;

  virtual void enterExpression3(CypherParser::Expression3Context *ctx) = 0;
  virtual void exitExpression3(CypherParser::Expression3Context *ctx) = 0;

  virtual void enterExpression2(CypherParser::Expression2Context *ctx) = 0;
  virtual void exitExpression2(CypherParser::Expression2Context *ctx) = 0;

  virtual void enterAtom(CypherParser::AtomContext *ctx) = 0;
  virtual void exitAtom(CypherParser::AtomContext *ctx) = 0;

  virtual void enterLiteral(CypherParser::LiteralContext *ctx) = 0;
  virtual void exitLiteral(CypherParser::LiteralContext *ctx) = 0;

  virtual void enterBooleanLiteral(CypherParser::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(CypherParser::BooleanLiteralContext *ctx) = 0;

  virtual void enterListLiteral(CypherParser::ListLiteralContext *ctx) = 0;
  virtual void exitListLiteral(CypherParser::ListLiteralContext *ctx) = 0;

  virtual void enterPartialComparisonExpression(CypherParser::PartialComparisonExpressionContext *ctx) = 0;
  virtual void exitPartialComparisonExpression(CypherParser::PartialComparisonExpressionContext *ctx) = 0;

  virtual void enterParenthesizedExpression(CypherParser::ParenthesizedExpressionContext *ctx) = 0;
  virtual void exitParenthesizedExpression(CypherParser::ParenthesizedExpressionContext *ctx) = 0;

  virtual void enterRelationshipsPattern(CypherParser::RelationshipsPatternContext *ctx) = 0;
  virtual void exitRelationshipsPattern(CypherParser::RelationshipsPatternContext *ctx) = 0;

  virtual void enterFilterExpression(CypherParser::FilterExpressionContext *ctx) = 0;
  virtual void exitFilterExpression(CypherParser::FilterExpressionContext *ctx) = 0;

  virtual void enterIdInColl(CypherParser::IdInCollContext *ctx) = 0;
  virtual void exitIdInColl(CypherParser::IdInCollContext *ctx) = 0;

  virtual void enterFunctionInvocation(CypherParser::FunctionInvocationContext *ctx) = 0;
  virtual void exitFunctionInvocation(CypherParser::FunctionInvocationContext *ctx) = 0;

  virtual void enterFunctionName(CypherParser::FunctionNameContext *ctx) = 0;
  virtual void exitFunctionName(CypherParser::FunctionNameContext *ctx) = 0;

  virtual void enterListComprehension(CypherParser::ListComprehensionContext *ctx) = 0;
  virtual void exitListComprehension(CypherParser::ListComprehensionContext *ctx) = 0;

  virtual void enterPatternComprehension(CypherParser::PatternComprehensionContext *ctx) = 0;
  virtual void exitPatternComprehension(CypherParser::PatternComprehensionContext *ctx) = 0;

  virtual void enterPropertyLookup(CypherParser::PropertyLookupContext *ctx) = 0;
  virtual void exitPropertyLookup(CypherParser::PropertyLookupContext *ctx) = 0;

  virtual void enterVariable(CypherParser::VariableContext *ctx) = 0;
  virtual void exitVariable(CypherParser::VariableContext *ctx) = 0;

  virtual void enterNumberLiteral(CypherParser::NumberLiteralContext *ctx) = 0;
  virtual void exitNumberLiteral(CypherParser::NumberLiteralContext *ctx) = 0;

  virtual void enterMapLiteral(CypherParser::MapLiteralContext *ctx) = 0;
  virtual void exitMapLiteral(CypherParser::MapLiteralContext *ctx) = 0;

  virtual void enterParameter(CypherParser::ParameterContext *ctx) = 0;
  virtual void exitParameter(CypherParser::ParameterContext *ctx) = 0;

  virtual void enterPropertyExpression(CypherParser::PropertyExpressionContext *ctx) = 0;
  virtual void exitPropertyExpression(CypherParser::PropertyExpressionContext *ctx) = 0;

  virtual void enterPropertyKeyName(CypherParser::PropertyKeyNameContext *ctx) = 0;
  virtual void exitPropertyKeyName(CypherParser::PropertyKeyNameContext *ctx) = 0;

  virtual void enterIntegerLiteral(CypherParser::IntegerLiteralContext *ctx) = 0;
  virtual void exitIntegerLiteral(CypherParser::IntegerLiteralContext *ctx) = 0;

  virtual void enterDoubleLiteral(CypherParser::DoubleLiteralContext *ctx) = 0;
  virtual void exitDoubleLiteral(CypherParser::DoubleLiteralContext *ctx) = 0;

  virtual void enterSymbolicName(CypherParser::SymbolicNameContext *ctx) = 0;
  virtual void exitSymbolicName(CypherParser::SymbolicNameContext *ctx) = 0;

  virtual void enterLeftArrowHead(CypherParser::LeftArrowHeadContext *ctx) = 0;
  virtual void exitLeftArrowHead(CypherParser::LeftArrowHeadContext *ctx) = 0;

  virtual void enterRightArrowHead(CypherParser::RightArrowHeadContext *ctx) = 0;
  virtual void exitRightArrowHead(CypherParser::RightArrowHeadContext *ctx) = 0;

  virtual void enterDash(CypherParser::DashContext *ctx) = 0;
  virtual void exitDash(CypherParser::DashContext *ctx) = 0;


};

}  // namespace antlropencypher
