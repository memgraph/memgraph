
// Generated from /home/mislav/code/memgraph/memgraph/src/query/frontend/opencypher/grammar/Cypher.g4 by ANTLR 4.6

#pragma once


#include "antlr4-runtime.h"
#include "CypherListener.h"


namespace antlropencypher {

/**
 * This class provides an empty implementation of CypherListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  CypherBaseListener : public CypherListener {
public:

  virtual void enterCypher(CypherParser::CypherContext * /*ctx*/) override { }
  virtual void exitCypher(CypherParser::CypherContext * /*ctx*/) override { }

  virtual void enterStatement(CypherParser::StatementContext * /*ctx*/) override { }
  virtual void exitStatement(CypherParser::StatementContext * /*ctx*/) override { }

  virtual void enterQuery(CypherParser::QueryContext * /*ctx*/) override { }
  virtual void exitQuery(CypherParser::QueryContext * /*ctx*/) override { }

  virtual void enterRegularQuery(CypherParser::RegularQueryContext * /*ctx*/) override { }
  virtual void exitRegularQuery(CypherParser::RegularQueryContext * /*ctx*/) override { }

  virtual void enterSingleQuery(CypherParser::SingleQueryContext * /*ctx*/) override { }
  virtual void exitSingleQuery(CypherParser::SingleQueryContext * /*ctx*/) override { }

  virtual void enterCypherUnion(CypherParser::CypherUnionContext * /*ctx*/) override { }
  virtual void exitCypherUnion(CypherParser::CypherUnionContext * /*ctx*/) override { }

  virtual void enterClause(CypherParser::ClauseContext * /*ctx*/) override { }
  virtual void exitClause(CypherParser::ClauseContext * /*ctx*/) override { }

  virtual void enterCypherMatch(CypherParser::CypherMatchContext * /*ctx*/) override { }
  virtual void exitCypherMatch(CypherParser::CypherMatchContext * /*ctx*/) override { }

  virtual void enterUnwind(CypherParser::UnwindContext * /*ctx*/) override { }
  virtual void exitUnwind(CypherParser::UnwindContext * /*ctx*/) override { }

  virtual void enterMerge(CypherParser::MergeContext * /*ctx*/) override { }
  virtual void exitMerge(CypherParser::MergeContext * /*ctx*/) override { }

  virtual void enterMergeAction(CypherParser::MergeActionContext * /*ctx*/) override { }
  virtual void exitMergeAction(CypherParser::MergeActionContext * /*ctx*/) override { }

  virtual void enterCreate(CypherParser::CreateContext * /*ctx*/) override { }
  virtual void exitCreate(CypherParser::CreateContext * /*ctx*/) override { }

  virtual void enterSet(CypherParser::SetContext * /*ctx*/) override { }
  virtual void exitSet(CypherParser::SetContext * /*ctx*/) override { }

  virtual void enterSetItem(CypherParser::SetItemContext * /*ctx*/) override { }
  virtual void exitSetItem(CypherParser::SetItemContext * /*ctx*/) override { }

  virtual void enterCypherDelete(CypherParser::CypherDeleteContext * /*ctx*/) override { }
  virtual void exitCypherDelete(CypherParser::CypherDeleteContext * /*ctx*/) override { }

  virtual void enterRemove(CypherParser::RemoveContext * /*ctx*/) override { }
  virtual void exitRemove(CypherParser::RemoveContext * /*ctx*/) override { }

  virtual void enterRemoveItem(CypherParser::RemoveItemContext * /*ctx*/) override { }
  virtual void exitRemoveItem(CypherParser::RemoveItemContext * /*ctx*/) override { }

  virtual void enterWith(CypherParser::WithContext * /*ctx*/) override { }
  virtual void exitWith(CypherParser::WithContext * /*ctx*/) override { }

  virtual void enterCypherReturn(CypherParser::CypherReturnContext * /*ctx*/) override { }
  virtual void exitCypherReturn(CypherParser::CypherReturnContext * /*ctx*/) override { }

  virtual void enterReturnBody(CypherParser::ReturnBodyContext * /*ctx*/) override { }
  virtual void exitReturnBody(CypherParser::ReturnBodyContext * /*ctx*/) override { }

  virtual void enterReturnItems(CypherParser::ReturnItemsContext * /*ctx*/) override { }
  virtual void exitReturnItems(CypherParser::ReturnItemsContext * /*ctx*/) override { }

  virtual void enterReturnItem(CypherParser::ReturnItemContext * /*ctx*/) override { }
  virtual void exitReturnItem(CypherParser::ReturnItemContext * /*ctx*/) override { }

  virtual void enterOrder(CypherParser::OrderContext * /*ctx*/) override { }
  virtual void exitOrder(CypherParser::OrderContext * /*ctx*/) override { }

  virtual void enterSkip(CypherParser::SkipContext * /*ctx*/) override { }
  virtual void exitSkip(CypherParser::SkipContext * /*ctx*/) override { }

  virtual void enterLimit(CypherParser::LimitContext * /*ctx*/) override { }
  virtual void exitLimit(CypherParser::LimitContext * /*ctx*/) override { }

  virtual void enterSortItem(CypherParser::SortItemContext * /*ctx*/) override { }
  virtual void exitSortItem(CypherParser::SortItemContext * /*ctx*/) override { }

  virtual void enterWhere(CypherParser::WhereContext * /*ctx*/) override { }
  virtual void exitWhere(CypherParser::WhereContext * /*ctx*/) override { }

  virtual void enterPattern(CypherParser::PatternContext * /*ctx*/) override { }
  virtual void exitPattern(CypherParser::PatternContext * /*ctx*/) override { }

  virtual void enterPatternPart(CypherParser::PatternPartContext * /*ctx*/) override { }
  virtual void exitPatternPart(CypherParser::PatternPartContext * /*ctx*/) override { }

  virtual void enterAnonymousPatternPart(CypherParser::AnonymousPatternPartContext * /*ctx*/) override { }
  virtual void exitAnonymousPatternPart(CypherParser::AnonymousPatternPartContext * /*ctx*/) override { }

  virtual void enterPatternElement(CypherParser::PatternElementContext * /*ctx*/) override { }
  virtual void exitPatternElement(CypherParser::PatternElementContext * /*ctx*/) override { }

  virtual void enterNodePattern(CypherParser::NodePatternContext * /*ctx*/) override { }
  virtual void exitNodePattern(CypherParser::NodePatternContext * /*ctx*/) override { }

  virtual void enterPatternElementChain(CypherParser::PatternElementChainContext * /*ctx*/) override { }
  virtual void exitPatternElementChain(CypherParser::PatternElementChainContext * /*ctx*/) override { }

  virtual void enterRelationshipPattern(CypherParser::RelationshipPatternContext * /*ctx*/) override { }
  virtual void exitRelationshipPattern(CypherParser::RelationshipPatternContext * /*ctx*/) override { }

  virtual void enterRelationshipDetail(CypherParser::RelationshipDetailContext * /*ctx*/) override { }
  virtual void exitRelationshipDetail(CypherParser::RelationshipDetailContext * /*ctx*/) override { }

  virtual void enterProperties(CypherParser::PropertiesContext * /*ctx*/) override { }
  virtual void exitProperties(CypherParser::PropertiesContext * /*ctx*/) override { }

  virtual void enterRelationshipTypes(CypherParser::RelationshipTypesContext * /*ctx*/) override { }
  virtual void exitRelationshipTypes(CypherParser::RelationshipTypesContext * /*ctx*/) override { }

  virtual void enterNodeLabels(CypherParser::NodeLabelsContext * /*ctx*/) override { }
  virtual void exitNodeLabels(CypherParser::NodeLabelsContext * /*ctx*/) override { }

  virtual void enterNodeLabel(CypherParser::NodeLabelContext * /*ctx*/) override { }
  virtual void exitNodeLabel(CypherParser::NodeLabelContext * /*ctx*/) override { }

  virtual void enterRangeLiteral(CypherParser::RangeLiteralContext * /*ctx*/) override { }
  virtual void exitRangeLiteral(CypherParser::RangeLiteralContext * /*ctx*/) override { }

  virtual void enterLabelName(CypherParser::LabelNameContext * /*ctx*/) override { }
  virtual void exitLabelName(CypherParser::LabelNameContext * /*ctx*/) override { }

  virtual void enterRelTypeName(CypherParser::RelTypeNameContext * /*ctx*/) override { }
  virtual void exitRelTypeName(CypherParser::RelTypeNameContext * /*ctx*/) override { }

  virtual void enterExpression(CypherParser::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(CypherParser::ExpressionContext * /*ctx*/) override { }

  virtual void enterExpression12(CypherParser::Expression12Context * /*ctx*/) override { }
  virtual void exitExpression12(CypherParser::Expression12Context * /*ctx*/) override { }

  virtual void enterExpression11(CypherParser::Expression11Context * /*ctx*/) override { }
  virtual void exitExpression11(CypherParser::Expression11Context * /*ctx*/) override { }

  virtual void enterExpression10(CypherParser::Expression10Context * /*ctx*/) override { }
  virtual void exitExpression10(CypherParser::Expression10Context * /*ctx*/) override { }

  virtual void enterExpression9(CypherParser::Expression9Context * /*ctx*/) override { }
  virtual void exitExpression9(CypherParser::Expression9Context * /*ctx*/) override { }

  virtual void enterExpression8(CypherParser::Expression8Context * /*ctx*/) override { }
  virtual void exitExpression8(CypherParser::Expression8Context * /*ctx*/) override { }

  virtual void enterExpression7(CypherParser::Expression7Context * /*ctx*/) override { }
  virtual void exitExpression7(CypherParser::Expression7Context * /*ctx*/) override { }

  virtual void enterExpression6(CypherParser::Expression6Context * /*ctx*/) override { }
  virtual void exitExpression6(CypherParser::Expression6Context * /*ctx*/) override { }

  virtual void enterExpression5(CypherParser::Expression5Context * /*ctx*/) override { }
  virtual void exitExpression5(CypherParser::Expression5Context * /*ctx*/) override { }

  virtual void enterExpression4(CypherParser::Expression4Context * /*ctx*/) override { }
  virtual void exitExpression4(CypherParser::Expression4Context * /*ctx*/) override { }

  virtual void enterExpression3(CypherParser::Expression3Context * /*ctx*/) override { }
  virtual void exitExpression3(CypherParser::Expression3Context * /*ctx*/) override { }

  virtual void enterExpression2(CypherParser::Expression2Context * /*ctx*/) override { }
  virtual void exitExpression2(CypherParser::Expression2Context * /*ctx*/) override { }

  virtual void enterAtom(CypherParser::AtomContext * /*ctx*/) override { }
  virtual void exitAtom(CypherParser::AtomContext * /*ctx*/) override { }

  virtual void enterLiteral(CypherParser::LiteralContext * /*ctx*/) override { }
  virtual void exitLiteral(CypherParser::LiteralContext * /*ctx*/) override { }

  virtual void enterBooleanLiteral(CypherParser::BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitBooleanLiteral(CypherParser::BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterListLiteral(CypherParser::ListLiteralContext * /*ctx*/) override { }
  virtual void exitListLiteral(CypherParser::ListLiteralContext * /*ctx*/) override { }

  virtual void enterPartialComparisonExpression(CypherParser::PartialComparisonExpressionContext * /*ctx*/) override { }
  virtual void exitPartialComparisonExpression(CypherParser::PartialComparisonExpressionContext * /*ctx*/) override { }

  virtual void enterParenthesizedExpression(CypherParser::ParenthesizedExpressionContext * /*ctx*/) override { }
  virtual void exitParenthesizedExpression(CypherParser::ParenthesizedExpressionContext * /*ctx*/) override { }

  virtual void enterRelationshipsPattern(CypherParser::RelationshipsPatternContext * /*ctx*/) override { }
  virtual void exitRelationshipsPattern(CypherParser::RelationshipsPatternContext * /*ctx*/) override { }

  virtual void enterFilterExpression(CypherParser::FilterExpressionContext * /*ctx*/) override { }
  virtual void exitFilterExpression(CypherParser::FilterExpressionContext * /*ctx*/) override { }

  virtual void enterIdInColl(CypherParser::IdInCollContext * /*ctx*/) override { }
  virtual void exitIdInColl(CypherParser::IdInCollContext * /*ctx*/) override { }

  virtual void enterFunctionInvocation(CypherParser::FunctionInvocationContext * /*ctx*/) override { }
  virtual void exitFunctionInvocation(CypherParser::FunctionInvocationContext * /*ctx*/) override { }

  virtual void enterFunctionName(CypherParser::FunctionNameContext * /*ctx*/) override { }
  virtual void exitFunctionName(CypherParser::FunctionNameContext * /*ctx*/) override { }

  virtual void enterListComprehension(CypherParser::ListComprehensionContext * /*ctx*/) override { }
  virtual void exitListComprehension(CypherParser::ListComprehensionContext * /*ctx*/) override { }

  virtual void enterPatternComprehension(CypherParser::PatternComprehensionContext * /*ctx*/) override { }
  virtual void exitPatternComprehension(CypherParser::PatternComprehensionContext * /*ctx*/) override { }

  virtual void enterPropertyLookup(CypherParser::PropertyLookupContext * /*ctx*/) override { }
  virtual void exitPropertyLookup(CypherParser::PropertyLookupContext * /*ctx*/) override { }

  virtual void enterVariable(CypherParser::VariableContext * /*ctx*/) override { }
  virtual void exitVariable(CypherParser::VariableContext * /*ctx*/) override { }

  virtual void enterNumberLiteral(CypherParser::NumberLiteralContext * /*ctx*/) override { }
  virtual void exitNumberLiteral(CypherParser::NumberLiteralContext * /*ctx*/) override { }

  virtual void enterMapLiteral(CypherParser::MapLiteralContext * /*ctx*/) override { }
  virtual void exitMapLiteral(CypherParser::MapLiteralContext * /*ctx*/) override { }

  virtual void enterParameter(CypherParser::ParameterContext * /*ctx*/) override { }
  virtual void exitParameter(CypherParser::ParameterContext * /*ctx*/) override { }

  virtual void enterPropertyExpression(CypherParser::PropertyExpressionContext * /*ctx*/) override { }
  virtual void exitPropertyExpression(CypherParser::PropertyExpressionContext * /*ctx*/) override { }

  virtual void enterPropertyKeyName(CypherParser::PropertyKeyNameContext * /*ctx*/) override { }
  virtual void exitPropertyKeyName(CypherParser::PropertyKeyNameContext * /*ctx*/) override { }

  virtual void enterIntegerLiteral(CypherParser::IntegerLiteralContext * /*ctx*/) override { }
  virtual void exitIntegerLiteral(CypherParser::IntegerLiteralContext * /*ctx*/) override { }

  virtual void enterDoubleLiteral(CypherParser::DoubleLiteralContext * /*ctx*/) override { }
  virtual void exitDoubleLiteral(CypherParser::DoubleLiteralContext * /*ctx*/) override { }

  virtual void enterSymbolicName(CypherParser::SymbolicNameContext * /*ctx*/) override { }
  virtual void exitSymbolicName(CypherParser::SymbolicNameContext * /*ctx*/) override { }

  virtual void enterLeftArrowHead(CypherParser::LeftArrowHeadContext * /*ctx*/) override { }
  virtual void exitLeftArrowHead(CypherParser::LeftArrowHeadContext * /*ctx*/) override { }

  virtual void enterRightArrowHead(CypherParser::RightArrowHeadContext * /*ctx*/) override { }
  virtual void exitRightArrowHead(CypherParser::RightArrowHeadContext * /*ctx*/) override { }

  virtual void enterDash(CypherParser::DashContext * /*ctx*/) override { }
  virtual void exitDash(CypherParser::DashContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

}  // namespace antlropencypher
