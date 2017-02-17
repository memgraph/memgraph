
// Generated from /home/buda/Workspace/code/memgraph/memgraph/src/query/frontend/opencypher/grammar/Cypher.g4 by ANTLR 4.6

#pragma once


#include "antlr4-runtime.h"
#include "CypherParser.h"


namespace antlrcpptest {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by CypherParser.
 */
class  CypherVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by CypherParser.
   */
    virtual antlrcpp::Any visitCypher(CypherParser::CypherContext *context) = 0;

    virtual antlrcpp::Any visitStatement(CypherParser::StatementContext *context) = 0;

    virtual antlrcpp::Any visitQuery(CypherParser::QueryContext *context) = 0;

    virtual antlrcpp::Any visitRegularQuery(CypherParser::RegularQueryContext *context) = 0;

    virtual antlrcpp::Any visitSingleQuery(CypherParser::SingleQueryContext *context) = 0;

    virtual antlrcpp::Any visitCypherUnion(CypherParser::CypherUnionContext *context) = 0;

    virtual antlrcpp::Any visitClause(CypherParser::ClauseContext *context) = 0;

    virtual antlrcpp::Any visitCypherMatch(CypherParser::CypherMatchContext *context) = 0;

    virtual antlrcpp::Any visitUnwind(CypherParser::UnwindContext *context) = 0;

    virtual antlrcpp::Any visitMerge(CypherParser::MergeContext *context) = 0;

    virtual antlrcpp::Any visitMergeAction(CypherParser::MergeActionContext *context) = 0;

    virtual antlrcpp::Any visitCreate(CypherParser::CreateContext *context) = 0;

    virtual antlrcpp::Any visitSet(CypherParser::SetContext *context) = 0;

    virtual antlrcpp::Any visitSetItem(CypherParser::SetItemContext *context) = 0;

    virtual antlrcpp::Any visitCypherDelete(CypherParser::CypherDeleteContext *context) = 0;

    virtual antlrcpp::Any visitRemove(CypherParser::RemoveContext *context) = 0;

    virtual antlrcpp::Any visitRemoveItem(CypherParser::RemoveItemContext *context) = 0;

    virtual antlrcpp::Any visitWith(CypherParser::WithContext *context) = 0;

    virtual antlrcpp::Any visitCypherReturn(CypherParser::CypherReturnContext *context) = 0;

    virtual antlrcpp::Any visitReturnBody(CypherParser::ReturnBodyContext *context) = 0;

    virtual antlrcpp::Any visitReturnItems(CypherParser::ReturnItemsContext *context) = 0;

    virtual antlrcpp::Any visitReturnItem(CypherParser::ReturnItemContext *context) = 0;

    virtual antlrcpp::Any visitOrder(CypherParser::OrderContext *context) = 0;

    virtual antlrcpp::Any visitSkip(CypherParser::SkipContext *context) = 0;

    virtual antlrcpp::Any visitLimit(CypherParser::LimitContext *context) = 0;

    virtual antlrcpp::Any visitSortItem(CypherParser::SortItemContext *context) = 0;

    virtual antlrcpp::Any visitWhere(CypherParser::WhereContext *context) = 0;

    virtual antlrcpp::Any visitPattern(CypherParser::PatternContext *context) = 0;

    virtual antlrcpp::Any visitPatternPart(CypherParser::PatternPartContext *context) = 0;

    virtual antlrcpp::Any visitAnonymousPatternPart(CypherParser::AnonymousPatternPartContext *context) = 0;

    virtual antlrcpp::Any visitPatternElement(CypherParser::PatternElementContext *context) = 0;

    virtual antlrcpp::Any visitNodePattern(CypherParser::NodePatternContext *context) = 0;

    virtual antlrcpp::Any visitPatternElementChain(CypherParser::PatternElementChainContext *context) = 0;

    virtual antlrcpp::Any visitRelationshipPattern(CypherParser::RelationshipPatternContext *context) = 0;

    virtual antlrcpp::Any visitRelationshipDetail(CypherParser::RelationshipDetailContext *context) = 0;

    virtual antlrcpp::Any visitProperties(CypherParser::PropertiesContext *context) = 0;

    virtual antlrcpp::Any visitRelationshipTypes(CypherParser::RelationshipTypesContext *context) = 0;

    virtual antlrcpp::Any visitNodeLabels(CypherParser::NodeLabelsContext *context) = 0;

    virtual antlrcpp::Any visitNodeLabel(CypherParser::NodeLabelContext *context) = 0;

    virtual antlrcpp::Any visitRangeLiteral(CypherParser::RangeLiteralContext *context) = 0;

    virtual antlrcpp::Any visitLabelName(CypherParser::LabelNameContext *context) = 0;

    virtual antlrcpp::Any visitRelTypeName(CypherParser::RelTypeNameContext *context) = 0;

    virtual antlrcpp::Any visitExpression(CypherParser::ExpressionContext *context) = 0;

    virtual antlrcpp::Any visitExpression12(CypherParser::Expression12Context *context) = 0;

    virtual antlrcpp::Any visitExpression11(CypherParser::Expression11Context *context) = 0;

    virtual antlrcpp::Any visitExpression10(CypherParser::Expression10Context *context) = 0;

    virtual antlrcpp::Any visitExpression9(CypherParser::Expression9Context *context) = 0;

    virtual antlrcpp::Any visitExpression8(CypherParser::Expression8Context *context) = 0;

    virtual antlrcpp::Any visitExpression7(CypherParser::Expression7Context *context) = 0;

    virtual antlrcpp::Any visitExpression6(CypherParser::Expression6Context *context) = 0;

    virtual antlrcpp::Any visitExpression5(CypherParser::Expression5Context *context) = 0;

    virtual antlrcpp::Any visitExpression4(CypherParser::Expression4Context *context) = 0;

    virtual antlrcpp::Any visitExpression3(CypherParser::Expression3Context *context) = 0;

    virtual antlrcpp::Any visitExpression2(CypherParser::Expression2Context *context) = 0;

    virtual antlrcpp::Any visitAtom(CypherParser::AtomContext *context) = 0;

    virtual antlrcpp::Any visitLiteral(CypherParser::LiteralContext *context) = 0;

    virtual antlrcpp::Any visitBooleanLiteral(CypherParser::BooleanLiteralContext *context) = 0;

    virtual antlrcpp::Any visitListLiteral(CypherParser::ListLiteralContext *context) = 0;

    virtual antlrcpp::Any visitPartialComparisonExpression(CypherParser::PartialComparisonExpressionContext *context) = 0;

    virtual antlrcpp::Any visitParenthesizedExpression(CypherParser::ParenthesizedExpressionContext *context) = 0;

    virtual antlrcpp::Any visitRelationshipsPattern(CypherParser::RelationshipsPatternContext *context) = 0;

    virtual antlrcpp::Any visitFilterExpression(CypherParser::FilterExpressionContext *context) = 0;

    virtual antlrcpp::Any visitIdInColl(CypherParser::IdInCollContext *context) = 0;

    virtual antlrcpp::Any visitFunctionInvocation(CypherParser::FunctionInvocationContext *context) = 0;

    virtual antlrcpp::Any visitFunctionName(CypherParser::FunctionNameContext *context) = 0;

    virtual antlrcpp::Any visitListComprehension(CypherParser::ListComprehensionContext *context) = 0;

    virtual antlrcpp::Any visitPropertyLookup(CypherParser::PropertyLookupContext *context) = 0;

    virtual antlrcpp::Any visitVariable(CypherParser::VariableContext *context) = 0;

    virtual antlrcpp::Any visitNumberLiteral(CypherParser::NumberLiteralContext *context) = 0;

    virtual antlrcpp::Any visitMapLiteral(CypherParser::MapLiteralContext *context) = 0;

    virtual antlrcpp::Any visitParameter(CypherParser::ParameterContext *context) = 0;

    virtual antlrcpp::Any visitPropertyExpression(CypherParser::PropertyExpressionContext *context) = 0;

    virtual antlrcpp::Any visitPropertyKeyName(CypherParser::PropertyKeyNameContext *context) = 0;

    virtual antlrcpp::Any visitIntegerLiteral(CypherParser::IntegerLiteralContext *context) = 0;

    virtual antlrcpp::Any visitDoubleLiteral(CypherParser::DoubleLiteralContext *context) = 0;

    virtual antlrcpp::Any visitSymbolicName(CypherParser::SymbolicNameContext *context) = 0;

    virtual antlrcpp::Any visitLeftArrowHead(CypherParser::LeftArrowHeadContext *context) = 0;

    virtual antlrcpp::Any visitRightArrowHead(CypherParser::RightArrowHeadContext *context) = 0;

    virtual antlrcpp::Any visitDash(CypherParser::DashContext *context) = 0;


};

}  // namespace antlrcpptest
