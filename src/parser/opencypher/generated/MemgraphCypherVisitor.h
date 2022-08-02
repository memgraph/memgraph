
// Generated from /home/kostas/Desktop/memgraph/src/parser/opencypher/grammar/MemgraphCypher.g4 by ANTLR 4.10.1

#pragma once


#include "antlr4-runtime.h"
#include "MemgraphCypher.h"


namespace antlropencypher {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by MemgraphCypher.
 */
class  MemgraphCypherVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by MemgraphCypher.
   */
    virtual std::any visitMemgraphCypherKeyword(MemgraphCypher::MemgraphCypherKeywordContext *context) = 0;

    virtual std::any visitSymbolicName(MemgraphCypher::SymbolicNameContext *context) = 0;

    virtual std::any visitQuery(MemgraphCypher::QueryContext *context) = 0;

    virtual std::any visitAuthQuery(MemgraphCypher::AuthQueryContext *context) = 0;

    virtual std::any visitReplicationQuery(MemgraphCypher::ReplicationQueryContext *context) = 0;

    virtual std::any visitTriggerQuery(MemgraphCypher::TriggerQueryContext *context) = 0;

    virtual std::any visitClause(MemgraphCypher::ClauseContext *context) = 0;

    virtual std::any visitUpdateClause(MemgraphCypher::UpdateClauseContext *context) = 0;

    virtual std::any visitForeach(MemgraphCypher::ForeachContext *context) = 0;

    virtual std::any visitStreamQuery(MemgraphCypher::StreamQueryContext *context) = 0;

    virtual std::any visitSettingQuery(MemgraphCypher::SettingQueryContext *context) = 0;

    virtual std::any visitLoadCsv(MemgraphCypher::LoadCsvContext *context) = 0;

    virtual std::any visitCsvFile(MemgraphCypher::CsvFileContext *context) = 0;

    virtual std::any visitDelimiter(MemgraphCypher::DelimiterContext *context) = 0;

    virtual std::any visitQuote(MemgraphCypher::QuoteContext *context) = 0;

    virtual std::any visitRowVar(MemgraphCypher::RowVarContext *context) = 0;

    virtual std::any visitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *context) = 0;

    virtual std::any visitCreateRole(MemgraphCypher::CreateRoleContext *context) = 0;

    virtual std::any visitDropRole(MemgraphCypher::DropRoleContext *context) = 0;

    virtual std::any visitShowRoles(MemgraphCypher::ShowRolesContext *context) = 0;

    virtual std::any visitCreateUser(MemgraphCypher::CreateUserContext *context) = 0;

    virtual std::any visitSetPassword(MemgraphCypher::SetPasswordContext *context) = 0;

    virtual std::any visitDropUser(MemgraphCypher::DropUserContext *context) = 0;

    virtual std::any visitShowUsers(MemgraphCypher::ShowUsersContext *context) = 0;

    virtual std::any visitSetRole(MemgraphCypher::SetRoleContext *context) = 0;

    virtual std::any visitClearRole(MemgraphCypher::ClearRoleContext *context) = 0;

    virtual std::any visitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *context) = 0;

    virtual std::any visitDenyPrivilege(MemgraphCypher::DenyPrivilegeContext *context) = 0;

    virtual std::any visitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *context) = 0;

    virtual std::any visitPrivilege(MemgraphCypher::PrivilegeContext *context) = 0;

    virtual std::any visitPrivilegeList(MemgraphCypher::PrivilegeListContext *context) = 0;

    virtual std::any visitShowPrivileges(MemgraphCypher::ShowPrivilegesContext *context) = 0;

    virtual std::any visitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *context) = 0;

    virtual std::any visitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *context) = 0;

    virtual std::any visitDumpQuery(MemgraphCypher::DumpQueryContext *context) = 0;

    virtual std::any visitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *context) = 0;

    virtual std::any visitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext *context) = 0;

    virtual std::any visitReplicaName(MemgraphCypher::ReplicaNameContext *context) = 0;

    virtual std::any visitSocketAddress(MemgraphCypher::SocketAddressContext *context) = 0;

    virtual std::any visitRegisterReplica(MemgraphCypher::RegisterReplicaContext *context) = 0;

    virtual std::any visitDropReplica(MemgraphCypher::DropReplicaContext *context) = 0;

    virtual std::any visitShowReplicas(MemgraphCypher::ShowReplicasContext *context) = 0;

    virtual std::any visitLockPathQuery(MemgraphCypher::LockPathQueryContext *context) = 0;

    virtual std::any visitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext *context) = 0;

    virtual std::any visitTriggerName(MemgraphCypher::TriggerNameContext *context) = 0;

    virtual std::any visitTriggerStatement(MemgraphCypher::TriggerStatementContext *context) = 0;

    virtual std::any visitEmptyVertex(MemgraphCypher::EmptyVertexContext *context) = 0;

    virtual std::any visitEmptyEdge(MemgraphCypher::EmptyEdgeContext *context) = 0;

    virtual std::any visitCreateTrigger(MemgraphCypher::CreateTriggerContext *context) = 0;

    virtual std::any visitDropTrigger(MemgraphCypher::DropTriggerContext *context) = 0;

    virtual std::any visitShowTriggers(MemgraphCypher::ShowTriggersContext *context) = 0;

    virtual std::any visitIsolationLevel(MemgraphCypher::IsolationLevelContext *context) = 0;

    virtual std::any visitIsolationLevelScope(MemgraphCypher::IsolationLevelScopeContext *context) = 0;

    virtual std::any visitIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext *context) = 0;

    virtual std::any visitCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext *context) = 0;

    virtual std::any visitStreamName(MemgraphCypher::StreamNameContext *context) = 0;

    virtual std::any visitSymbolicNameWithMinus(MemgraphCypher::SymbolicNameWithMinusContext *context) = 0;

    virtual std::any visitSymbolicNameWithDotsAndMinus(MemgraphCypher::SymbolicNameWithDotsAndMinusContext *context) = 0;

    virtual std::any visitSymbolicTopicNames(MemgraphCypher::SymbolicTopicNamesContext *context) = 0;

    virtual std::any visitTopicNames(MemgraphCypher::TopicNamesContext *context) = 0;

    virtual std::any visitCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext *context) = 0;

    virtual std::any visitCreateStream(MemgraphCypher::CreateStreamContext *context) = 0;

    virtual std::any visitConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext *context) = 0;

    virtual std::any visitConfigMap(MemgraphCypher::ConfigMapContext *context) = 0;

    virtual std::any visitKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext *context) = 0;

    virtual std::any visitKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext *context) = 0;

    virtual std::any visitPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext *context) = 0;

    virtual std::any visitPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext *context) = 0;

    virtual std::any visitDropStream(MemgraphCypher::DropStreamContext *context) = 0;

    virtual std::any visitStartStream(MemgraphCypher::StartStreamContext *context) = 0;

    virtual std::any visitStartAllStreams(MemgraphCypher::StartAllStreamsContext *context) = 0;

    virtual std::any visitStopStream(MemgraphCypher::StopStreamContext *context) = 0;

    virtual std::any visitStopAllStreams(MemgraphCypher::StopAllStreamsContext *context) = 0;

    virtual std::any visitShowStreams(MemgraphCypher::ShowStreamsContext *context) = 0;

    virtual std::any visitCheckStream(MemgraphCypher::CheckStreamContext *context) = 0;

    virtual std::any visitSettingName(MemgraphCypher::SettingNameContext *context) = 0;

    virtual std::any visitSettingValue(MemgraphCypher::SettingValueContext *context) = 0;

    virtual std::any visitSetSetting(MemgraphCypher::SetSettingContext *context) = 0;

    virtual std::any visitShowSetting(MemgraphCypher::ShowSettingContext *context) = 0;

    virtual std::any visitShowSettings(MemgraphCypher::ShowSettingsContext *context) = 0;

    virtual std::any visitVersionQuery(MemgraphCypher::VersionQueryContext *context) = 0;

    virtual std::any visitCypher(MemgraphCypher::CypherContext *context) = 0;

    virtual std::any visitStatement(MemgraphCypher::StatementContext *context) = 0;

    virtual std::any visitConstraintQuery(MemgraphCypher::ConstraintQueryContext *context) = 0;

    virtual std::any visitConstraint(MemgraphCypher::ConstraintContext *context) = 0;

    virtual std::any visitConstraintPropertyList(MemgraphCypher::ConstraintPropertyListContext *context) = 0;

    virtual std::any visitStorageInfo(MemgraphCypher::StorageInfoContext *context) = 0;

    virtual std::any visitIndexInfo(MemgraphCypher::IndexInfoContext *context) = 0;

    virtual std::any visitConstraintInfo(MemgraphCypher::ConstraintInfoContext *context) = 0;

    virtual std::any visitInfoQuery(MemgraphCypher::InfoQueryContext *context) = 0;

    virtual std::any visitExplainQuery(MemgraphCypher::ExplainQueryContext *context) = 0;

    virtual std::any visitProfileQuery(MemgraphCypher::ProfileQueryContext *context) = 0;

    virtual std::any visitCypherQuery(MemgraphCypher::CypherQueryContext *context) = 0;

    virtual std::any visitIndexQuery(MemgraphCypher::IndexQueryContext *context) = 0;

    virtual std::any visitSingleQuery(MemgraphCypher::SingleQueryContext *context) = 0;

    virtual std::any visitCypherUnion(MemgraphCypher::CypherUnionContext *context) = 0;

    virtual std::any visitCypherMatch(MemgraphCypher::CypherMatchContext *context) = 0;

    virtual std::any visitUnwind(MemgraphCypher::UnwindContext *context) = 0;

    virtual std::any visitMerge(MemgraphCypher::MergeContext *context) = 0;

    virtual std::any visitMergeAction(MemgraphCypher::MergeActionContext *context) = 0;

    virtual std::any visitCreate(MemgraphCypher::CreateContext *context) = 0;

    virtual std::any visitSet(MemgraphCypher::SetContext *context) = 0;

    virtual std::any visitSetItem(MemgraphCypher::SetItemContext *context) = 0;

    virtual std::any visitCypherDelete(MemgraphCypher::CypherDeleteContext *context) = 0;

    virtual std::any visitRemove(MemgraphCypher::RemoveContext *context) = 0;

    virtual std::any visitRemoveItem(MemgraphCypher::RemoveItemContext *context) = 0;

    virtual std::any visitWith(MemgraphCypher::WithContext *context) = 0;

    virtual std::any visitCypherReturn(MemgraphCypher::CypherReturnContext *context) = 0;

    virtual std::any visitCallProcedure(MemgraphCypher::CallProcedureContext *context) = 0;

    virtual std::any visitProcedureName(MemgraphCypher::ProcedureNameContext *context) = 0;

    virtual std::any visitYieldProcedureResults(MemgraphCypher::YieldProcedureResultsContext *context) = 0;

    virtual std::any visitMemoryLimit(MemgraphCypher::MemoryLimitContext *context) = 0;

    virtual std::any visitQueryMemoryLimit(MemgraphCypher::QueryMemoryLimitContext *context) = 0;

    virtual std::any visitProcedureMemoryLimit(MemgraphCypher::ProcedureMemoryLimitContext *context) = 0;

    virtual std::any visitProcedureResult(MemgraphCypher::ProcedureResultContext *context) = 0;

    virtual std::any visitReturnBody(MemgraphCypher::ReturnBodyContext *context) = 0;

    virtual std::any visitReturnItems(MemgraphCypher::ReturnItemsContext *context) = 0;

    virtual std::any visitReturnItem(MemgraphCypher::ReturnItemContext *context) = 0;

    virtual std::any visitOrder(MemgraphCypher::OrderContext *context) = 0;

    virtual std::any visitSkip(MemgraphCypher::SkipContext *context) = 0;

    virtual std::any visitLimit(MemgraphCypher::LimitContext *context) = 0;

    virtual std::any visitSortItem(MemgraphCypher::SortItemContext *context) = 0;

    virtual std::any visitWhere(MemgraphCypher::WhereContext *context) = 0;

    virtual std::any visitPattern(MemgraphCypher::PatternContext *context) = 0;

    virtual std::any visitPatternPart(MemgraphCypher::PatternPartContext *context) = 0;

    virtual std::any visitAnonymousPatternPart(MemgraphCypher::AnonymousPatternPartContext *context) = 0;

    virtual std::any visitPatternElement(MemgraphCypher::PatternElementContext *context) = 0;

    virtual std::any visitNodePattern(MemgraphCypher::NodePatternContext *context) = 0;

    virtual std::any visitPatternElementChain(MemgraphCypher::PatternElementChainContext *context) = 0;

    virtual std::any visitRelationshipPattern(MemgraphCypher::RelationshipPatternContext *context) = 0;

    virtual std::any visitLeftArrowHead(MemgraphCypher::LeftArrowHeadContext *context) = 0;

    virtual std::any visitRightArrowHead(MemgraphCypher::RightArrowHeadContext *context) = 0;

    virtual std::any visitDash(MemgraphCypher::DashContext *context) = 0;

    virtual std::any visitRelationshipDetail(MemgraphCypher::RelationshipDetailContext *context) = 0;

    virtual std::any visitRelationshipLambda(MemgraphCypher::RelationshipLambdaContext *context) = 0;

    virtual std::any visitVariableExpansion(MemgraphCypher::VariableExpansionContext *context) = 0;

    virtual std::any visitProperties(MemgraphCypher::PropertiesContext *context) = 0;

    virtual std::any visitRelationshipTypes(MemgraphCypher::RelationshipTypesContext *context) = 0;

    virtual std::any visitNodeLabels(MemgraphCypher::NodeLabelsContext *context) = 0;

    virtual std::any visitNodeLabel(MemgraphCypher::NodeLabelContext *context) = 0;

    virtual std::any visitLabelName(MemgraphCypher::LabelNameContext *context) = 0;

    virtual std::any visitRelTypeName(MemgraphCypher::RelTypeNameContext *context) = 0;

    virtual std::any visitExpression(MemgraphCypher::ExpressionContext *context) = 0;

    virtual std::any visitExpression12(MemgraphCypher::Expression12Context *context) = 0;

    virtual std::any visitExpression11(MemgraphCypher::Expression11Context *context) = 0;

    virtual std::any visitExpression10(MemgraphCypher::Expression10Context *context) = 0;

    virtual std::any visitExpression9(MemgraphCypher::Expression9Context *context) = 0;

    virtual std::any visitExpression8(MemgraphCypher::Expression8Context *context) = 0;

    virtual std::any visitExpression7(MemgraphCypher::Expression7Context *context) = 0;

    virtual std::any visitExpression6(MemgraphCypher::Expression6Context *context) = 0;

    virtual std::any visitExpression5(MemgraphCypher::Expression5Context *context) = 0;

    virtual std::any visitExpression4(MemgraphCypher::Expression4Context *context) = 0;

    virtual std::any visitExpression3a(MemgraphCypher::Expression3aContext *context) = 0;

    virtual std::any visitStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext *context) = 0;

    virtual std::any visitExpression3b(MemgraphCypher::Expression3bContext *context) = 0;

    virtual std::any visitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext *context) = 0;

    virtual std::any visitExpression2a(MemgraphCypher::Expression2aContext *context) = 0;

    virtual std::any visitExpression2b(MemgraphCypher::Expression2bContext *context) = 0;

    virtual std::any visitAtom(MemgraphCypher::AtomContext *context) = 0;

    virtual std::any visitLiteral(MemgraphCypher::LiteralContext *context) = 0;

    virtual std::any visitBooleanLiteral(MemgraphCypher::BooleanLiteralContext *context) = 0;

    virtual std::any visitListLiteral(MemgraphCypher::ListLiteralContext *context) = 0;

    virtual std::any visitPartialComparisonExpression(MemgraphCypher::PartialComparisonExpressionContext *context) = 0;

    virtual std::any visitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *context) = 0;

    virtual std::any visitRelationshipsPattern(MemgraphCypher::RelationshipsPatternContext *context) = 0;

    virtual std::any visitFilterExpression(MemgraphCypher::FilterExpressionContext *context) = 0;

    virtual std::any visitReduceExpression(MemgraphCypher::ReduceExpressionContext *context) = 0;

    virtual std::any visitExtractExpression(MemgraphCypher::ExtractExpressionContext *context) = 0;

    virtual std::any visitIdInColl(MemgraphCypher::IdInCollContext *context) = 0;

    virtual std::any visitFunctionInvocation(MemgraphCypher::FunctionInvocationContext *context) = 0;

    virtual std::any visitFunctionName(MemgraphCypher::FunctionNameContext *context) = 0;

    virtual std::any visitListComprehension(MemgraphCypher::ListComprehensionContext *context) = 0;

    virtual std::any visitPatternComprehension(MemgraphCypher::PatternComprehensionContext *context) = 0;

    virtual std::any visitPropertyLookup(MemgraphCypher::PropertyLookupContext *context) = 0;

    virtual std::any visitCaseExpression(MemgraphCypher::CaseExpressionContext *context) = 0;

    virtual std::any visitCaseAlternatives(MemgraphCypher::CaseAlternativesContext *context) = 0;

    virtual std::any visitVariable(MemgraphCypher::VariableContext *context) = 0;

    virtual std::any visitNumberLiteral(MemgraphCypher::NumberLiteralContext *context) = 0;

    virtual std::any visitMapLiteral(MemgraphCypher::MapLiteralContext *context) = 0;

    virtual std::any visitParameter(MemgraphCypher::ParameterContext *context) = 0;

    virtual std::any visitPropertyExpression(MemgraphCypher::PropertyExpressionContext *context) = 0;

    virtual std::any visitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *context) = 0;

    virtual std::any visitIntegerLiteral(MemgraphCypher::IntegerLiteralContext *context) = 0;

    virtual std::any visitCreateIndex(MemgraphCypher::CreateIndexContext *context) = 0;

    virtual std::any visitDropIndex(MemgraphCypher::DropIndexContext *context) = 0;

    virtual std::any visitDoubleLiteral(MemgraphCypher::DoubleLiteralContext *context) = 0;

    virtual std::any visitCypherKeyword(MemgraphCypher::CypherKeywordContext *context) = 0;


};

}  // namespace antlropencypher
