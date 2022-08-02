
// Generated from /home/kostas/Desktop/memgraph/src/parser/opencypher/grammar/MemgraphCypher.g4 by ANTLR 4.10.1

#pragma once


#include "antlr4-runtime.h"
#include "MemgraphCypherVisitor.h"


namespace antlropencypher {

/**
 * This class provides an empty implementation of MemgraphCypherVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  MemgraphCypherBaseVisitor : public MemgraphCypherVisitor {
public:

  virtual std::any visitMemgraphCypherKeyword(MemgraphCypher::MemgraphCypherKeywordContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSymbolicName(MemgraphCypher::SymbolicNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuery(MemgraphCypher::QueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAuthQuery(MemgraphCypher::AuthQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReplicationQuery(MemgraphCypher::ReplicationQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriggerQuery(MemgraphCypher::TriggerQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitClause(MemgraphCypher::ClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUpdateClause(MemgraphCypher::UpdateClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitForeach(MemgraphCypher::ForeachContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStreamQuery(MemgraphCypher::StreamQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSettingQuery(MemgraphCypher::SettingQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLoadCsv(MemgraphCypher::LoadCsvContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCsvFile(MemgraphCypher::CsvFileContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDelimiter(MemgraphCypher::DelimiterContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQuote(MemgraphCypher::QuoteContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRowVar(MemgraphCypher::RowVarContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateRole(MemgraphCypher::CreateRoleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropRole(MemgraphCypher::DropRoleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowRoles(MemgraphCypher::ShowRolesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateUser(MemgraphCypher::CreateUserContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetPassword(MemgraphCypher::SetPasswordContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropUser(MemgraphCypher::DropUserContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowUsers(MemgraphCypher::ShowUsersContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetRole(MemgraphCypher::SetRoleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitClearRole(MemgraphCypher::ClearRoleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDenyPrivilege(MemgraphCypher::DenyPrivilegeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrivilege(MemgraphCypher::PrivilegeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPrivilegeList(MemgraphCypher::PrivilegeListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowPrivileges(MemgraphCypher::ShowPrivilegesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDumpQuery(MemgraphCypher::DumpQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReplicaName(MemgraphCypher::ReplicaNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSocketAddress(MemgraphCypher::SocketAddressContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRegisterReplica(MemgraphCypher::RegisterReplicaContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropReplica(MemgraphCypher::DropReplicaContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowReplicas(MemgraphCypher::ShowReplicasContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLockPathQuery(MemgraphCypher::LockPathQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriggerName(MemgraphCypher::TriggerNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTriggerStatement(MemgraphCypher::TriggerStatementContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitEmptyVertex(MemgraphCypher::EmptyVertexContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitEmptyEdge(MemgraphCypher::EmptyEdgeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateTrigger(MemgraphCypher::CreateTriggerContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropTrigger(MemgraphCypher::DropTriggerContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowTriggers(MemgraphCypher::ShowTriggersContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIsolationLevel(MemgraphCypher::IsolationLevelContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIsolationLevelScope(MemgraphCypher::IsolationLevelScopeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStreamName(MemgraphCypher::StreamNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSymbolicNameWithMinus(MemgraphCypher::SymbolicNameWithMinusContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSymbolicNameWithDotsAndMinus(MemgraphCypher::SymbolicNameWithDotsAndMinusContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSymbolicTopicNames(MemgraphCypher::SymbolicTopicNamesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitTopicNames(MemgraphCypher::TopicNamesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateStream(MemgraphCypher::CreateStreamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConfigMap(MemgraphCypher::ConfigMapContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropStream(MemgraphCypher::DropStreamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStartStream(MemgraphCypher::StartStreamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStartAllStreams(MemgraphCypher::StartAllStreamsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStopStream(MemgraphCypher::StopStreamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStopAllStreams(MemgraphCypher::StopAllStreamsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowStreams(MemgraphCypher::ShowStreamsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCheckStream(MemgraphCypher::CheckStreamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSettingName(MemgraphCypher::SettingNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSettingValue(MemgraphCypher::SettingValueContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetSetting(MemgraphCypher::SetSettingContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowSetting(MemgraphCypher::ShowSettingContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitShowSettings(MemgraphCypher::ShowSettingsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVersionQuery(MemgraphCypher::VersionQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCypher(MemgraphCypher::CypherContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStatement(MemgraphCypher::StatementContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintQuery(MemgraphCypher::ConstraintQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraint(MemgraphCypher::ConstraintContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintPropertyList(MemgraphCypher::ConstraintPropertyListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStorageInfo(MemgraphCypher::StorageInfoContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIndexInfo(MemgraphCypher::IndexInfoContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitConstraintInfo(MemgraphCypher::ConstraintInfoContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitInfoQuery(MemgraphCypher::InfoQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExplainQuery(MemgraphCypher::ExplainQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProfileQuery(MemgraphCypher::ProfileQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCypherQuery(MemgraphCypher::CypherQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIndexQuery(MemgraphCypher::IndexQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSingleQuery(MemgraphCypher::SingleQueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCypherUnion(MemgraphCypher::CypherUnionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCypherMatch(MemgraphCypher::CypherMatchContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitUnwind(MemgraphCypher::UnwindContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMerge(MemgraphCypher::MergeContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMergeAction(MemgraphCypher::MergeActionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreate(MemgraphCypher::CreateContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSet(MemgraphCypher::SetContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSetItem(MemgraphCypher::SetItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCypherDelete(MemgraphCypher::CypherDeleteContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRemove(MemgraphCypher::RemoveContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRemoveItem(MemgraphCypher::RemoveItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWith(MemgraphCypher::WithContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCypherReturn(MemgraphCypher::CypherReturnContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCallProcedure(MemgraphCypher::CallProcedureContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProcedureName(MemgraphCypher::ProcedureNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitYieldProcedureResults(MemgraphCypher::YieldProcedureResultsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMemoryLimit(MemgraphCypher::MemoryLimitContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitQueryMemoryLimit(MemgraphCypher::QueryMemoryLimitContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProcedureMemoryLimit(MemgraphCypher::ProcedureMemoryLimitContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProcedureResult(MemgraphCypher::ProcedureResultContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReturnBody(MemgraphCypher::ReturnBodyContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReturnItems(MemgraphCypher::ReturnItemsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReturnItem(MemgraphCypher::ReturnItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitOrder(MemgraphCypher::OrderContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSkip(MemgraphCypher::SkipContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLimit(MemgraphCypher::LimitContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitSortItem(MemgraphCypher::SortItemContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitWhere(MemgraphCypher::WhereContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPattern(MemgraphCypher::PatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPatternPart(MemgraphCypher::PatternPartContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAnonymousPatternPart(MemgraphCypher::AnonymousPatternPartContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPatternElement(MemgraphCypher::PatternElementContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNodePattern(MemgraphCypher::NodePatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPatternElementChain(MemgraphCypher::PatternElementChainContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationshipPattern(MemgraphCypher::RelationshipPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLeftArrowHead(MemgraphCypher::LeftArrowHeadContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRightArrowHead(MemgraphCypher::RightArrowHeadContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDash(MemgraphCypher::DashContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationshipDetail(MemgraphCypher::RelationshipDetailContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationshipLambda(MemgraphCypher::RelationshipLambdaContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVariableExpansion(MemgraphCypher::VariableExpansionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitProperties(MemgraphCypher::PropertiesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationshipTypes(MemgraphCypher::RelationshipTypesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNodeLabels(MemgraphCypher::NodeLabelsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNodeLabel(MemgraphCypher::NodeLabelContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLabelName(MemgraphCypher::LabelNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelTypeName(MemgraphCypher::RelTypeNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression(MemgraphCypher::ExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression12(MemgraphCypher::Expression12Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression11(MemgraphCypher::Expression11Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression10(MemgraphCypher::Expression10Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression9(MemgraphCypher::Expression9Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression8(MemgraphCypher::Expression8Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression7(MemgraphCypher::Expression7Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression6(MemgraphCypher::Expression6Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression5(MemgraphCypher::Expression5Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression4(MemgraphCypher::Expression4Context *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression3a(MemgraphCypher::Expression3aContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression3b(MemgraphCypher::Expression3bContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression2a(MemgraphCypher::Expression2aContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExpression2b(MemgraphCypher::Expression2bContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitAtom(MemgraphCypher::AtomContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitLiteral(MemgraphCypher::LiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitBooleanLiteral(MemgraphCypher::BooleanLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitListLiteral(MemgraphCypher::ListLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPartialComparisonExpression(MemgraphCypher::PartialComparisonExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitRelationshipsPattern(MemgraphCypher::RelationshipsPatternContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFilterExpression(MemgraphCypher::FilterExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitReduceExpression(MemgraphCypher::ReduceExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitExtractExpression(MemgraphCypher::ExtractExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIdInColl(MemgraphCypher::IdInCollContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionInvocation(MemgraphCypher::FunctionInvocationContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitFunctionName(MemgraphCypher::FunctionNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitListComprehension(MemgraphCypher::ListComprehensionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPatternComprehension(MemgraphCypher::PatternComprehensionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyLookup(MemgraphCypher::PropertyLookupContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCaseExpression(MemgraphCypher::CaseExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCaseAlternatives(MemgraphCypher::CaseAlternativesContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitVariable(MemgraphCypher::VariableContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitNumberLiteral(MemgraphCypher::NumberLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitMapLiteral(MemgraphCypher::MapLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitParameter(MemgraphCypher::ParameterContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyExpression(MemgraphCypher::PropertyExpressionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitIntegerLiteral(MemgraphCypher::IntegerLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCreateIndex(MemgraphCypher::CreateIndexContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDropIndex(MemgraphCypher::DropIndexContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitDoubleLiteral(MemgraphCypher::DoubleLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual std::any visitCypherKeyword(MemgraphCypher::CypherKeywordContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace antlropencypher
