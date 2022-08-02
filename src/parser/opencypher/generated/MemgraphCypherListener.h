
// Generated from /home/kostas/Desktop/memgraph/src/parser/opencypher/grammar/MemgraphCypher.g4 by ANTLR 4.10.1

#pragma once


#include "antlr4-runtime.h"
#include "MemgraphCypher.h"


namespace antlropencypher {

/**
 * This interface defines an abstract listener for a parse tree produced by MemgraphCypher.
 */
class  MemgraphCypherListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterMemgraphCypherKeyword(MemgraphCypher::MemgraphCypherKeywordContext *ctx) = 0;
  virtual void exitMemgraphCypherKeyword(MemgraphCypher::MemgraphCypherKeywordContext *ctx) = 0;

  virtual void enterSymbolicName(MemgraphCypher::SymbolicNameContext *ctx) = 0;
  virtual void exitSymbolicName(MemgraphCypher::SymbolicNameContext *ctx) = 0;

  virtual void enterQuery(MemgraphCypher::QueryContext *ctx) = 0;
  virtual void exitQuery(MemgraphCypher::QueryContext *ctx) = 0;

  virtual void enterAuthQuery(MemgraphCypher::AuthQueryContext *ctx) = 0;
  virtual void exitAuthQuery(MemgraphCypher::AuthQueryContext *ctx) = 0;

  virtual void enterReplicationQuery(MemgraphCypher::ReplicationQueryContext *ctx) = 0;
  virtual void exitReplicationQuery(MemgraphCypher::ReplicationQueryContext *ctx) = 0;

  virtual void enterTriggerQuery(MemgraphCypher::TriggerQueryContext *ctx) = 0;
  virtual void exitTriggerQuery(MemgraphCypher::TriggerQueryContext *ctx) = 0;

  virtual void enterClause(MemgraphCypher::ClauseContext *ctx) = 0;
  virtual void exitClause(MemgraphCypher::ClauseContext *ctx) = 0;

  virtual void enterUpdateClause(MemgraphCypher::UpdateClauseContext *ctx) = 0;
  virtual void exitUpdateClause(MemgraphCypher::UpdateClauseContext *ctx) = 0;

  virtual void enterForeach(MemgraphCypher::ForeachContext *ctx) = 0;
  virtual void exitForeach(MemgraphCypher::ForeachContext *ctx) = 0;

  virtual void enterStreamQuery(MemgraphCypher::StreamQueryContext *ctx) = 0;
  virtual void exitStreamQuery(MemgraphCypher::StreamQueryContext *ctx) = 0;

  virtual void enterSettingQuery(MemgraphCypher::SettingQueryContext *ctx) = 0;
  virtual void exitSettingQuery(MemgraphCypher::SettingQueryContext *ctx) = 0;

  virtual void enterLoadCsv(MemgraphCypher::LoadCsvContext *ctx) = 0;
  virtual void exitLoadCsv(MemgraphCypher::LoadCsvContext *ctx) = 0;

  virtual void enterCsvFile(MemgraphCypher::CsvFileContext *ctx) = 0;
  virtual void exitCsvFile(MemgraphCypher::CsvFileContext *ctx) = 0;

  virtual void enterDelimiter(MemgraphCypher::DelimiterContext *ctx) = 0;
  virtual void exitDelimiter(MemgraphCypher::DelimiterContext *ctx) = 0;

  virtual void enterQuote(MemgraphCypher::QuoteContext *ctx) = 0;
  virtual void exitQuote(MemgraphCypher::QuoteContext *ctx) = 0;

  virtual void enterRowVar(MemgraphCypher::RowVarContext *ctx) = 0;
  virtual void exitRowVar(MemgraphCypher::RowVarContext *ctx) = 0;

  virtual void enterUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *ctx) = 0;
  virtual void exitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext *ctx) = 0;

  virtual void enterCreateRole(MemgraphCypher::CreateRoleContext *ctx) = 0;
  virtual void exitCreateRole(MemgraphCypher::CreateRoleContext *ctx) = 0;

  virtual void enterDropRole(MemgraphCypher::DropRoleContext *ctx) = 0;
  virtual void exitDropRole(MemgraphCypher::DropRoleContext *ctx) = 0;

  virtual void enterShowRoles(MemgraphCypher::ShowRolesContext *ctx) = 0;
  virtual void exitShowRoles(MemgraphCypher::ShowRolesContext *ctx) = 0;

  virtual void enterCreateUser(MemgraphCypher::CreateUserContext *ctx) = 0;
  virtual void exitCreateUser(MemgraphCypher::CreateUserContext *ctx) = 0;

  virtual void enterSetPassword(MemgraphCypher::SetPasswordContext *ctx) = 0;
  virtual void exitSetPassword(MemgraphCypher::SetPasswordContext *ctx) = 0;

  virtual void enterDropUser(MemgraphCypher::DropUserContext *ctx) = 0;
  virtual void exitDropUser(MemgraphCypher::DropUserContext *ctx) = 0;

  virtual void enterShowUsers(MemgraphCypher::ShowUsersContext *ctx) = 0;
  virtual void exitShowUsers(MemgraphCypher::ShowUsersContext *ctx) = 0;

  virtual void enterSetRole(MemgraphCypher::SetRoleContext *ctx) = 0;
  virtual void exitSetRole(MemgraphCypher::SetRoleContext *ctx) = 0;

  virtual void enterClearRole(MemgraphCypher::ClearRoleContext *ctx) = 0;
  virtual void exitClearRole(MemgraphCypher::ClearRoleContext *ctx) = 0;

  virtual void enterGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *ctx) = 0;
  virtual void exitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext *ctx) = 0;

  virtual void enterDenyPrivilege(MemgraphCypher::DenyPrivilegeContext *ctx) = 0;
  virtual void exitDenyPrivilege(MemgraphCypher::DenyPrivilegeContext *ctx) = 0;

  virtual void enterRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *ctx) = 0;
  virtual void exitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext *ctx) = 0;

  virtual void enterPrivilege(MemgraphCypher::PrivilegeContext *ctx) = 0;
  virtual void exitPrivilege(MemgraphCypher::PrivilegeContext *ctx) = 0;

  virtual void enterPrivilegeList(MemgraphCypher::PrivilegeListContext *ctx) = 0;
  virtual void exitPrivilegeList(MemgraphCypher::PrivilegeListContext *ctx) = 0;

  virtual void enterShowPrivileges(MemgraphCypher::ShowPrivilegesContext *ctx) = 0;
  virtual void exitShowPrivileges(MemgraphCypher::ShowPrivilegesContext *ctx) = 0;

  virtual void enterShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *ctx) = 0;
  virtual void exitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext *ctx) = 0;

  virtual void enterShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *ctx) = 0;
  virtual void exitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext *ctx) = 0;

  virtual void enterDumpQuery(MemgraphCypher::DumpQueryContext *ctx) = 0;
  virtual void exitDumpQuery(MemgraphCypher::DumpQueryContext *ctx) = 0;

  virtual void enterSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *ctx) = 0;
  virtual void exitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext *ctx) = 0;

  virtual void enterShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext *ctx) = 0;
  virtual void exitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext *ctx) = 0;

  virtual void enterReplicaName(MemgraphCypher::ReplicaNameContext *ctx) = 0;
  virtual void exitReplicaName(MemgraphCypher::ReplicaNameContext *ctx) = 0;

  virtual void enterSocketAddress(MemgraphCypher::SocketAddressContext *ctx) = 0;
  virtual void exitSocketAddress(MemgraphCypher::SocketAddressContext *ctx) = 0;

  virtual void enterRegisterReplica(MemgraphCypher::RegisterReplicaContext *ctx) = 0;
  virtual void exitRegisterReplica(MemgraphCypher::RegisterReplicaContext *ctx) = 0;

  virtual void enterDropReplica(MemgraphCypher::DropReplicaContext *ctx) = 0;
  virtual void exitDropReplica(MemgraphCypher::DropReplicaContext *ctx) = 0;

  virtual void enterShowReplicas(MemgraphCypher::ShowReplicasContext *ctx) = 0;
  virtual void exitShowReplicas(MemgraphCypher::ShowReplicasContext *ctx) = 0;

  virtual void enterLockPathQuery(MemgraphCypher::LockPathQueryContext *ctx) = 0;
  virtual void exitLockPathQuery(MemgraphCypher::LockPathQueryContext *ctx) = 0;

  virtual void enterFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext *ctx) = 0;
  virtual void exitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext *ctx) = 0;

  virtual void enterTriggerName(MemgraphCypher::TriggerNameContext *ctx) = 0;
  virtual void exitTriggerName(MemgraphCypher::TriggerNameContext *ctx) = 0;

  virtual void enterTriggerStatement(MemgraphCypher::TriggerStatementContext *ctx) = 0;
  virtual void exitTriggerStatement(MemgraphCypher::TriggerStatementContext *ctx) = 0;

  virtual void enterEmptyVertex(MemgraphCypher::EmptyVertexContext *ctx) = 0;
  virtual void exitEmptyVertex(MemgraphCypher::EmptyVertexContext *ctx) = 0;

  virtual void enterEmptyEdge(MemgraphCypher::EmptyEdgeContext *ctx) = 0;
  virtual void exitEmptyEdge(MemgraphCypher::EmptyEdgeContext *ctx) = 0;

  virtual void enterCreateTrigger(MemgraphCypher::CreateTriggerContext *ctx) = 0;
  virtual void exitCreateTrigger(MemgraphCypher::CreateTriggerContext *ctx) = 0;

  virtual void enterDropTrigger(MemgraphCypher::DropTriggerContext *ctx) = 0;
  virtual void exitDropTrigger(MemgraphCypher::DropTriggerContext *ctx) = 0;

  virtual void enterShowTriggers(MemgraphCypher::ShowTriggersContext *ctx) = 0;
  virtual void exitShowTriggers(MemgraphCypher::ShowTriggersContext *ctx) = 0;

  virtual void enterIsolationLevel(MemgraphCypher::IsolationLevelContext *ctx) = 0;
  virtual void exitIsolationLevel(MemgraphCypher::IsolationLevelContext *ctx) = 0;

  virtual void enterIsolationLevelScope(MemgraphCypher::IsolationLevelScopeContext *ctx) = 0;
  virtual void exitIsolationLevelScope(MemgraphCypher::IsolationLevelScopeContext *ctx) = 0;

  virtual void enterIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext *ctx) = 0;
  virtual void exitIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext *ctx) = 0;

  virtual void enterCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext *ctx) = 0;
  virtual void exitCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext *ctx) = 0;

  virtual void enterStreamName(MemgraphCypher::StreamNameContext *ctx) = 0;
  virtual void exitStreamName(MemgraphCypher::StreamNameContext *ctx) = 0;

  virtual void enterSymbolicNameWithMinus(MemgraphCypher::SymbolicNameWithMinusContext *ctx) = 0;
  virtual void exitSymbolicNameWithMinus(MemgraphCypher::SymbolicNameWithMinusContext *ctx) = 0;

  virtual void enterSymbolicNameWithDotsAndMinus(MemgraphCypher::SymbolicNameWithDotsAndMinusContext *ctx) = 0;
  virtual void exitSymbolicNameWithDotsAndMinus(MemgraphCypher::SymbolicNameWithDotsAndMinusContext *ctx) = 0;

  virtual void enterSymbolicTopicNames(MemgraphCypher::SymbolicTopicNamesContext *ctx) = 0;
  virtual void exitSymbolicTopicNames(MemgraphCypher::SymbolicTopicNamesContext *ctx) = 0;

  virtual void enterTopicNames(MemgraphCypher::TopicNamesContext *ctx) = 0;
  virtual void exitTopicNames(MemgraphCypher::TopicNamesContext *ctx) = 0;

  virtual void enterCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext *ctx) = 0;
  virtual void exitCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext *ctx) = 0;

  virtual void enterCreateStream(MemgraphCypher::CreateStreamContext *ctx) = 0;
  virtual void exitCreateStream(MemgraphCypher::CreateStreamContext *ctx) = 0;

  virtual void enterConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext *ctx) = 0;
  virtual void exitConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext *ctx) = 0;

  virtual void enterConfigMap(MemgraphCypher::ConfigMapContext *ctx) = 0;
  virtual void exitConfigMap(MemgraphCypher::ConfigMapContext *ctx) = 0;

  virtual void enterKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext *ctx) = 0;
  virtual void exitKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext *ctx) = 0;

  virtual void enterKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext *ctx) = 0;
  virtual void exitKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext *ctx) = 0;

  virtual void enterPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext *ctx) = 0;
  virtual void exitPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext *ctx) = 0;

  virtual void enterPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext *ctx) = 0;
  virtual void exitPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext *ctx) = 0;

  virtual void enterDropStream(MemgraphCypher::DropStreamContext *ctx) = 0;
  virtual void exitDropStream(MemgraphCypher::DropStreamContext *ctx) = 0;

  virtual void enterStartStream(MemgraphCypher::StartStreamContext *ctx) = 0;
  virtual void exitStartStream(MemgraphCypher::StartStreamContext *ctx) = 0;

  virtual void enterStartAllStreams(MemgraphCypher::StartAllStreamsContext *ctx) = 0;
  virtual void exitStartAllStreams(MemgraphCypher::StartAllStreamsContext *ctx) = 0;

  virtual void enterStopStream(MemgraphCypher::StopStreamContext *ctx) = 0;
  virtual void exitStopStream(MemgraphCypher::StopStreamContext *ctx) = 0;

  virtual void enterStopAllStreams(MemgraphCypher::StopAllStreamsContext *ctx) = 0;
  virtual void exitStopAllStreams(MemgraphCypher::StopAllStreamsContext *ctx) = 0;

  virtual void enterShowStreams(MemgraphCypher::ShowStreamsContext *ctx) = 0;
  virtual void exitShowStreams(MemgraphCypher::ShowStreamsContext *ctx) = 0;

  virtual void enterCheckStream(MemgraphCypher::CheckStreamContext *ctx) = 0;
  virtual void exitCheckStream(MemgraphCypher::CheckStreamContext *ctx) = 0;

  virtual void enterSettingName(MemgraphCypher::SettingNameContext *ctx) = 0;
  virtual void exitSettingName(MemgraphCypher::SettingNameContext *ctx) = 0;

  virtual void enterSettingValue(MemgraphCypher::SettingValueContext *ctx) = 0;
  virtual void exitSettingValue(MemgraphCypher::SettingValueContext *ctx) = 0;

  virtual void enterSetSetting(MemgraphCypher::SetSettingContext *ctx) = 0;
  virtual void exitSetSetting(MemgraphCypher::SetSettingContext *ctx) = 0;

  virtual void enterShowSetting(MemgraphCypher::ShowSettingContext *ctx) = 0;
  virtual void exitShowSetting(MemgraphCypher::ShowSettingContext *ctx) = 0;

  virtual void enterShowSettings(MemgraphCypher::ShowSettingsContext *ctx) = 0;
  virtual void exitShowSettings(MemgraphCypher::ShowSettingsContext *ctx) = 0;

  virtual void enterVersionQuery(MemgraphCypher::VersionQueryContext *ctx) = 0;
  virtual void exitVersionQuery(MemgraphCypher::VersionQueryContext *ctx) = 0;

  virtual void enterCypher(MemgraphCypher::CypherContext *ctx) = 0;
  virtual void exitCypher(MemgraphCypher::CypherContext *ctx) = 0;

  virtual void enterStatement(MemgraphCypher::StatementContext *ctx) = 0;
  virtual void exitStatement(MemgraphCypher::StatementContext *ctx) = 0;

  virtual void enterConstraintQuery(MemgraphCypher::ConstraintQueryContext *ctx) = 0;
  virtual void exitConstraintQuery(MemgraphCypher::ConstraintQueryContext *ctx) = 0;

  virtual void enterConstraint(MemgraphCypher::ConstraintContext *ctx) = 0;
  virtual void exitConstraint(MemgraphCypher::ConstraintContext *ctx) = 0;

  virtual void enterConstraintPropertyList(MemgraphCypher::ConstraintPropertyListContext *ctx) = 0;
  virtual void exitConstraintPropertyList(MemgraphCypher::ConstraintPropertyListContext *ctx) = 0;

  virtual void enterStorageInfo(MemgraphCypher::StorageInfoContext *ctx) = 0;
  virtual void exitStorageInfo(MemgraphCypher::StorageInfoContext *ctx) = 0;

  virtual void enterIndexInfo(MemgraphCypher::IndexInfoContext *ctx) = 0;
  virtual void exitIndexInfo(MemgraphCypher::IndexInfoContext *ctx) = 0;

  virtual void enterConstraintInfo(MemgraphCypher::ConstraintInfoContext *ctx) = 0;
  virtual void exitConstraintInfo(MemgraphCypher::ConstraintInfoContext *ctx) = 0;

  virtual void enterInfoQuery(MemgraphCypher::InfoQueryContext *ctx) = 0;
  virtual void exitInfoQuery(MemgraphCypher::InfoQueryContext *ctx) = 0;

  virtual void enterExplainQuery(MemgraphCypher::ExplainQueryContext *ctx) = 0;
  virtual void exitExplainQuery(MemgraphCypher::ExplainQueryContext *ctx) = 0;

  virtual void enterProfileQuery(MemgraphCypher::ProfileQueryContext *ctx) = 0;
  virtual void exitProfileQuery(MemgraphCypher::ProfileQueryContext *ctx) = 0;

  virtual void enterCypherQuery(MemgraphCypher::CypherQueryContext *ctx) = 0;
  virtual void exitCypherQuery(MemgraphCypher::CypherQueryContext *ctx) = 0;

  virtual void enterIndexQuery(MemgraphCypher::IndexQueryContext *ctx) = 0;
  virtual void exitIndexQuery(MemgraphCypher::IndexQueryContext *ctx) = 0;

  virtual void enterSingleQuery(MemgraphCypher::SingleQueryContext *ctx) = 0;
  virtual void exitSingleQuery(MemgraphCypher::SingleQueryContext *ctx) = 0;

  virtual void enterCypherUnion(MemgraphCypher::CypherUnionContext *ctx) = 0;
  virtual void exitCypherUnion(MemgraphCypher::CypherUnionContext *ctx) = 0;

  virtual void enterCypherMatch(MemgraphCypher::CypherMatchContext *ctx) = 0;
  virtual void exitCypherMatch(MemgraphCypher::CypherMatchContext *ctx) = 0;

  virtual void enterUnwind(MemgraphCypher::UnwindContext *ctx) = 0;
  virtual void exitUnwind(MemgraphCypher::UnwindContext *ctx) = 0;

  virtual void enterMerge(MemgraphCypher::MergeContext *ctx) = 0;
  virtual void exitMerge(MemgraphCypher::MergeContext *ctx) = 0;

  virtual void enterMergeAction(MemgraphCypher::MergeActionContext *ctx) = 0;
  virtual void exitMergeAction(MemgraphCypher::MergeActionContext *ctx) = 0;

  virtual void enterCreate(MemgraphCypher::CreateContext *ctx) = 0;
  virtual void exitCreate(MemgraphCypher::CreateContext *ctx) = 0;

  virtual void enterSet(MemgraphCypher::SetContext *ctx) = 0;
  virtual void exitSet(MemgraphCypher::SetContext *ctx) = 0;

  virtual void enterSetItem(MemgraphCypher::SetItemContext *ctx) = 0;
  virtual void exitSetItem(MemgraphCypher::SetItemContext *ctx) = 0;

  virtual void enterCypherDelete(MemgraphCypher::CypherDeleteContext *ctx) = 0;
  virtual void exitCypherDelete(MemgraphCypher::CypherDeleteContext *ctx) = 0;

  virtual void enterRemove(MemgraphCypher::RemoveContext *ctx) = 0;
  virtual void exitRemove(MemgraphCypher::RemoveContext *ctx) = 0;

  virtual void enterRemoveItem(MemgraphCypher::RemoveItemContext *ctx) = 0;
  virtual void exitRemoveItem(MemgraphCypher::RemoveItemContext *ctx) = 0;

  virtual void enterWith(MemgraphCypher::WithContext *ctx) = 0;
  virtual void exitWith(MemgraphCypher::WithContext *ctx) = 0;

  virtual void enterCypherReturn(MemgraphCypher::CypherReturnContext *ctx) = 0;
  virtual void exitCypherReturn(MemgraphCypher::CypherReturnContext *ctx) = 0;

  virtual void enterCallProcedure(MemgraphCypher::CallProcedureContext *ctx) = 0;
  virtual void exitCallProcedure(MemgraphCypher::CallProcedureContext *ctx) = 0;

  virtual void enterProcedureName(MemgraphCypher::ProcedureNameContext *ctx) = 0;
  virtual void exitProcedureName(MemgraphCypher::ProcedureNameContext *ctx) = 0;

  virtual void enterYieldProcedureResults(MemgraphCypher::YieldProcedureResultsContext *ctx) = 0;
  virtual void exitYieldProcedureResults(MemgraphCypher::YieldProcedureResultsContext *ctx) = 0;

  virtual void enterMemoryLimit(MemgraphCypher::MemoryLimitContext *ctx) = 0;
  virtual void exitMemoryLimit(MemgraphCypher::MemoryLimitContext *ctx) = 0;

  virtual void enterQueryMemoryLimit(MemgraphCypher::QueryMemoryLimitContext *ctx) = 0;
  virtual void exitQueryMemoryLimit(MemgraphCypher::QueryMemoryLimitContext *ctx) = 0;

  virtual void enterProcedureMemoryLimit(MemgraphCypher::ProcedureMemoryLimitContext *ctx) = 0;
  virtual void exitProcedureMemoryLimit(MemgraphCypher::ProcedureMemoryLimitContext *ctx) = 0;

  virtual void enterProcedureResult(MemgraphCypher::ProcedureResultContext *ctx) = 0;
  virtual void exitProcedureResult(MemgraphCypher::ProcedureResultContext *ctx) = 0;

  virtual void enterReturnBody(MemgraphCypher::ReturnBodyContext *ctx) = 0;
  virtual void exitReturnBody(MemgraphCypher::ReturnBodyContext *ctx) = 0;

  virtual void enterReturnItems(MemgraphCypher::ReturnItemsContext *ctx) = 0;
  virtual void exitReturnItems(MemgraphCypher::ReturnItemsContext *ctx) = 0;

  virtual void enterReturnItem(MemgraphCypher::ReturnItemContext *ctx) = 0;
  virtual void exitReturnItem(MemgraphCypher::ReturnItemContext *ctx) = 0;

  virtual void enterOrder(MemgraphCypher::OrderContext *ctx) = 0;
  virtual void exitOrder(MemgraphCypher::OrderContext *ctx) = 0;

  virtual void enterSkip(MemgraphCypher::SkipContext *ctx) = 0;
  virtual void exitSkip(MemgraphCypher::SkipContext *ctx) = 0;

  virtual void enterLimit(MemgraphCypher::LimitContext *ctx) = 0;
  virtual void exitLimit(MemgraphCypher::LimitContext *ctx) = 0;

  virtual void enterSortItem(MemgraphCypher::SortItemContext *ctx) = 0;
  virtual void exitSortItem(MemgraphCypher::SortItemContext *ctx) = 0;

  virtual void enterWhere(MemgraphCypher::WhereContext *ctx) = 0;
  virtual void exitWhere(MemgraphCypher::WhereContext *ctx) = 0;

  virtual void enterPattern(MemgraphCypher::PatternContext *ctx) = 0;
  virtual void exitPattern(MemgraphCypher::PatternContext *ctx) = 0;

  virtual void enterPatternPart(MemgraphCypher::PatternPartContext *ctx) = 0;
  virtual void exitPatternPart(MemgraphCypher::PatternPartContext *ctx) = 0;

  virtual void enterAnonymousPatternPart(MemgraphCypher::AnonymousPatternPartContext *ctx) = 0;
  virtual void exitAnonymousPatternPart(MemgraphCypher::AnonymousPatternPartContext *ctx) = 0;

  virtual void enterPatternElement(MemgraphCypher::PatternElementContext *ctx) = 0;
  virtual void exitPatternElement(MemgraphCypher::PatternElementContext *ctx) = 0;

  virtual void enterNodePattern(MemgraphCypher::NodePatternContext *ctx) = 0;
  virtual void exitNodePattern(MemgraphCypher::NodePatternContext *ctx) = 0;

  virtual void enterPatternElementChain(MemgraphCypher::PatternElementChainContext *ctx) = 0;
  virtual void exitPatternElementChain(MemgraphCypher::PatternElementChainContext *ctx) = 0;

  virtual void enterRelationshipPattern(MemgraphCypher::RelationshipPatternContext *ctx) = 0;
  virtual void exitRelationshipPattern(MemgraphCypher::RelationshipPatternContext *ctx) = 0;

  virtual void enterLeftArrowHead(MemgraphCypher::LeftArrowHeadContext *ctx) = 0;
  virtual void exitLeftArrowHead(MemgraphCypher::LeftArrowHeadContext *ctx) = 0;

  virtual void enterRightArrowHead(MemgraphCypher::RightArrowHeadContext *ctx) = 0;
  virtual void exitRightArrowHead(MemgraphCypher::RightArrowHeadContext *ctx) = 0;

  virtual void enterDash(MemgraphCypher::DashContext *ctx) = 0;
  virtual void exitDash(MemgraphCypher::DashContext *ctx) = 0;

  virtual void enterRelationshipDetail(MemgraphCypher::RelationshipDetailContext *ctx) = 0;
  virtual void exitRelationshipDetail(MemgraphCypher::RelationshipDetailContext *ctx) = 0;

  virtual void enterRelationshipLambda(MemgraphCypher::RelationshipLambdaContext *ctx) = 0;
  virtual void exitRelationshipLambda(MemgraphCypher::RelationshipLambdaContext *ctx) = 0;

  virtual void enterVariableExpansion(MemgraphCypher::VariableExpansionContext *ctx) = 0;
  virtual void exitVariableExpansion(MemgraphCypher::VariableExpansionContext *ctx) = 0;

  virtual void enterProperties(MemgraphCypher::PropertiesContext *ctx) = 0;
  virtual void exitProperties(MemgraphCypher::PropertiesContext *ctx) = 0;

  virtual void enterRelationshipTypes(MemgraphCypher::RelationshipTypesContext *ctx) = 0;
  virtual void exitRelationshipTypes(MemgraphCypher::RelationshipTypesContext *ctx) = 0;

  virtual void enterNodeLabels(MemgraphCypher::NodeLabelsContext *ctx) = 0;
  virtual void exitNodeLabels(MemgraphCypher::NodeLabelsContext *ctx) = 0;

  virtual void enterNodeLabel(MemgraphCypher::NodeLabelContext *ctx) = 0;
  virtual void exitNodeLabel(MemgraphCypher::NodeLabelContext *ctx) = 0;

  virtual void enterLabelName(MemgraphCypher::LabelNameContext *ctx) = 0;
  virtual void exitLabelName(MemgraphCypher::LabelNameContext *ctx) = 0;

  virtual void enterRelTypeName(MemgraphCypher::RelTypeNameContext *ctx) = 0;
  virtual void exitRelTypeName(MemgraphCypher::RelTypeNameContext *ctx) = 0;

  virtual void enterExpression(MemgraphCypher::ExpressionContext *ctx) = 0;
  virtual void exitExpression(MemgraphCypher::ExpressionContext *ctx) = 0;

  virtual void enterExpression12(MemgraphCypher::Expression12Context *ctx) = 0;
  virtual void exitExpression12(MemgraphCypher::Expression12Context *ctx) = 0;

  virtual void enterExpression11(MemgraphCypher::Expression11Context *ctx) = 0;
  virtual void exitExpression11(MemgraphCypher::Expression11Context *ctx) = 0;

  virtual void enterExpression10(MemgraphCypher::Expression10Context *ctx) = 0;
  virtual void exitExpression10(MemgraphCypher::Expression10Context *ctx) = 0;

  virtual void enterExpression9(MemgraphCypher::Expression9Context *ctx) = 0;
  virtual void exitExpression9(MemgraphCypher::Expression9Context *ctx) = 0;

  virtual void enterExpression8(MemgraphCypher::Expression8Context *ctx) = 0;
  virtual void exitExpression8(MemgraphCypher::Expression8Context *ctx) = 0;

  virtual void enterExpression7(MemgraphCypher::Expression7Context *ctx) = 0;
  virtual void exitExpression7(MemgraphCypher::Expression7Context *ctx) = 0;

  virtual void enterExpression6(MemgraphCypher::Expression6Context *ctx) = 0;
  virtual void exitExpression6(MemgraphCypher::Expression6Context *ctx) = 0;

  virtual void enterExpression5(MemgraphCypher::Expression5Context *ctx) = 0;
  virtual void exitExpression5(MemgraphCypher::Expression5Context *ctx) = 0;

  virtual void enterExpression4(MemgraphCypher::Expression4Context *ctx) = 0;
  virtual void exitExpression4(MemgraphCypher::Expression4Context *ctx) = 0;

  virtual void enterExpression3a(MemgraphCypher::Expression3aContext *ctx) = 0;
  virtual void exitExpression3a(MemgraphCypher::Expression3aContext *ctx) = 0;

  virtual void enterStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext *ctx) = 0;
  virtual void exitStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext *ctx) = 0;

  virtual void enterExpression3b(MemgraphCypher::Expression3bContext *ctx) = 0;
  virtual void exitExpression3b(MemgraphCypher::Expression3bContext *ctx) = 0;

  virtual void enterListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext *ctx) = 0;
  virtual void exitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext *ctx) = 0;

  virtual void enterExpression2a(MemgraphCypher::Expression2aContext *ctx) = 0;
  virtual void exitExpression2a(MemgraphCypher::Expression2aContext *ctx) = 0;

  virtual void enterExpression2b(MemgraphCypher::Expression2bContext *ctx) = 0;
  virtual void exitExpression2b(MemgraphCypher::Expression2bContext *ctx) = 0;

  virtual void enterAtom(MemgraphCypher::AtomContext *ctx) = 0;
  virtual void exitAtom(MemgraphCypher::AtomContext *ctx) = 0;

  virtual void enterLiteral(MemgraphCypher::LiteralContext *ctx) = 0;
  virtual void exitLiteral(MemgraphCypher::LiteralContext *ctx) = 0;

  virtual void enterBooleanLiteral(MemgraphCypher::BooleanLiteralContext *ctx) = 0;
  virtual void exitBooleanLiteral(MemgraphCypher::BooleanLiteralContext *ctx) = 0;

  virtual void enterListLiteral(MemgraphCypher::ListLiteralContext *ctx) = 0;
  virtual void exitListLiteral(MemgraphCypher::ListLiteralContext *ctx) = 0;

  virtual void enterPartialComparisonExpression(MemgraphCypher::PartialComparisonExpressionContext *ctx) = 0;
  virtual void exitPartialComparisonExpression(MemgraphCypher::PartialComparisonExpressionContext *ctx) = 0;

  virtual void enterParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *ctx) = 0;
  virtual void exitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext *ctx) = 0;

  virtual void enterRelationshipsPattern(MemgraphCypher::RelationshipsPatternContext *ctx) = 0;
  virtual void exitRelationshipsPattern(MemgraphCypher::RelationshipsPatternContext *ctx) = 0;

  virtual void enterFilterExpression(MemgraphCypher::FilterExpressionContext *ctx) = 0;
  virtual void exitFilterExpression(MemgraphCypher::FilterExpressionContext *ctx) = 0;

  virtual void enterReduceExpression(MemgraphCypher::ReduceExpressionContext *ctx) = 0;
  virtual void exitReduceExpression(MemgraphCypher::ReduceExpressionContext *ctx) = 0;

  virtual void enterExtractExpression(MemgraphCypher::ExtractExpressionContext *ctx) = 0;
  virtual void exitExtractExpression(MemgraphCypher::ExtractExpressionContext *ctx) = 0;

  virtual void enterIdInColl(MemgraphCypher::IdInCollContext *ctx) = 0;
  virtual void exitIdInColl(MemgraphCypher::IdInCollContext *ctx) = 0;

  virtual void enterFunctionInvocation(MemgraphCypher::FunctionInvocationContext *ctx) = 0;
  virtual void exitFunctionInvocation(MemgraphCypher::FunctionInvocationContext *ctx) = 0;

  virtual void enterFunctionName(MemgraphCypher::FunctionNameContext *ctx) = 0;
  virtual void exitFunctionName(MemgraphCypher::FunctionNameContext *ctx) = 0;

  virtual void enterListComprehension(MemgraphCypher::ListComprehensionContext *ctx) = 0;
  virtual void exitListComprehension(MemgraphCypher::ListComprehensionContext *ctx) = 0;

  virtual void enterPatternComprehension(MemgraphCypher::PatternComprehensionContext *ctx) = 0;
  virtual void exitPatternComprehension(MemgraphCypher::PatternComprehensionContext *ctx) = 0;

  virtual void enterPropertyLookup(MemgraphCypher::PropertyLookupContext *ctx) = 0;
  virtual void exitPropertyLookup(MemgraphCypher::PropertyLookupContext *ctx) = 0;

  virtual void enterCaseExpression(MemgraphCypher::CaseExpressionContext *ctx) = 0;
  virtual void exitCaseExpression(MemgraphCypher::CaseExpressionContext *ctx) = 0;

  virtual void enterCaseAlternatives(MemgraphCypher::CaseAlternativesContext *ctx) = 0;
  virtual void exitCaseAlternatives(MemgraphCypher::CaseAlternativesContext *ctx) = 0;

  virtual void enterVariable(MemgraphCypher::VariableContext *ctx) = 0;
  virtual void exitVariable(MemgraphCypher::VariableContext *ctx) = 0;

  virtual void enterNumberLiteral(MemgraphCypher::NumberLiteralContext *ctx) = 0;
  virtual void exitNumberLiteral(MemgraphCypher::NumberLiteralContext *ctx) = 0;

  virtual void enterMapLiteral(MemgraphCypher::MapLiteralContext *ctx) = 0;
  virtual void exitMapLiteral(MemgraphCypher::MapLiteralContext *ctx) = 0;

  virtual void enterParameter(MemgraphCypher::ParameterContext *ctx) = 0;
  virtual void exitParameter(MemgraphCypher::ParameterContext *ctx) = 0;

  virtual void enterPropertyExpression(MemgraphCypher::PropertyExpressionContext *ctx) = 0;
  virtual void exitPropertyExpression(MemgraphCypher::PropertyExpressionContext *ctx) = 0;

  virtual void enterPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *ctx) = 0;
  virtual void exitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext *ctx) = 0;

  virtual void enterIntegerLiteral(MemgraphCypher::IntegerLiteralContext *ctx) = 0;
  virtual void exitIntegerLiteral(MemgraphCypher::IntegerLiteralContext *ctx) = 0;

  virtual void enterCreateIndex(MemgraphCypher::CreateIndexContext *ctx) = 0;
  virtual void exitCreateIndex(MemgraphCypher::CreateIndexContext *ctx) = 0;

  virtual void enterDropIndex(MemgraphCypher::DropIndexContext *ctx) = 0;
  virtual void exitDropIndex(MemgraphCypher::DropIndexContext *ctx) = 0;

  virtual void enterDoubleLiteral(MemgraphCypher::DoubleLiteralContext *ctx) = 0;
  virtual void exitDoubleLiteral(MemgraphCypher::DoubleLiteralContext *ctx) = 0;

  virtual void enterCypherKeyword(MemgraphCypher::CypherKeywordContext *ctx) = 0;
  virtual void exitCypherKeyword(MemgraphCypher::CypherKeywordContext *ctx) = 0;


};

}  // namespace antlropencypher
