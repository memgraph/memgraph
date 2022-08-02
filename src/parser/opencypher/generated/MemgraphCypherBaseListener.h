
// Generated from /home/kostas/Desktop/memgraph/src/parser/opencypher/grammar/MemgraphCypher.g4 by ANTLR 4.10.1

#pragma once


#include "antlr4-runtime.h"
#include "MemgraphCypherListener.h"


namespace antlropencypher {

/**
 * This class provides an empty implementation of MemgraphCypherListener,
 * which can be extended to create a listener which only needs to handle a subset
 * of the available methods.
 */
class  MemgraphCypherBaseListener : public MemgraphCypherListener {
public:

  virtual void enterMemgraphCypherKeyword(MemgraphCypher::MemgraphCypherKeywordContext * /*ctx*/) override { }
  virtual void exitMemgraphCypherKeyword(MemgraphCypher::MemgraphCypherKeywordContext * /*ctx*/) override { }

  virtual void enterSymbolicName(MemgraphCypher::SymbolicNameContext * /*ctx*/) override { }
  virtual void exitSymbolicName(MemgraphCypher::SymbolicNameContext * /*ctx*/) override { }

  virtual void enterQuery(MemgraphCypher::QueryContext * /*ctx*/) override { }
  virtual void exitQuery(MemgraphCypher::QueryContext * /*ctx*/) override { }

  virtual void enterAuthQuery(MemgraphCypher::AuthQueryContext * /*ctx*/) override { }
  virtual void exitAuthQuery(MemgraphCypher::AuthQueryContext * /*ctx*/) override { }

  virtual void enterReplicationQuery(MemgraphCypher::ReplicationQueryContext * /*ctx*/) override { }
  virtual void exitReplicationQuery(MemgraphCypher::ReplicationQueryContext * /*ctx*/) override { }

  virtual void enterTriggerQuery(MemgraphCypher::TriggerQueryContext * /*ctx*/) override { }
  virtual void exitTriggerQuery(MemgraphCypher::TriggerQueryContext * /*ctx*/) override { }

  virtual void enterClause(MemgraphCypher::ClauseContext * /*ctx*/) override { }
  virtual void exitClause(MemgraphCypher::ClauseContext * /*ctx*/) override { }

  virtual void enterUpdateClause(MemgraphCypher::UpdateClauseContext * /*ctx*/) override { }
  virtual void exitUpdateClause(MemgraphCypher::UpdateClauseContext * /*ctx*/) override { }

  virtual void enterForeach(MemgraphCypher::ForeachContext * /*ctx*/) override { }
  virtual void exitForeach(MemgraphCypher::ForeachContext * /*ctx*/) override { }

  virtual void enterStreamQuery(MemgraphCypher::StreamQueryContext * /*ctx*/) override { }
  virtual void exitStreamQuery(MemgraphCypher::StreamQueryContext * /*ctx*/) override { }

  virtual void enterSettingQuery(MemgraphCypher::SettingQueryContext * /*ctx*/) override { }
  virtual void exitSettingQuery(MemgraphCypher::SettingQueryContext * /*ctx*/) override { }

  virtual void enterLoadCsv(MemgraphCypher::LoadCsvContext * /*ctx*/) override { }
  virtual void exitLoadCsv(MemgraphCypher::LoadCsvContext * /*ctx*/) override { }

  virtual void enterCsvFile(MemgraphCypher::CsvFileContext * /*ctx*/) override { }
  virtual void exitCsvFile(MemgraphCypher::CsvFileContext * /*ctx*/) override { }

  virtual void enterDelimiter(MemgraphCypher::DelimiterContext * /*ctx*/) override { }
  virtual void exitDelimiter(MemgraphCypher::DelimiterContext * /*ctx*/) override { }

  virtual void enterQuote(MemgraphCypher::QuoteContext * /*ctx*/) override { }
  virtual void exitQuote(MemgraphCypher::QuoteContext * /*ctx*/) override { }

  virtual void enterRowVar(MemgraphCypher::RowVarContext * /*ctx*/) override { }
  virtual void exitRowVar(MemgraphCypher::RowVarContext * /*ctx*/) override { }

  virtual void enterUserOrRoleName(MemgraphCypher::UserOrRoleNameContext * /*ctx*/) override { }
  virtual void exitUserOrRoleName(MemgraphCypher::UserOrRoleNameContext * /*ctx*/) override { }

  virtual void enterCreateRole(MemgraphCypher::CreateRoleContext * /*ctx*/) override { }
  virtual void exitCreateRole(MemgraphCypher::CreateRoleContext * /*ctx*/) override { }

  virtual void enterDropRole(MemgraphCypher::DropRoleContext * /*ctx*/) override { }
  virtual void exitDropRole(MemgraphCypher::DropRoleContext * /*ctx*/) override { }

  virtual void enterShowRoles(MemgraphCypher::ShowRolesContext * /*ctx*/) override { }
  virtual void exitShowRoles(MemgraphCypher::ShowRolesContext * /*ctx*/) override { }

  virtual void enterCreateUser(MemgraphCypher::CreateUserContext * /*ctx*/) override { }
  virtual void exitCreateUser(MemgraphCypher::CreateUserContext * /*ctx*/) override { }

  virtual void enterSetPassword(MemgraphCypher::SetPasswordContext * /*ctx*/) override { }
  virtual void exitSetPassword(MemgraphCypher::SetPasswordContext * /*ctx*/) override { }

  virtual void enterDropUser(MemgraphCypher::DropUserContext * /*ctx*/) override { }
  virtual void exitDropUser(MemgraphCypher::DropUserContext * /*ctx*/) override { }

  virtual void enterShowUsers(MemgraphCypher::ShowUsersContext * /*ctx*/) override { }
  virtual void exitShowUsers(MemgraphCypher::ShowUsersContext * /*ctx*/) override { }

  virtual void enterSetRole(MemgraphCypher::SetRoleContext * /*ctx*/) override { }
  virtual void exitSetRole(MemgraphCypher::SetRoleContext * /*ctx*/) override { }

  virtual void enterClearRole(MemgraphCypher::ClearRoleContext * /*ctx*/) override { }
  virtual void exitClearRole(MemgraphCypher::ClearRoleContext * /*ctx*/) override { }

  virtual void enterGrantPrivilege(MemgraphCypher::GrantPrivilegeContext * /*ctx*/) override { }
  virtual void exitGrantPrivilege(MemgraphCypher::GrantPrivilegeContext * /*ctx*/) override { }

  virtual void enterDenyPrivilege(MemgraphCypher::DenyPrivilegeContext * /*ctx*/) override { }
  virtual void exitDenyPrivilege(MemgraphCypher::DenyPrivilegeContext * /*ctx*/) override { }

  virtual void enterRevokePrivilege(MemgraphCypher::RevokePrivilegeContext * /*ctx*/) override { }
  virtual void exitRevokePrivilege(MemgraphCypher::RevokePrivilegeContext * /*ctx*/) override { }

  virtual void enterPrivilege(MemgraphCypher::PrivilegeContext * /*ctx*/) override { }
  virtual void exitPrivilege(MemgraphCypher::PrivilegeContext * /*ctx*/) override { }

  virtual void enterPrivilegeList(MemgraphCypher::PrivilegeListContext * /*ctx*/) override { }
  virtual void exitPrivilegeList(MemgraphCypher::PrivilegeListContext * /*ctx*/) override { }

  virtual void enterShowPrivileges(MemgraphCypher::ShowPrivilegesContext * /*ctx*/) override { }
  virtual void exitShowPrivileges(MemgraphCypher::ShowPrivilegesContext * /*ctx*/) override { }

  virtual void enterShowRoleForUser(MemgraphCypher::ShowRoleForUserContext * /*ctx*/) override { }
  virtual void exitShowRoleForUser(MemgraphCypher::ShowRoleForUserContext * /*ctx*/) override { }

  virtual void enterShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext * /*ctx*/) override { }
  virtual void exitShowUsersForRole(MemgraphCypher::ShowUsersForRoleContext * /*ctx*/) override { }

  virtual void enterDumpQuery(MemgraphCypher::DumpQueryContext * /*ctx*/) override { }
  virtual void exitDumpQuery(MemgraphCypher::DumpQueryContext * /*ctx*/) override { }

  virtual void enterSetReplicationRole(MemgraphCypher::SetReplicationRoleContext * /*ctx*/) override { }
  virtual void exitSetReplicationRole(MemgraphCypher::SetReplicationRoleContext * /*ctx*/) override { }

  virtual void enterShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext * /*ctx*/) override { }
  virtual void exitShowReplicationRole(MemgraphCypher::ShowReplicationRoleContext * /*ctx*/) override { }

  virtual void enterReplicaName(MemgraphCypher::ReplicaNameContext * /*ctx*/) override { }
  virtual void exitReplicaName(MemgraphCypher::ReplicaNameContext * /*ctx*/) override { }

  virtual void enterSocketAddress(MemgraphCypher::SocketAddressContext * /*ctx*/) override { }
  virtual void exitSocketAddress(MemgraphCypher::SocketAddressContext * /*ctx*/) override { }

  virtual void enterRegisterReplica(MemgraphCypher::RegisterReplicaContext * /*ctx*/) override { }
  virtual void exitRegisterReplica(MemgraphCypher::RegisterReplicaContext * /*ctx*/) override { }

  virtual void enterDropReplica(MemgraphCypher::DropReplicaContext * /*ctx*/) override { }
  virtual void exitDropReplica(MemgraphCypher::DropReplicaContext * /*ctx*/) override { }

  virtual void enterShowReplicas(MemgraphCypher::ShowReplicasContext * /*ctx*/) override { }
  virtual void exitShowReplicas(MemgraphCypher::ShowReplicasContext * /*ctx*/) override { }

  virtual void enterLockPathQuery(MemgraphCypher::LockPathQueryContext * /*ctx*/) override { }
  virtual void exitLockPathQuery(MemgraphCypher::LockPathQueryContext * /*ctx*/) override { }

  virtual void enterFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext * /*ctx*/) override { }
  virtual void exitFreeMemoryQuery(MemgraphCypher::FreeMemoryQueryContext * /*ctx*/) override { }

  virtual void enterTriggerName(MemgraphCypher::TriggerNameContext * /*ctx*/) override { }
  virtual void exitTriggerName(MemgraphCypher::TriggerNameContext * /*ctx*/) override { }

  virtual void enterTriggerStatement(MemgraphCypher::TriggerStatementContext * /*ctx*/) override { }
  virtual void exitTriggerStatement(MemgraphCypher::TriggerStatementContext * /*ctx*/) override { }

  virtual void enterEmptyVertex(MemgraphCypher::EmptyVertexContext * /*ctx*/) override { }
  virtual void exitEmptyVertex(MemgraphCypher::EmptyVertexContext * /*ctx*/) override { }

  virtual void enterEmptyEdge(MemgraphCypher::EmptyEdgeContext * /*ctx*/) override { }
  virtual void exitEmptyEdge(MemgraphCypher::EmptyEdgeContext * /*ctx*/) override { }

  virtual void enterCreateTrigger(MemgraphCypher::CreateTriggerContext * /*ctx*/) override { }
  virtual void exitCreateTrigger(MemgraphCypher::CreateTriggerContext * /*ctx*/) override { }

  virtual void enterDropTrigger(MemgraphCypher::DropTriggerContext * /*ctx*/) override { }
  virtual void exitDropTrigger(MemgraphCypher::DropTriggerContext * /*ctx*/) override { }

  virtual void enterShowTriggers(MemgraphCypher::ShowTriggersContext * /*ctx*/) override { }
  virtual void exitShowTriggers(MemgraphCypher::ShowTriggersContext * /*ctx*/) override { }

  virtual void enterIsolationLevel(MemgraphCypher::IsolationLevelContext * /*ctx*/) override { }
  virtual void exitIsolationLevel(MemgraphCypher::IsolationLevelContext * /*ctx*/) override { }

  virtual void enterIsolationLevelScope(MemgraphCypher::IsolationLevelScopeContext * /*ctx*/) override { }
  virtual void exitIsolationLevelScope(MemgraphCypher::IsolationLevelScopeContext * /*ctx*/) override { }

  virtual void enterIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext * /*ctx*/) override { }
  virtual void exitIsolationLevelQuery(MemgraphCypher::IsolationLevelQueryContext * /*ctx*/) override { }

  virtual void enterCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext * /*ctx*/) override { }
  virtual void exitCreateSnapshotQuery(MemgraphCypher::CreateSnapshotQueryContext * /*ctx*/) override { }

  virtual void enterStreamName(MemgraphCypher::StreamNameContext * /*ctx*/) override { }
  virtual void exitStreamName(MemgraphCypher::StreamNameContext * /*ctx*/) override { }

  virtual void enterSymbolicNameWithMinus(MemgraphCypher::SymbolicNameWithMinusContext * /*ctx*/) override { }
  virtual void exitSymbolicNameWithMinus(MemgraphCypher::SymbolicNameWithMinusContext * /*ctx*/) override { }

  virtual void enterSymbolicNameWithDotsAndMinus(MemgraphCypher::SymbolicNameWithDotsAndMinusContext * /*ctx*/) override { }
  virtual void exitSymbolicNameWithDotsAndMinus(MemgraphCypher::SymbolicNameWithDotsAndMinusContext * /*ctx*/) override { }

  virtual void enterSymbolicTopicNames(MemgraphCypher::SymbolicTopicNamesContext * /*ctx*/) override { }
  virtual void exitSymbolicTopicNames(MemgraphCypher::SymbolicTopicNamesContext * /*ctx*/) override { }

  virtual void enterTopicNames(MemgraphCypher::TopicNamesContext * /*ctx*/) override { }
  virtual void exitTopicNames(MemgraphCypher::TopicNamesContext * /*ctx*/) override { }

  virtual void enterCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext * /*ctx*/) override { }
  virtual void exitCommonCreateStreamConfig(MemgraphCypher::CommonCreateStreamConfigContext * /*ctx*/) override { }

  virtual void enterCreateStream(MemgraphCypher::CreateStreamContext * /*ctx*/) override { }
  virtual void exitCreateStream(MemgraphCypher::CreateStreamContext * /*ctx*/) override { }

  virtual void enterConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext * /*ctx*/) override { }
  virtual void exitConfigKeyValuePair(MemgraphCypher::ConfigKeyValuePairContext * /*ctx*/) override { }

  virtual void enterConfigMap(MemgraphCypher::ConfigMapContext * /*ctx*/) override { }
  virtual void exitConfigMap(MemgraphCypher::ConfigMapContext * /*ctx*/) override { }

  virtual void enterKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext * /*ctx*/) override { }
  virtual void exitKafkaCreateStreamConfig(MemgraphCypher::KafkaCreateStreamConfigContext * /*ctx*/) override { }

  virtual void enterKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext * /*ctx*/) override { }
  virtual void exitKafkaCreateStream(MemgraphCypher::KafkaCreateStreamContext * /*ctx*/) override { }

  virtual void enterPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext * /*ctx*/) override { }
  virtual void exitPulsarCreateStreamConfig(MemgraphCypher::PulsarCreateStreamConfigContext * /*ctx*/) override { }

  virtual void enterPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext * /*ctx*/) override { }
  virtual void exitPulsarCreateStream(MemgraphCypher::PulsarCreateStreamContext * /*ctx*/) override { }

  virtual void enterDropStream(MemgraphCypher::DropStreamContext * /*ctx*/) override { }
  virtual void exitDropStream(MemgraphCypher::DropStreamContext * /*ctx*/) override { }

  virtual void enterStartStream(MemgraphCypher::StartStreamContext * /*ctx*/) override { }
  virtual void exitStartStream(MemgraphCypher::StartStreamContext * /*ctx*/) override { }

  virtual void enterStartAllStreams(MemgraphCypher::StartAllStreamsContext * /*ctx*/) override { }
  virtual void exitStartAllStreams(MemgraphCypher::StartAllStreamsContext * /*ctx*/) override { }

  virtual void enterStopStream(MemgraphCypher::StopStreamContext * /*ctx*/) override { }
  virtual void exitStopStream(MemgraphCypher::StopStreamContext * /*ctx*/) override { }

  virtual void enterStopAllStreams(MemgraphCypher::StopAllStreamsContext * /*ctx*/) override { }
  virtual void exitStopAllStreams(MemgraphCypher::StopAllStreamsContext * /*ctx*/) override { }

  virtual void enterShowStreams(MemgraphCypher::ShowStreamsContext * /*ctx*/) override { }
  virtual void exitShowStreams(MemgraphCypher::ShowStreamsContext * /*ctx*/) override { }

  virtual void enterCheckStream(MemgraphCypher::CheckStreamContext * /*ctx*/) override { }
  virtual void exitCheckStream(MemgraphCypher::CheckStreamContext * /*ctx*/) override { }

  virtual void enterSettingName(MemgraphCypher::SettingNameContext * /*ctx*/) override { }
  virtual void exitSettingName(MemgraphCypher::SettingNameContext * /*ctx*/) override { }

  virtual void enterSettingValue(MemgraphCypher::SettingValueContext * /*ctx*/) override { }
  virtual void exitSettingValue(MemgraphCypher::SettingValueContext * /*ctx*/) override { }

  virtual void enterSetSetting(MemgraphCypher::SetSettingContext * /*ctx*/) override { }
  virtual void exitSetSetting(MemgraphCypher::SetSettingContext * /*ctx*/) override { }

  virtual void enterShowSetting(MemgraphCypher::ShowSettingContext * /*ctx*/) override { }
  virtual void exitShowSetting(MemgraphCypher::ShowSettingContext * /*ctx*/) override { }

  virtual void enterShowSettings(MemgraphCypher::ShowSettingsContext * /*ctx*/) override { }
  virtual void exitShowSettings(MemgraphCypher::ShowSettingsContext * /*ctx*/) override { }

  virtual void enterVersionQuery(MemgraphCypher::VersionQueryContext * /*ctx*/) override { }
  virtual void exitVersionQuery(MemgraphCypher::VersionQueryContext * /*ctx*/) override { }

  virtual void enterCypher(MemgraphCypher::CypherContext * /*ctx*/) override { }
  virtual void exitCypher(MemgraphCypher::CypherContext * /*ctx*/) override { }

  virtual void enterStatement(MemgraphCypher::StatementContext * /*ctx*/) override { }
  virtual void exitStatement(MemgraphCypher::StatementContext * /*ctx*/) override { }

  virtual void enterConstraintQuery(MemgraphCypher::ConstraintQueryContext * /*ctx*/) override { }
  virtual void exitConstraintQuery(MemgraphCypher::ConstraintQueryContext * /*ctx*/) override { }

  virtual void enterConstraint(MemgraphCypher::ConstraintContext * /*ctx*/) override { }
  virtual void exitConstraint(MemgraphCypher::ConstraintContext * /*ctx*/) override { }

  virtual void enterConstraintPropertyList(MemgraphCypher::ConstraintPropertyListContext * /*ctx*/) override { }
  virtual void exitConstraintPropertyList(MemgraphCypher::ConstraintPropertyListContext * /*ctx*/) override { }

  virtual void enterStorageInfo(MemgraphCypher::StorageInfoContext * /*ctx*/) override { }
  virtual void exitStorageInfo(MemgraphCypher::StorageInfoContext * /*ctx*/) override { }

  virtual void enterIndexInfo(MemgraphCypher::IndexInfoContext * /*ctx*/) override { }
  virtual void exitIndexInfo(MemgraphCypher::IndexInfoContext * /*ctx*/) override { }

  virtual void enterConstraintInfo(MemgraphCypher::ConstraintInfoContext * /*ctx*/) override { }
  virtual void exitConstraintInfo(MemgraphCypher::ConstraintInfoContext * /*ctx*/) override { }

  virtual void enterInfoQuery(MemgraphCypher::InfoQueryContext * /*ctx*/) override { }
  virtual void exitInfoQuery(MemgraphCypher::InfoQueryContext * /*ctx*/) override { }

  virtual void enterExplainQuery(MemgraphCypher::ExplainQueryContext * /*ctx*/) override { }
  virtual void exitExplainQuery(MemgraphCypher::ExplainQueryContext * /*ctx*/) override { }

  virtual void enterProfileQuery(MemgraphCypher::ProfileQueryContext * /*ctx*/) override { }
  virtual void exitProfileQuery(MemgraphCypher::ProfileQueryContext * /*ctx*/) override { }

  virtual void enterCypherQuery(MemgraphCypher::CypherQueryContext * /*ctx*/) override { }
  virtual void exitCypherQuery(MemgraphCypher::CypherQueryContext * /*ctx*/) override { }

  virtual void enterIndexQuery(MemgraphCypher::IndexQueryContext * /*ctx*/) override { }
  virtual void exitIndexQuery(MemgraphCypher::IndexQueryContext * /*ctx*/) override { }

  virtual void enterSingleQuery(MemgraphCypher::SingleQueryContext * /*ctx*/) override { }
  virtual void exitSingleQuery(MemgraphCypher::SingleQueryContext * /*ctx*/) override { }

  virtual void enterCypherUnion(MemgraphCypher::CypherUnionContext * /*ctx*/) override { }
  virtual void exitCypherUnion(MemgraphCypher::CypherUnionContext * /*ctx*/) override { }

  virtual void enterCypherMatch(MemgraphCypher::CypherMatchContext * /*ctx*/) override { }
  virtual void exitCypherMatch(MemgraphCypher::CypherMatchContext * /*ctx*/) override { }

  virtual void enterUnwind(MemgraphCypher::UnwindContext * /*ctx*/) override { }
  virtual void exitUnwind(MemgraphCypher::UnwindContext * /*ctx*/) override { }

  virtual void enterMerge(MemgraphCypher::MergeContext * /*ctx*/) override { }
  virtual void exitMerge(MemgraphCypher::MergeContext * /*ctx*/) override { }

  virtual void enterMergeAction(MemgraphCypher::MergeActionContext * /*ctx*/) override { }
  virtual void exitMergeAction(MemgraphCypher::MergeActionContext * /*ctx*/) override { }

  virtual void enterCreate(MemgraphCypher::CreateContext * /*ctx*/) override { }
  virtual void exitCreate(MemgraphCypher::CreateContext * /*ctx*/) override { }

  virtual void enterSet(MemgraphCypher::SetContext * /*ctx*/) override { }
  virtual void exitSet(MemgraphCypher::SetContext * /*ctx*/) override { }

  virtual void enterSetItem(MemgraphCypher::SetItemContext * /*ctx*/) override { }
  virtual void exitSetItem(MemgraphCypher::SetItemContext * /*ctx*/) override { }

  virtual void enterCypherDelete(MemgraphCypher::CypherDeleteContext * /*ctx*/) override { }
  virtual void exitCypherDelete(MemgraphCypher::CypherDeleteContext * /*ctx*/) override { }

  virtual void enterRemove(MemgraphCypher::RemoveContext * /*ctx*/) override { }
  virtual void exitRemove(MemgraphCypher::RemoveContext * /*ctx*/) override { }

  virtual void enterRemoveItem(MemgraphCypher::RemoveItemContext * /*ctx*/) override { }
  virtual void exitRemoveItem(MemgraphCypher::RemoveItemContext * /*ctx*/) override { }

  virtual void enterWith(MemgraphCypher::WithContext * /*ctx*/) override { }
  virtual void exitWith(MemgraphCypher::WithContext * /*ctx*/) override { }

  virtual void enterCypherReturn(MemgraphCypher::CypherReturnContext * /*ctx*/) override { }
  virtual void exitCypherReturn(MemgraphCypher::CypherReturnContext * /*ctx*/) override { }

  virtual void enterCallProcedure(MemgraphCypher::CallProcedureContext * /*ctx*/) override { }
  virtual void exitCallProcedure(MemgraphCypher::CallProcedureContext * /*ctx*/) override { }

  virtual void enterProcedureName(MemgraphCypher::ProcedureNameContext * /*ctx*/) override { }
  virtual void exitProcedureName(MemgraphCypher::ProcedureNameContext * /*ctx*/) override { }

  virtual void enterYieldProcedureResults(MemgraphCypher::YieldProcedureResultsContext * /*ctx*/) override { }
  virtual void exitYieldProcedureResults(MemgraphCypher::YieldProcedureResultsContext * /*ctx*/) override { }

  virtual void enterMemoryLimit(MemgraphCypher::MemoryLimitContext * /*ctx*/) override { }
  virtual void exitMemoryLimit(MemgraphCypher::MemoryLimitContext * /*ctx*/) override { }

  virtual void enterQueryMemoryLimit(MemgraphCypher::QueryMemoryLimitContext * /*ctx*/) override { }
  virtual void exitQueryMemoryLimit(MemgraphCypher::QueryMemoryLimitContext * /*ctx*/) override { }

  virtual void enterProcedureMemoryLimit(MemgraphCypher::ProcedureMemoryLimitContext * /*ctx*/) override { }
  virtual void exitProcedureMemoryLimit(MemgraphCypher::ProcedureMemoryLimitContext * /*ctx*/) override { }

  virtual void enterProcedureResult(MemgraphCypher::ProcedureResultContext * /*ctx*/) override { }
  virtual void exitProcedureResult(MemgraphCypher::ProcedureResultContext * /*ctx*/) override { }

  virtual void enterReturnBody(MemgraphCypher::ReturnBodyContext * /*ctx*/) override { }
  virtual void exitReturnBody(MemgraphCypher::ReturnBodyContext * /*ctx*/) override { }

  virtual void enterReturnItems(MemgraphCypher::ReturnItemsContext * /*ctx*/) override { }
  virtual void exitReturnItems(MemgraphCypher::ReturnItemsContext * /*ctx*/) override { }

  virtual void enterReturnItem(MemgraphCypher::ReturnItemContext * /*ctx*/) override { }
  virtual void exitReturnItem(MemgraphCypher::ReturnItemContext * /*ctx*/) override { }

  virtual void enterOrder(MemgraphCypher::OrderContext * /*ctx*/) override { }
  virtual void exitOrder(MemgraphCypher::OrderContext * /*ctx*/) override { }

  virtual void enterSkip(MemgraphCypher::SkipContext * /*ctx*/) override { }
  virtual void exitSkip(MemgraphCypher::SkipContext * /*ctx*/) override { }

  virtual void enterLimit(MemgraphCypher::LimitContext * /*ctx*/) override { }
  virtual void exitLimit(MemgraphCypher::LimitContext * /*ctx*/) override { }

  virtual void enterSortItem(MemgraphCypher::SortItemContext * /*ctx*/) override { }
  virtual void exitSortItem(MemgraphCypher::SortItemContext * /*ctx*/) override { }

  virtual void enterWhere(MemgraphCypher::WhereContext * /*ctx*/) override { }
  virtual void exitWhere(MemgraphCypher::WhereContext * /*ctx*/) override { }

  virtual void enterPattern(MemgraphCypher::PatternContext * /*ctx*/) override { }
  virtual void exitPattern(MemgraphCypher::PatternContext * /*ctx*/) override { }

  virtual void enterPatternPart(MemgraphCypher::PatternPartContext * /*ctx*/) override { }
  virtual void exitPatternPart(MemgraphCypher::PatternPartContext * /*ctx*/) override { }

  virtual void enterAnonymousPatternPart(MemgraphCypher::AnonymousPatternPartContext * /*ctx*/) override { }
  virtual void exitAnonymousPatternPart(MemgraphCypher::AnonymousPatternPartContext * /*ctx*/) override { }

  virtual void enterPatternElement(MemgraphCypher::PatternElementContext * /*ctx*/) override { }
  virtual void exitPatternElement(MemgraphCypher::PatternElementContext * /*ctx*/) override { }

  virtual void enterNodePattern(MemgraphCypher::NodePatternContext * /*ctx*/) override { }
  virtual void exitNodePattern(MemgraphCypher::NodePatternContext * /*ctx*/) override { }

  virtual void enterPatternElementChain(MemgraphCypher::PatternElementChainContext * /*ctx*/) override { }
  virtual void exitPatternElementChain(MemgraphCypher::PatternElementChainContext * /*ctx*/) override { }

  virtual void enterRelationshipPattern(MemgraphCypher::RelationshipPatternContext * /*ctx*/) override { }
  virtual void exitRelationshipPattern(MemgraphCypher::RelationshipPatternContext * /*ctx*/) override { }

  virtual void enterLeftArrowHead(MemgraphCypher::LeftArrowHeadContext * /*ctx*/) override { }
  virtual void exitLeftArrowHead(MemgraphCypher::LeftArrowHeadContext * /*ctx*/) override { }

  virtual void enterRightArrowHead(MemgraphCypher::RightArrowHeadContext * /*ctx*/) override { }
  virtual void exitRightArrowHead(MemgraphCypher::RightArrowHeadContext * /*ctx*/) override { }

  virtual void enterDash(MemgraphCypher::DashContext * /*ctx*/) override { }
  virtual void exitDash(MemgraphCypher::DashContext * /*ctx*/) override { }

  virtual void enterRelationshipDetail(MemgraphCypher::RelationshipDetailContext * /*ctx*/) override { }
  virtual void exitRelationshipDetail(MemgraphCypher::RelationshipDetailContext * /*ctx*/) override { }

  virtual void enterRelationshipLambda(MemgraphCypher::RelationshipLambdaContext * /*ctx*/) override { }
  virtual void exitRelationshipLambda(MemgraphCypher::RelationshipLambdaContext * /*ctx*/) override { }

  virtual void enterVariableExpansion(MemgraphCypher::VariableExpansionContext * /*ctx*/) override { }
  virtual void exitVariableExpansion(MemgraphCypher::VariableExpansionContext * /*ctx*/) override { }

  virtual void enterProperties(MemgraphCypher::PropertiesContext * /*ctx*/) override { }
  virtual void exitProperties(MemgraphCypher::PropertiesContext * /*ctx*/) override { }

  virtual void enterRelationshipTypes(MemgraphCypher::RelationshipTypesContext * /*ctx*/) override { }
  virtual void exitRelationshipTypes(MemgraphCypher::RelationshipTypesContext * /*ctx*/) override { }

  virtual void enterNodeLabels(MemgraphCypher::NodeLabelsContext * /*ctx*/) override { }
  virtual void exitNodeLabels(MemgraphCypher::NodeLabelsContext * /*ctx*/) override { }

  virtual void enterNodeLabel(MemgraphCypher::NodeLabelContext * /*ctx*/) override { }
  virtual void exitNodeLabel(MemgraphCypher::NodeLabelContext * /*ctx*/) override { }

  virtual void enterLabelName(MemgraphCypher::LabelNameContext * /*ctx*/) override { }
  virtual void exitLabelName(MemgraphCypher::LabelNameContext * /*ctx*/) override { }

  virtual void enterRelTypeName(MemgraphCypher::RelTypeNameContext * /*ctx*/) override { }
  virtual void exitRelTypeName(MemgraphCypher::RelTypeNameContext * /*ctx*/) override { }

  virtual void enterExpression(MemgraphCypher::ExpressionContext * /*ctx*/) override { }
  virtual void exitExpression(MemgraphCypher::ExpressionContext * /*ctx*/) override { }

  virtual void enterExpression12(MemgraphCypher::Expression12Context * /*ctx*/) override { }
  virtual void exitExpression12(MemgraphCypher::Expression12Context * /*ctx*/) override { }

  virtual void enterExpression11(MemgraphCypher::Expression11Context * /*ctx*/) override { }
  virtual void exitExpression11(MemgraphCypher::Expression11Context * /*ctx*/) override { }

  virtual void enterExpression10(MemgraphCypher::Expression10Context * /*ctx*/) override { }
  virtual void exitExpression10(MemgraphCypher::Expression10Context * /*ctx*/) override { }

  virtual void enterExpression9(MemgraphCypher::Expression9Context * /*ctx*/) override { }
  virtual void exitExpression9(MemgraphCypher::Expression9Context * /*ctx*/) override { }

  virtual void enterExpression8(MemgraphCypher::Expression8Context * /*ctx*/) override { }
  virtual void exitExpression8(MemgraphCypher::Expression8Context * /*ctx*/) override { }

  virtual void enterExpression7(MemgraphCypher::Expression7Context * /*ctx*/) override { }
  virtual void exitExpression7(MemgraphCypher::Expression7Context * /*ctx*/) override { }

  virtual void enterExpression6(MemgraphCypher::Expression6Context * /*ctx*/) override { }
  virtual void exitExpression6(MemgraphCypher::Expression6Context * /*ctx*/) override { }

  virtual void enterExpression5(MemgraphCypher::Expression5Context * /*ctx*/) override { }
  virtual void exitExpression5(MemgraphCypher::Expression5Context * /*ctx*/) override { }

  virtual void enterExpression4(MemgraphCypher::Expression4Context * /*ctx*/) override { }
  virtual void exitExpression4(MemgraphCypher::Expression4Context * /*ctx*/) override { }

  virtual void enterExpression3a(MemgraphCypher::Expression3aContext * /*ctx*/) override { }
  virtual void exitExpression3a(MemgraphCypher::Expression3aContext * /*ctx*/) override { }

  virtual void enterStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext * /*ctx*/) override { }
  virtual void exitStringAndNullOperators(MemgraphCypher::StringAndNullOperatorsContext * /*ctx*/) override { }

  virtual void enterExpression3b(MemgraphCypher::Expression3bContext * /*ctx*/) override { }
  virtual void exitExpression3b(MemgraphCypher::Expression3bContext * /*ctx*/) override { }

  virtual void enterListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext * /*ctx*/) override { }
  virtual void exitListIndexingOrSlicing(MemgraphCypher::ListIndexingOrSlicingContext * /*ctx*/) override { }

  virtual void enterExpression2a(MemgraphCypher::Expression2aContext * /*ctx*/) override { }
  virtual void exitExpression2a(MemgraphCypher::Expression2aContext * /*ctx*/) override { }

  virtual void enterExpression2b(MemgraphCypher::Expression2bContext * /*ctx*/) override { }
  virtual void exitExpression2b(MemgraphCypher::Expression2bContext * /*ctx*/) override { }

  virtual void enterAtom(MemgraphCypher::AtomContext * /*ctx*/) override { }
  virtual void exitAtom(MemgraphCypher::AtomContext * /*ctx*/) override { }

  virtual void enterLiteral(MemgraphCypher::LiteralContext * /*ctx*/) override { }
  virtual void exitLiteral(MemgraphCypher::LiteralContext * /*ctx*/) override { }

  virtual void enterBooleanLiteral(MemgraphCypher::BooleanLiteralContext * /*ctx*/) override { }
  virtual void exitBooleanLiteral(MemgraphCypher::BooleanLiteralContext * /*ctx*/) override { }

  virtual void enterListLiteral(MemgraphCypher::ListLiteralContext * /*ctx*/) override { }
  virtual void exitListLiteral(MemgraphCypher::ListLiteralContext * /*ctx*/) override { }

  virtual void enterPartialComparisonExpression(MemgraphCypher::PartialComparisonExpressionContext * /*ctx*/) override { }
  virtual void exitPartialComparisonExpression(MemgraphCypher::PartialComparisonExpressionContext * /*ctx*/) override { }

  virtual void enterParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext * /*ctx*/) override { }
  virtual void exitParenthesizedExpression(MemgraphCypher::ParenthesizedExpressionContext * /*ctx*/) override { }

  virtual void enterRelationshipsPattern(MemgraphCypher::RelationshipsPatternContext * /*ctx*/) override { }
  virtual void exitRelationshipsPattern(MemgraphCypher::RelationshipsPatternContext * /*ctx*/) override { }

  virtual void enterFilterExpression(MemgraphCypher::FilterExpressionContext * /*ctx*/) override { }
  virtual void exitFilterExpression(MemgraphCypher::FilterExpressionContext * /*ctx*/) override { }

  virtual void enterReduceExpression(MemgraphCypher::ReduceExpressionContext * /*ctx*/) override { }
  virtual void exitReduceExpression(MemgraphCypher::ReduceExpressionContext * /*ctx*/) override { }

  virtual void enterExtractExpression(MemgraphCypher::ExtractExpressionContext * /*ctx*/) override { }
  virtual void exitExtractExpression(MemgraphCypher::ExtractExpressionContext * /*ctx*/) override { }

  virtual void enterIdInColl(MemgraphCypher::IdInCollContext * /*ctx*/) override { }
  virtual void exitIdInColl(MemgraphCypher::IdInCollContext * /*ctx*/) override { }

  virtual void enterFunctionInvocation(MemgraphCypher::FunctionInvocationContext * /*ctx*/) override { }
  virtual void exitFunctionInvocation(MemgraphCypher::FunctionInvocationContext * /*ctx*/) override { }

  virtual void enterFunctionName(MemgraphCypher::FunctionNameContext * /*ctx*/) override { }
  virtual void exitFunctionName(MemgraphCypher::FunctionNameContext * /*ctx*/) override { }

  virtual void enterListComprehension(MemgraphCypher::ListComprehensionContext * /*ctx*/) override { }
  virtual void exitListComprehension(MemgraphCypher::ListComprehensionContext * /*ctx*/) override { }

  virtual void enterPatternComprehension(MemgraphCypher::PatternComprehensionContext * /*ctx*/) override { }
  virtual void exitPatternComprehension(MemgraphCypher::PatternComprehensionContext * /*ctx*/) override { }

  virtual void enterPropertyLookup(MemgraphCypher::PropertyLookupContext * /*ctx*/) override { }
  virtual void exitPropertyLookup(MemgraphCypher::PropertyLookupContext * /*ctx*/) override { }

  virtual void enterCaseExpression(MemgraphCypher::CaseExpressionContext * /*ctx*/) override { }
  virtual void exitCaseExpression(MemgraphCypher::CaseExpressionContext * /*ctx*/) override { }

  virtual void enterCaseAlternatives(MemgraphCypher::CaseAlternativesContext * /*ctx*/) override { }
  virtual void exitCaseAlternatives(MemgraphCypher::CaseAlternativesContext * /*ctx*/) override { }

  virtual void enterVariable(MemgraphCypher::VariableContext * /*ctx*/) override { }
  virtual void exitVariable(MemgraphCypher::VariableContext * /*ctx*/) override { }

  virtual void enterNumberLiteral(MemgraphCypher::NumberLiteralContext * /*ctx*/) override { }
  virtual void exitNumberLiteral(MemgraphCypher::NumberLiteralContext * /*ctx*/) override { }

  virtual void enterMapLiteral(MemgraphCypher::MapLiteralContext * /*ctx*/) override { }
  virtual void exitMapLiteral(MemgraphCypher::MapLiteralContext * /*ctx*/) override { }

  virtual void enterParameter(MemgraphCypher::ParameterContext * /*ctx*/) override { }
  virtual void exitParameter(MemgraphCypher::ParameterContext * /*ctx*/) override { }

  virtual void enterPropertyExpression(MemgraphCypher::PropertyExpressionContext * /*ctx*/) override { }
  virtual void exitPropertyExpression(MemgraphCypher::PropertyExpressionContext * /*ctx*/) override { }

  virtual void enterPropertyKeyName(MemgraphCypher::PropertyKeyNameContext * /*ctx*/) override { }
  virtual void exitPropertyKeyName(MemgraphCypher::PropertyKeyNameContext * /*ctx*/) override { }

  virtual void enterIntegerLiteral(MemgraphCypher::IntegerLiteralContext * /*ctx*/) override { }
  virtual void exitIntegerLiteral(MemgraphCypher::IntegerLiteralContext * /*ctx*/) override { }

  virtual void enterCreateIndex(MemgraphCypher::CreateIndexContext * /*ctx*/) override { }
  virtual void exitCreateIndex(MemgraphCypher::CreateIndexContext * /*ctx*/) override { }

  virtual void enterDropIndex(MemgraphCypher::DropIndexContext * /*ctx*/) override { }
  virtual void exitDropIndex(MemgraphCypher::DropIndexContext * /*ctx*/) override { }

  virtual void enterDoubleLiteral(MemgraphCypher::DoubleLiteralContext * /*ctx*/) override { }
  virtual void exitDoubleLiteral(MemgraphCypher::DoubleLiteralContext * /*ctx*/) override { }

  virtual void enterCypherKeyword(MemgraphCypher::CypherKeywordContext * /*ctx*/) override { }
  virtual void exitCypherKeyword(MemgraphCypher::CypherKeywordContext * /*ctx*/) override { }


  virtual void enterEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void exitEveryRule(antlr4::ParserRuleContext * /*ctx*/) override { }
  virtual void visitTerminal(antlr4::tree::TerminalNode * /*node*/) override { }
  virtual void visitErrorNode(antlr4::tree::ErrorNode * /*node*/) override { }

};

}  // namespace antlropencypher
