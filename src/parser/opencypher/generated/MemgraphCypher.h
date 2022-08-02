
// Generated from /home/kostas/Desktop/memgraph/src/parser/opencypher/grammar/MemgraphCypher.g4 by ANTLR 4.10.1

#pragma once


#include "antlr4-runtime.h"


namespace antlropencypher {


class  MemgraphCypher : public antlr4::Parser {
public:
  enum {
    UNDERSCORE = 1, AFTER = 2, ALTER = 3, ASYNC = 4, AUTH = 5, BAD = 6, 
    BATCH_INTERVAL = 7, BATCH_LIMIT = 8, BATCH_SIZE = 9, BEFORE = 10, BOOTSTRAP_SERVERS = 11, 
    CHECK = 12, CLEAR = 13, COMMIT = 14, COMMITTED = 15, CONFIG = 16, CONFIGS = 17, 
    CONSUMER_GROUP = 18, CREDENTIALS = 19, CSV = 20, DATA = 21, DELIMITER = 22, 
    DATABASE = 23, DENY = 24, DIRECTORY = 25, DROP = 26, DUMP = 27, DURABILITY = 28, 
    EXECUTE = 29, FOR = 30, FOREACH = 31, FREE = 32, FREE_MEMORY = 33, FROM = 34, 
    GLOBAL = 35, GRANT = 36, GRANTS = 37, HEADER = 38, IDENTIFIED = 39, 
    IGNORE = 40, ISOLATION = 41, KAFKA = 42, LEVEL = 43, LOAD = 44, LOCK = 45, 
    MAIN = 46, MODE = 47, MODULE_READ = 48, MODULE_WRITE = 49, NEXT = 50, 
    NO = 51, PASSWORD = 52, PORT = 53, PRIVILEGES = 54, PULSAR = 55, READ = 56, 
    READ_FILE = 57, REGISTER = 58, REPLICA = 59, REPLICAS = 60, REPLICATION = 61, 
    REVOKE = 62, ROLE = 63, ROLES = 64, QUOTE = 65, SERVICE_URL = 66, SESSION = 67, 
    SETTING = 68, SETTINGS = 69, SNAPSHOT = 70, START = 71, STATS = 72, 
    STOP = 73, STREAM = 74, STREAMS = 75, SYNC = 76, TIMEOUT = 77, TO = 78, 
    TOPICS = 79, TRANSACTION = 80, TRANSFORM = 81, TRIGGER = 82, TRIGGERS = 83, 
    UNCOMMITTED = 84, UNLOCK = 85, UPDATE = 86, USER = 87, USERS = 88, VERSION = 89, 
    WEBSOCKET = 90, Skipped = 91, LPAREN = 92, RPAREN = 93, LBRACK = 94, 
    RBRACK = 95, LBRACE = 96, RBRACE = 97, COMMA = 98, DOT = 99, DOTS = 100, 
    COLON = 101, SEMICOLON = 102, DOLLAR = 103, PIPE = 104, EQ = 105, LT = 106, 
    GT = 107, LTE = 108, GTE = 109, NEQ1 = 110, NEQ2 = 111, SIM = 112, PLUS = 113, 
    MINUS = 114, ASTERISK = 115, SLASH = 116, PERCENT = 117, CARET = 118, 
    PLUS_EQ = 119, LeftArrowHeadPart = 120, RightArrowHeadPart = 121, DashPart = 122, 
    ALL = 123, AND = 124, ANY = 125, AS = 126, ASC = 127, ASCENDING = 128, 
    ASSERT = 129, BFS = 130, BY = 131, CALL = 132, CASE = 133, COALESCE = 134, 
    CONSTRAINT = 135, CONTAINS = 136, COUNT = 137, CREATE = 138, CYPHERNULL = 139, 
    DELETE = 140, DESC = 141, DESCENDING = 142, DETACH = 143, DISTINCT = 144, 
    ELSE = 145, END = 146, ENDS = 147, EXISTS = 148, EXPLAIN = 149, EXTRACT = 150, 
    FALSE = 151, FILTER = 152, IN = 153, INDEX = 154, INFO = 155, IS = 156, 
    KB = 157, KEY = 158, LIMIT = 159, L_SKIP = 160, MATCH = 161, MB = 162, 
    MEMORY = 163, MERGE = 164, NODE = 165, NONE = 166, NOT = 167, ON = 168, 
    OPTIONAL = 169, OR = 170, ORDER = 171, PROCEDURE = 172, PROFILE = 173, 
    QUERY = 174, REDUCE = 175, REMOVE = 176, RETURN = 177, SET = 178, SHOW = 179, 
    SINGLE = 180, STARTS = 181, STORAGE = 182, THEN = 183, TRUE = 184, UNION = 185, 
    UNIQUE = 186, UNLIMITED = 187, UNWIND = 188, WHEN = 189, WHERE = 190, 
    WITH = 191, WSHORTEST = 192, XOR = 193, YIELD = 194, StringLiteral = 195, 
    DecimalLiteral = 196, OctalLiteral = 197, HexadecimalLiteral = 198, 
    FloatingLiteral = 199, UnescapedSymbolicName = 200, EscapedSymbolicName = 201, 
    IdentifierStart = 202, IdentifierPart = 203
  };

  enum {
    RuleMemgraphCypherKeyword = 0, RuleSymbolicName = 1, RuleQuery = 2, 
    RuleAuthQuery = 3, RuleReplicationQuery = 4, RuleTriggerQuery = 5, RuleClause = 6, 
    RuleUpdateClause = 7, RuleForeach = 8, RuleStreamQuery = 9, RuleSettingQuery = 10, 
    RuleLoadCsv = 11, RuleCsvFile = 12, RuleDelimiter = 13, RuleQuote = 14, 
    RuleRowVar = 15, RuleUserOrRoleName = 16, RuleCreateRole = 17, RuleDropRole = 18, 
    RuleShowRoles = 19, RuleCreateUser = 20, RuleSetPassword = 21, RuleDropUser = 22, 
    RuleShowUsers = 23, RuleSetRole = 24, RuleClearRole = 25, RuleGrantPrivilege = 26, 
    RuleDenyPrivilege = 27, RuleRevokePrivilege = 28, RulePrivilege = 29, 
    RulePrivilegeList = 30, RuleShowPrivileges = 31, RuleShowRoleForUser = 32, 
    RuleShowUsersForRole = 33, RuleDumpQuery = 34, RuleSetReplicationRole = 35, 
    RuleShowReplicationRole = 36, RuleReplicaName = 37, RuleSocketAddress = 38, 
    RuleRegisterReplica = 39, RuleDropReplica = 40, RuleShowReplicas = 41, 
    RuleLockPathQuery = 42, RuleFreeMemoryQuery = 43, RuleTriggerName = 44, 
    RuleTriggerStatement = 45, RuleEmptyVertex = 46, RuleEmptyEdge = 47, 
    RuleCreateTrigger = 48, RuleDropTrigger = 49, RuleShowTriggers = 50, 
    RuleIsolationLevel = 51, RuleIsolationLevelScope = 52, RuleIsolationLevelQuery = 53, 
    RuleCreateSnapshotQuery = 54, RuleStreamName = 55, RuleSymbolicNameWithMinus = 56, 
    RuleSymbolicNameWithDotsAndMinus = 57, RuleSymbolicTopicNames = 58, 
    RuleTopicNames = 59, RuleCommonCreateStreamConfig = 60, RuleCreateStream = 61, 
    RuleConfigKeyValuePair = 62, RuleConfigMap = 63, RuleKafkaCreateStreamConfig = 64, 
    RuleKafkaCreateStream = 65, RulePulsarCreateStreamConfig = 66, RulePulsarCreateStream = 67, 
    RuleDropStream = 68, RuleStartStream = 69, RuleStartAllStreams = 70, 
    RuleStopStream = 71, RuleStopAllStreams = 72, RuleShowStreams = 73, 
    RuleCheckStream = 74, RuleSettingName = 75, RuleSettingValue = 76, RuleSetSetting = 77, 
    RuleShowSetting = 78, RuleShowSettings = 79, RuleVersionQuery = 80, 
    RuleCypher = 81, RuleStatement = 82, RuleConstraintQuery = 83, RuleConstraint = 84, 
    RuleConstraintPropertyList = 85, RuleStorageInfo = 86, RuleIndexInfo = 87, 
    RuleConstraintInfo = 88, RuleInfoQuery = 89, RuleExplainQuery = 90, 
    RuleProfileQuery = 91, RuleCypherQuery = 92, RuleIndexQuery = 93, RuleSingleQuery = 94, 
    RuleCypherUnion = 95, RuleCypherMatch = 96, RuleUnwind = 97, RuleMerge = 98, 
    RuleMergeAction = 99, RuleCreate = 100, RuleSet = 101, RuleSetItem = 102, 
    RuleCypherDelete = 103, RuleRemove = 104, RuleRemoveItem = 105, RuleWith = 106, 
    RuleCypherReturn = 107, RuleCallProcedure = 108, RuleProcedureName = 109, 
    RuleYieldProcedureResults = 110, RuleMemoryLimit = 111, RuleQueryMemoryLimit = 112, 
    RuleProcedureMemoryLimit = 113, RuleProcedureResult = 114, RuleReturnBody = 115, 
    RuleReturnItems = 116, RuleReturnItem = 117, RuleOrder = 118, RuleSkip = 119, 
    RuleLimit = 120, RuleSortItem = 121, RuleWhere = 122, RulePattern = 123, 
    RulePatternPart = 124, RuleAnonymousPatternPart = 125, RulePatternElement = 126, 
    RuleNodePattern = 127, RulePatternElementChain = 128, RuleRelationshipPattern = 129, 
    RuleLeftArrowHead = 130, RuleRightArrowHead = 131, RuleDash = 132, RuleRelationshipDetail = 133, 
    RuleRelationshipLambda = 134, RuleVariableExpansion = 135, RuleProperties = 136, 
    RuleRelationshipTypes = 137, RuleNodeLabels = 138, RuleNodeLabel = 139, 
    RuleLabelName = 140, RuleRelTypeName = 141, RuleExpression = 142, RuleExpression12 = 143, 
    RuleExpression11 = 144, RuleExpression10 = 145, RuleExpression9 = 146, 
    RuleExpression8 = 147, RuleExpression7 = 148, RuleExpression6 = 149, 
    RuleExpression5 = 150, RuleExpression4 = 151, RuleExpression3a = 152, 
    RuleStringAndNullOperators = 153, RuleExpression3b = 154, RuleListIndexingOrSlicing = 155, 
    RuleExpression2a = 156, RuleExpression2b = 157, RuleAtom = 158, RuleLiteral = 159, 
    RuleBooleanLiteral = 160, RuleListLiteral = 161, RulePartialComparisonExpression = 162, 
    RuleParenthesizedExpression = 163, RuleRelationshipsPattern = 164, RuleFilterExpression = 165, 
    RuleReduceExpression = 166, RuleExtractExpression = 167, RuleIdInColl = 168, 
    RuleFunctionInvocation = 169, RuleFunctionName = 170, RuleListComprehension = 171, 
    RulePatternComprehension = 172, RulePropertyLookup = 173, RuleCaseExpression = 174, 
    RuleCaseAlternatives = 175, RuleVariable = 176, RuleNumberLiteral = 177, 
    RuleMapLiteral = 178, RuleParameter = 179, RulePropertyExpression = 180, 
    RulePropertyKeyName = 181, RuleIntegerLiteral = 182, RuleCreateIndex = 183, 
    RuleDropIndex = 184, RuleDoubleLiteral = 185, RuleCypherKeyword = 186
  };

  explicit MemgraphCypher(antlr4::TokenStream *input);

  MemgraphCypher(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~MemgraphCypher() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


  class MemgraphCypherKeywordContext;
  class SymbolicNameContext;
  class QueryContext;
  class AuthQueryContext;
  class ReplicationQueryContext;
  class TriggerQueryContext;
  class ClauseContext;
  class UpdateClauseContext;
  class ForeachContext;
  class StreamQueryContext;
  class SettingQueryContext;
  class LoadCsvContext;
  class CsvFileContext;
  class DelimiterContext;
  class QuoteContext;
  class RowVarContext;
  class UserOrRoleNameContext;
  class CreateRoleContext;
  class DropRoleContext;
  class ShowRolesContext;
  class CreateUserContext;
  class SetPasswordContext;
  class DropUserContext;
  class ShowUsersContext;
  class SetRoleContext;
  class ClearRoleContext;
  class GrantPrivilegeContext;
  class DenyPrivilegeContext;
  class RevokePrivilegeContext;
  class PrivilegeContext;
  class PrivilegeListContext;
  class ShowPrivilegesContext;
  class ShowRoleForUserContext;
  class ShowUsersForRoleContext;
  class DumpQueryContext;
  class SetReplicationRoleContext;
  class ShowReplicationRoleContext;
  class ReplicaNameContext;
  class SocketAddressContext;
  class RegisterReplicaContext;
  class DropReplicaContext;
  class ShowReplicasContext;
  class LockPathQueryContext;
  class FreeMemoryQueryContext;
  class TriggerNameContext;
  class TriggerStatementContext;
  class EmptyVertexContext;
  class EmptyEdgeContext;
  class CreateTriggerContext;
  class DropTriggerContext;
  class ShowTriggersContext;
  class IsolationLevelContext;
  class IsolationLevelScopeContext;
  class IsolationLevelQueryContext;
  class CreateSnapshotQueryContext;
  class StreamNameContext;
  class SymbolicNameWithMinusContext;
  class SymbolicNameWithDotsAndMinusContext;
  class SymbolicTopicNamesContext;
  class TopicNamesContext;
  class CommonCreateStreamConfigContext;
  class CreateStreamContext;
  class ConfigKeyValuePairContext;
  class ConfigMapContext;
  class KafkaCreateStreamConfigContext;
  class KafkaCreateStreamContext;
  class PulsarCreateStreamConfigContext;
  class PulsarCreateStreamContext;
  class DropStreamContext;
  class StartStreamContext;
  class StartAllStreamsContext;
  class StopStreamContext;
  class StopAllStreamsContext;
  class ShowStreamsContext;
  class CheckStreamContext;
  class SettingNameContext;
  class SettingValueContext;
  class SetSettingContext;
  class ShowSettingContext;
  class ShowSettingsContext;
  class VersionQueryContext;
  class CypherContext;
  class StatementContext;
  class ConstraintQueryContext;
  class ConstraintContext;
  class ConstraintPropertyListContext;
  class StorageInfoContext;
  class IndexInfoContext;
  class ConstraintInfoContext;
  class InfoQueryContext;
  class ExplainQueryContext;
  class ProfileQueryContext;
  class CypherQueryContext;
  class IndexQueryContext;
  class SingleQueryContext;
  class CypherUnionContext;
  class CypherMatchContext;
  class UnwindContext;
  class MergeContext;
  class MergeActionContext;
  class CreateContext;
  class SetContext;
  class SetItemContext;
  class CypherDeleteContext;
  class RemoveContext;
  class RemoveItemContext;
  class WithContext;
  class CypherReturnContext;
  class CallProcedureContext;
  class ProcedureNameContext;
  class YieldProcedureResultsContext;
  class MemoryLimitContext;
  class QueryMemoryLimitContext;
  class ProcedureMemoryLimitContext;
  class ProcedureResultContext;
  class ReturnBodyContext;
  class ReturnItemsContext;
  class ReturnItemContext;
  class OrderContext;
  class SkipContext;
  class LimitContext;
  class SortItemContext;
  class WhereContext;
  class PatternContext;
  class PatternPartContext;
  class AnonymousPatternPartContext;
  class PatternElementContext;
  class NodePatternContext;
  class PatternElementChainContext;
  class RelationshipPatternContext;
  class LeftArrowHeadContext;
  class RightArrowHeadContext;
  class DashContext;
  class RelationshipDetailContext;
  class RelationshipLambdaContext;
  class VariableExpansionContext;
  class PropertiesContext;
  class RelationshipTypesContext;
  class NodeLabelsContext;
  class NodeLabelContext;
  class LabelNameContext;
  class RelTypeNameContext;
  class ExpressionContext;
  class Expression12Context;
  class Expression11Context;
  class Expression10Context;
  class Expression9Context;
  class Expression8Context;
  class Expression7Context;
  class Expression6Context;
  class Expression5Context;
  class Expression4Context;
  class Expression3aContext;
  class StringAndNullOperatorsContext;
  class Expression3bContext;
  class ListIndexingOrSlicingContext;
  class Expression2aContext;
  class Expression2bContext;
  class AtomContext;
  class LiteralContext;
  class BooleanLiteralContext;
  class ListLiteralContext;
  class PartialComparisonExpressionContext;
  class ParenthesizedExpressionContext;
  class RelationshipsPatternContext;
  class FilterExpressionContext;
  class ReduceExpressionContext;
  class ExtractExpressionContext;
  class IdInCollContext;
  class FunctionInvocationContext;
  class FunctionNameContext;
  class ListComprehensionContext;
  class PatternComprehensionContext;
  class PropertyLookupContext;
  class CaseExpressionContext;
  class CaseAlternativesContext;
  class VariableContext;
  class NumberLiteralContext;
  class MapLiteralContext;
  class ParameterContext;
  class PropertyExpressionContext;
  class PropertyKeyNameContext;
  class IntegerLiteralContext;
  class CreateIndexContext;
  class DropIndexContext;
  class DoubleLiteralContext;
  class CypherKeywordContext; 

  class  MemgraphCypherKeywordContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypherKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CypherKeywordContext *cypherKeyword();
    antlr4::tree::TerminalNode *AFTER();
    antlr4::tree::TerminalNode *ALTER();
    antlr4::tree::TerminalNode *ASYNC();
    antlr4::tree::TerminalNode *AUTH();
    antlr4::tree::TerminalNode *BAD();
    antlr4::tree::TerminalNode *BATCH_INTERVAL();
    antlr4::tree::TerminalNode *BATCH_LIMIT();
    antlr4::tree::TerminalNode *BATCH_SIZE();
    antlr4::tree::TerminalNode *BEFORE();
    antlr4::tree::TerminalNode *BOOTSTRAP_SERVERS();
    antlr4::tree::TerminalNode *CHECK();
    antlr4::tree::TerminalNode *CLEAR();
    antlr4::tree::TerminalNode *COMMIT();
    antlr4::tree::TerminalNode *COMMITTED();
    antlr4::tree::TerminalNode *CONFIG();
    antlr4::tree::TerminalNode *CONFIGS();
    antlr4::tree::TerminalNode *CONSUMER_GROUP();
    antlr4::tree::TerminalNode *CREDENTIALS();
    antlr4::tree::TerminalNode *CSV();
    antlr4::tree::TerminalNode *DATA();
    antlr4::tree::TerminalNode *DELIMITER();
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *DENY();
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *DUMP();
    antlr4::tree::TerminalNode *EXECUTE();
    antlr4::tree::TerminalNode *FOR();
    antlr4::tree::TerminalNode *FOREACH();
    antlr4::tree::TerminalNode *FREE();
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *GLOBAL();
    antlr4::tree::TerminalNode *GRANT();
    antlr4::tree::TerminalNode *HEADER();
    antlr4::tree::TerminalNode *IDENTIFIED();
    antlr4::tree::TerminalNode *ISOLATION();
    antlr4::tree::TerminalNode *KAFKA();
    antlr4::tree::TerminalNode *LEVEL();
    antlr4::tree::TerminalNode *LOAD();
    antlr4::tree::TerminalNode *LOCK();
    antlr4::tree::TerminalNode *MAIN();
    antlr4::tree::TerminalNode *MODE();
    antlr4::tree::TerminalNode *NEXT();
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *PASSWORD();
    antlr4::tree::TerminalNode *PULSAR();
    antlr4::tree::TerminalNode *PORT();
    antlr4::tree::TerminalNode *PRIVILEGES();
    antlr4::tree::TerminalNode *READ();
    antlr4::tree::TerminalNode *REGISTER();
    antlr4::tree::TerminalNode *REPLICA();
    antlr4::tree::TerminalNode *REPLICAS();
    antlr4::tree::TerminalNode *REPLICATION();
    antlr4::tree::TerminalNode *REVOKE();
    antlr4::tree::TerminalNode *ROLE();
    antlr4::tree::TerminalNode *ROLES();
    antlr4::tree::TerminalNode *QUOTE();
    antlr4::tree::TerminalNode *SESSION();
    antlr4::tree::TerminalNode *SETTING();
    antlr4::tree::TerminalNode *SETTINGS();
    antlr4::tree::TerminalNode *SNAPSHOT();
    antlr4::tree::TerminalNode *START();
    antlr4::tree::TerminalNode *STATS();
    antlr4::tree::TerminalNode *STREAM();
    antlr4::tree::TerminalNode *STREAMS();
    antlr4::tree::TerminalNode *SYNC();
    antlr4::tree::TerminalNode *TIMEOUT();
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *TOPICS();
    antlr4::tree::TerminalNode *TRANSACTION();
    antlr4::tree::TerminalNode *TRANSFORM();
    antlr4::tree::TerminalNode *TRIGGER();
    antlr4::tree::TerminalNode *TRIGGERS();
    antlr4::tree::TerminalNode *UNCOMMITTED();
    antlr4::tree::TerminalNode *UNLOCK();
    antlr4::tree::TerminalNode *UPDATE();
    antlr4::tree::TerminalNode *USER();
    antlr4::tree::TerminalNode *USERS();
    antlr4::tree::TerminalNode *VERSION();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MemgraphCypherKeywordContext* memgraphCypherKeyword();

  class  SymbolicNameContext : public antlr4::ParserRuleContext {
  public:
    SymbolicNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UnescapedSymbolicName();
    antlr4::tree::TerminalNode *EscapedSymbolicName();
    MemgraphCypherKeywordContext *memgraphCypherKeyword();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SymbolicNameContext* symbolicName();

  class  QueryContext : public antlr4::ParserRuleContext {
  public:
    QueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CypherQueryContext *cypherQuery();
    IndexQueryContext *indexQuery();
    ExplainQueryContext *explainQuery();
    ProfileQueryContext *profileQuery();
    InfoQueryContext *infoQuery();
    ConstraintQueryContext *constraintQuery();
    AuthQueryContext *authQuery();
    DumpQueryContext *dumpQuery();
    ReplicationQueryContext *replicationQuery();
    LockPathQueryContext *lockPathQuery();
    FreeMemoryQueryContext *freeMemoryQuery();
    TriggerQueryContext *triggerQuery();
    IsolationLevelQueryContext *isolationLevelQuery();
    CreateSnapshotQueryContext *createSnapshotQuery();
    StreamQueryContext *streamQuery();
    SettingQueryContext *settingQuery();
    VersionQueryContext *versionQuery();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  QueryContext* query();

  class  AuthQueryContext : public antlr4::ParserRuleContext {
  public:
    AuthQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateRoleContext *createRole();
    DropRoleContext *dropRole();
    ShowRolesContext *showRoles();
    CreateUserContext *createUser();
    SetPasswordContext *setPassword();
    DropUserContext *dropUser();
    ShowUsersContext *showUsers();
    SetRoleContext *setRole();
    ClearRoleContext *clearRole();
    GrantPrivilegeContext *grantPrivilege();
    DenyPrivilegeContext *denyPrivilege();
    RevokePrivilegeContext *revokePrivilege();
    ShowPrivilegesContext *showPrivileges();
    ShowRoleForUserContext *showRoleForUser();
    ShowUsersForRoleContext *showUsersForRole();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AuthQueryContext* authQuery();

  class  ReplicationQueryContext : public antlr4::ParserRuleContext {
  public:
    ReplicationQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SetReplicationRoleContext *setReplicationRole();
    ShowReplicationRoleContext *showReplicationRole();
    RegisterReplicaContext *registerReplica();
    DropReplicaContext *dropReplica();
    ShowReplicasContext *showReplicas();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ReplicationQueryContext* replicationQuery();

  class  TriggerQueryContext : public antlr4::ParserRuleContext {
  public:
    TriggerQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateTriggerContext *createTrigger();
    DropTriggerContext *dropTrigger();
    ShowTriggersContext *showTriggers();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TriggerQueryContext* triggerQuery();

  class  ClauseContext : public antlr4::ParserRuleContext {
  public:
    ClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CypherMatchContext *cypherMatch();
    UnwindContext *unwind();
    MergeContext *merge();
    CreateContext *create();
    SetContext *set();
    CypherDeleteContext *cypherDelete();
    RemoveContext *remove();
    WithContext *with();
    CypherReturnContext *cypherReturn();
    CallProcedureContext *callProcedure();
    LoadCsvContext *loadCsv();
    ForeachContext *foreach();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ClauseContext* clause();

  class  UpdateClauseContext : public antlr4::ParserRuleContext {
  public:
    UpdateClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SetContext *set();
    RemoveContext *remove();
    CreateContext *create();
    MergeContext *merge();
    CypherDeleteContext *cypherDelete();
    ForeachContext *foreach();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  UpdateClauseContext* updateClause();

  class  ForeachContext : public antlr4::ParserRuleContext {
  public:
    ForeachContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FOREACH();
    antlr4::tree::TerminalNode *LPAREN();
    VariableContext *variable();
    antlr4::tree::TerminalNode *IN();
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *PIPE();
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<UpdateClauseContext *> updateClause();
    UpdateClauseContext* updateClause(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ForeachContext* foreach();

  class  StreamQueryContext : public antlr4::ParserRuleContext {
  public:
    StreamQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CheckStreamContext *checkStream();
    CreateStreamContext *createStream();
    DropStreamContext *dropStream();
    StartStreamContext *startStream();
    StartAllStreamsContext *startAllStreams();
    StopStreamContext *stopStream();
    StopAllStreamsContext *stopAllStreams();
    ShowStreamsContext *showStreams();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StreamQueryContext* streamQuery();

  class  SettingQueryContext : public antlr4::ParserRuleContext {
  public:
    SettingQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SetSettingContext *setSetting();
    ShowSettingContext *showSetting();
    ShowSettingsContext *showSettings();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SettingQueryContext* settingQuery();

  class  LoadCsvContext : public antlr4::ParserRuleContext {
  public:
    LoadCsvContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOAD();
    antlr4::tree::TerminalNode *CSV();
    antlr4::tree::TerminalNode *FROM();
    CsvFileContext *csvFile();
    antlr4::tree::TerminalNode *HEADER();
    antlr4::tree::TerminalNode *AS();
    RowVarContext *rowVar();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *NO();
    antlr4::tree::TerminalNode *IGNORE();
    antlr4::tree::TerminalNode *BAD();
    antlr4::tree::TerminalNode *DELIMITER();
    DelimiterContext *delimiter();
    antlr4::tree::TerminalNode *QUOTE();
    QuoteContext *quote();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LoadCsvContext* loadCsv();

  class  CsvFileContext : public antlr4::ParserRuleContext {
  public:
    CsvFileContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CsvFileContext* csvFile();

  class  DelimiterContext : public antlr4::ParserRuleContext {
  public:
    DelimiterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DelimiterContext* delimiter();

  class  QuoteContext : public antlr4::ParserRuleContext {
  public:
    QuoteContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  QuoteContext* quote();

  class  RowVarContext : public antlr4::ParserRuleContext {
  public:
    RowVarContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VariableContext *variable();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RowVarContext* rowVar();

  class  UserOrRoleNameContext : public antlr4::ParserRuleContext {
  public:
    UserOrRoleNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  UserOrRoleNameContext* userOrRoleName();

  class  CreateRoleContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *role = nullptr;
    CreateRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *ROLE();
    UserOrRoleNameContext *userOrRoleName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CreateRoleContext* createRole();

  class  DropRoleContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *role = nullptr;
    DropRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *ROLE();
    UserOrRoleNameContext *userOrRoleName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DropRoleContext* dropRole();

  class  ShowRolesContext : public antlr4::ParserRuleContext {
  public:
    ShowRolesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *ROLES();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowRolesContext* showRoles();

  class  CreateUserContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *user = nullptr;
    MemgraphCypher::LiteralContext *password = nullptr;
    CreateUserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *USER();
    UserOrRoleNameContext *userOrRoleName();
    antlr4::tree::TerminalNode *IDENTIFIED();
    antlr4::tree::TerminalNode *BY();
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CreateUserContext* createUser();

  class  SetPasswordContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *user = nullptr;
    MemgraphCypher::LiteralContext *password = nullptr;
    SetPasswordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *PASSWORD();
    antlr4::tree::TerminalNode *FOR();
    antlr4::tree::TerminalNode *TO();
    UserOrRoleNameContext *userOrRoleName();
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SetPasswordContext* setPassword();

  class  DropUserContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *user = nullptr;
    DropUserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *USER();
    UserOrRoleNameContext *userOrRoleName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DropUserContext* dropUser();

  class  ShowUsersContext : public antlr4::ParserRuleContext {
  public:
    ShowUsersContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *USERS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowUsersContext* showUsers();

  class  SetRoleContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *user = nullptr;
    MemgraphCypher::UserOrRoleNameContext *role = nullptr;
    SetRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *ROLE();
    antlr4::tree::TerminalNode *FOR();
    antlr4::tree::TerminalNode *TO();
    std::vector<UserOrRoleNameContext *> userOrRoleName();
    UserOrRoleNameContext* userOrRoleName(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SetRoleContext* setRole();

  class  ClearRoleContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *user = nullptr;
    ClearRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CLEAR();
    antlr4::tree::TerminalNode *ROLE();
    antlr4::tree::TerminalNode *FOR();
    UserOrRoleNameContext *userOrRoleName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ClearRoleContext* clearRole();

  class  GrantPrivilegeContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::PrivilegeListContext *privileges = nullptr;
    MemgraphCypher::UserOrRoleNameContext *userOrRole = nullptr;
    GrantPrivilegeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GRANT();
    antlr4::tree::TerminalNode *TO();
    UserOrRoleNameContext *userOrRoleName();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *PRIVILEGES();
    PrivilegeListContext *privilegeList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  GrantPrivilegeContext* grantPrivilege();

  class  DenyPrivilegeContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::PrivilegeListContext *privileges = nullptr;
    MemgraphCypher::UserOrRoleNameContext *userOrRole = nullptr;
    DenyPrivilegeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DENY();
    antlr4::tree::TerminalNode *TO();
    UserOrRoleNameContext *userOrRoleName();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *PRIVILEGES();
    PrivilegeListContext *privilegeList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DenyPrivilegeContext* denyPrivilege();

  class  RevokePrivilegeContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::PrivilegeListContext *privileges = nullptr;
    MemgraphCypher::UserOrRoleNameContext *userOrRole = nullptr;
    RevokePrivilegeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REVOKE();
    antlr4::tree::TerminalNode *FROM();
    UserOrRoleNameContext *userOrRoleName();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *PRIVILEGES();
    PrivilegeListContext *privilegeList();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RevokePrivilegeContext* revokePrivilege();

  class  PrivilegeContext : public antlr4::ParserRuleContext {
  public:
    PrivilegeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *MATCH();
    antlr4::tree::TerminalNode *MERGE();
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *REMOVE();
    antlr4::tree::TerminalNode *INDEX();
    antlr4::tree::TerminalNode *STATS();
    antlr4::tree::TerminalNode *AUTH();
    antlr4::tree::TerminalNode *CONSTRAINT();
    antlr4::tree::TerminalNode *DUMP();
    antlr4::tree::TerminalNode *REPLICATION();
    antlr4::tree::TerminalNode *READ_FILE();
    antlr4::tree::TerminalNode *FREE_MEMORY();
    antlr4::tree::TerminalNode *TRIGGER();
    antlr4::tree::TerminalNode *CONFIG();
    antlr4::tree::TerminalNode *DURABILITY();
    antlr4::tree::TerminalNode *STREAM();
    antlr4::tree::TerminalNode *MODULE_READ();
    antlr4::tree::TerminalNode *MODULE_WRITE();
    antlr4::tree::TerminalNode *WEBSOCKET();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PrivilegeContext* privilege();

  class  PrivilegeListContext : public antlr4::ParserRuleContext {
  public:
    PrivilegeListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PrivilegeContext *> privilege();
    PrivilegeContext* privilege(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PrivilegeListContext* privilegeList();

  class  ShowPrivilegesContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *userOrRole = nullptr;
    ShowPrivilegesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *PRIVILEGES();
    antlr4::tree::TerminalNode *FOR();
    UserOrRoleNameContext *userOrRoleName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowPrivilegesContext* showPrivileges();

  class  ShowRoleForUserContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *user = nullptr;
    ShowRoleForUserContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *ROLE();
    antlr4::tree::TerminalNode *FOR();
    UserOrRoleNameContext *userOrRoleName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowRoleForUserContext* showRoleForUser();

  class  ShowUsersForRoleContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::UserOrRoleNameContext *role = nullptr;
    ShowUsersForRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *USERS();
    antlr4::tree::TerminalNode *FOR();
    UserOrRoleNameContext *userOrRoleName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowUsersForRoleContext* showUsersForRole();

  class  DumpQueryContext : public antlr4::ParserRuleContext {
  public:
    DumpQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DUMP();
    antlr4::tree::TerminalNode *DATABASE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DumpQueryContext* dumpQuery();

  class  SetReplicationRoleContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::LiteralContext *port = nullptr;
    SetReplicationRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *REPLICATION();
    antlr4::tree::TerminalNode *ROLE();
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *MAIN();
    antlr4::tree::TerminalNode *REPLICA();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *PORT();
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SetReplicationRoleContext* setReplicationRole();

  class  ShowReplicationRoleContext : public antlr4::ParserRuleContext {
  public:
    ShowReplicationRoleContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *REPLICATION();
    antlr4::tree::TerminalNode *ROLE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowReplicationRoleContext* showReplicationRole();

  class  ReplicaNameContext : public antlr4::ParserRuleContext {
  public:
    ReplicaNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ReplicaNameContext* replicaName();

  class  SocketAddressContext : public antlr4::ParserRuleContext {
  public:
    SocketAddressContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SocketAddressContext* socketAddress();

  class  RegisterReplicaContext : public antlr4::ParserRuleContext {
  public:
    RegisterReplicaContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REGISTER();
    antlr4::tree::TerminalNode *REPLICA();
    ReplicaNameContext *replicaName();
    antlr4::tree::TerminalNode *TO();
    SocketAddressContext *socketAddress();
    antlr4::tree::TerminalNode *SYNC();
    antlr4::tree::TerminalNode *ASYNC();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RegisterReplicaContext* registerReplica();

  class  DropReplicaContext : public antlr4::ParserRuleContext {
  public:
    DropReplicaContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *REPLICA();
    ReplicaNameContext *replicaName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DropReplicaContext* dropReplica();

  class  ShowReplicasContext : public antlr4::ParserRuleContext {
  public:
    ShowReplicasContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *REPLICAS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowReplicasContext* showReplicas();

  class  LockPathQueryContext : public antlr4::ParserRuleContext {
  public:
    LockPathQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DATA();
    antlr4::tree::TerminalNode *DIRECTORY();
    antlr4::tree::TerminalNode *LOCK();
    antlr4::tree::TerminalNode *UNLOCK();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LockPathQueryContext* lockPathQuery();

  class  FreeMemoryQueryContext : public antlr4::ParserRuleContext {
  public:
    FreeMemoryQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FREE();
    antlr4::tree::TerminalNode *MEMORY();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FreeMemoryQueryContext* freeMemoryQuery();

  class  TriggerNameContext : public antlr4::ParserRuleContext {
  public:
    TriggerNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TriggerNameContext* triggerName();

  class  TriggerStatementContext : public antlr4::ParserRuleContext {
  public:
    TriggerStatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TriggerStatementContext* triggerStatement();

  class  EmptyVertexContext : public antlr4::ParserRuleContext {
  public:
    EmptyVertexContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  EmptyVertexContext* emptyVertex();

  class  EmptyEdgeContext : public antlr4::ParserRuleContext {
  public:
    EmptyEdgeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<DashContext *> dash();
    DashContext* dash(size_t i);
    RightArrowHeadContext *rightArrowHead();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  EmptyEdgeContext* emptyEdge();

  class  CreateTriggerContext : public antlr4::ParserRuleContext {
  public:
    CreateTriggerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> CREATE();
    antlr4::tree::TerminalNode* CREATE(size_t i);
    antlr4::tree::TerminalNode *TRIGGER();
    TriggerNameContext *triggerName();
    antlr4::tree::TerminalNode *COMMIT();
    antlr4::tree::TerminalNode *EXECUTE();
    TriggerStatementContext *triggerStatement();
    antlr4::tree::TerminalNode *AFTER();
    antlr4::tree::TerminalNode *BEFORE();
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *UPDATE();
    antlr4::tree::TerminalNode *DELETE();
    EmptyVertexContext *emptyVertex();
    EmptyEdgeContext *emptyEdge();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CreateTriggerContext* createTrigger();

  class  DropTriggerContext : public antlr4::ParserRuleContext {
  public:
    DropTriggerContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *TRIGGER();
    TriggerNameContext *triggerName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DropTriggerContext* dropTrigger();

  class  ShowTriggersContext : public antlr4::ParserRuleContext {
  public:
    ShowTriggersContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *TRIGGERS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowTriggersContext* showTriggers();

  class  IsolationLevelContext : public antlr4::ParserRuleContext {
  public:
    IsolationLevelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SNAPSHOT();
    antlr4::tree::TerminalNode *ISOLATION();
    antlr4::tree::TerminalNode *READ();
    antlr4::tree::TerminalNode *COMMITTED();
    antlr4::tree::TerminalNode *UNCOMMITTED();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IsolationLevelContext* isolationLevel();

  class  IsolationLevelScopeContext : public antlr4::ParserRuleContext {
  public:
    IsolationLevelScopeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GLOBAL();
    antlr4::tree::TerminalNode *SESSION();
    antlr4::tree::TerminalNode *NEXT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IsolationLevelScopeContext* isolationLevelScope();

  class  IsolationLevelQueryContext : public antlr4::ParserRuleContext {
  public:
    IsolationLevelQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    IsolationLevelScopeContext *isolationLevelScope();
    antlr4::tree::TerminalNode *TRANSACTION();
    antlr4::tree::TerminalNode *ISOLATION();
    antlr4::tree::TerminalNode *LEVEL();
    IsolationLevelContext *isolationLevel();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IsolationLevelQueryContext* isolationLevelQuery();

  class  CreateSnapshotQueryContext : public antlr4::ParserRuleContext {
  public:
    CreateSnapshotQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *SNAPSHOT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CreateSnapshotQueryContext* createSnapshotQuery();

  class  StreamNameContext : public antlr4::ParserRuleContext {
  public:
    StreamNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StreamNameContext* streamName();

  class  SymbolicNameWithMinusContext : public antlr4::ParserRuleContext {
  public:
    SymbolicNameWithMinusContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SymbolicNameContext *> symbolicName();
    SymbolicNameContext* symbolicName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SymbolicNameWithMinusContext* symbolicNameWithMinus();

  class  SymbolicNameWithDotsAndMinusContext : public antlr4::ParserRuleContext {
  public:
    SymbolicNameWithDotsAndMinusContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SymbolicNameWithMinusContext *> symbolicNameWithMinus();
    SymbolicNameWithMinusContext* symbolicNameWithMinus(size_t i);
    std::vector<antlr4::tree::TerminalNode *> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SymbolicNameWithDotsAndMinusContext* symbolicNameWithDotsAndMinus();

  class  SymbolicTopicNamesContext : public antlr4::ParserRuleContext {
  public:
    SymbolicTopicNamesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SymbolicNameWithDotsAndMinusContext *> symbolicNameWithDotsAndMinus();
    SymbolicNameWithDotsAndMinusContext* symbolicNameWithDotsAndMinus(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SymbolicTopicNamesContext* symbolicTopicNames();

  class  TopicNamesContext : public antlr4::ParserRuleContext {
  public:
    TopicNamesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicTopicNamesContext *symbolicTopicNames();
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  TopicNamesContext* topicNames();

  class  CommonCreateStreamConfigContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::ProcedureNameContext *transformationName = nullptr;
    MemgraphCypher::LiteralContext *batchInterval = nullptr;
    MemgraphCypher::LiteralContext *batchSize = nullptr;
    CommonCreateStreamConfigContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRANSFORM();
    ProcedureNameContext *procedureName();
    antlr4::tree::TerminalNode *BATCH_INTERVAL();
    LiteralContext *literal();
    antlr4::tree::TerminalNode *BATCH_SIZE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CommonCreateStreamConfigContext* commonCreateStreamConfig();

  class  CreateStreamContext : public antlr4::ParserRuleContext {
  public:
    CreateStreamContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KafkaCreateStreamContext *kafkaCreateStream();
    PulsarCreateStreamContext *pulsarCreateStream();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CreateStreamContext* createStream();

  class  ConfigKeyValuePairContext : public antlr4::ParserRuleContext {
  public:
    ConfigKeyValuePairContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<LiteralContext *> literal();
    LiteralContext* literal(size_t i);
    antlr4::tree::TerminalNode *COLON();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ConfigKeyValuePairContext* configKeyValuePair();

  class  ConfigMapContext : public antlr4::ParserRuleContext {
  public:
    ConfigMapContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LBRACE();
    antlr4::tree::TerminalNode *RBRACE();
    std::vector<ConfigKeyValuePairContext *> configKeyValuePair();
    ConfigKeyValuePairContext* configKeyValuePair(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ConfigMapContext* configMap();

  class  KafkaCreateStreamConfigContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::SymbolicNameWithDotsAndMinusContext *consumerGroup = nullptr;
    MemgraphCypher::LiteralContext *bootstrapServers = nullptr;
    MemgraphCypher::ConfigMapContext *configsMap = nullptr;
    MemgraphCypher::ConfigMapContext *credentialsMap = nullptr;
    KafkaCreateStreamConfigContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TOPICS();
    TopicNamesContext *topicNames();
    antlr4::tree::TerminalNode *CONSUMER_GROUP();
    SymbolicNameWithDotsAndMinusContext *symbolicNameWithDotsAndMinus();
    antlr4::tree::TerminalNode *BOOTSTRAP_SERVERS();
    LiteralContext *literal();
    antlr4::tree::TerminalNode *CONFIGS();
    ConfigMapContext *configMap();
    antlr4::tree::TerminalNode *CREDENTIALS();
    CommonCreateStreamConfigContext *commonCreateStreamConfig();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  KafkaCreateStreamConfigContext* kafkaCreateStreamConfig();

  class  KafkaCreateStreamContext : public antlr4::ParserRuleContext {
  public:
    KafkaCreateStreamContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *KAFKA();
    antlr4::tree::TerminalNode *STREAM();
    StreamNameContext *streamName();
    std::vector<KafkaCreateStreamConfigContext *> kafkaCreateStreamConfig();
    KafkaCreateStreamConfigContext* kafkaCreateStreamConfig(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  KafkaCreateStreamContext* kafkaCreateStream();

  class  PulsarCreateStreamConfigContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::LiteralContext *serviceUrl = nullptr;
    PulsarCreateStreamConfigContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TOPICS();
    TopicNamesContext *topicNames();
    antlr4::tree::TerminalNode *SERVICE_URL();
    LiteralContext *literal();
    CommonCreateStreamConfigContext *commonCreateStreamConfig();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PulsarCreateStreamConfigContext* pulsarCreateStreamConfig();

  class  PulsarCreateStreamContext : public antlr4::ParserRuleContext {
  public:
    PulsarCreateStreamContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *PULSAR();
    antlr4::tree::TerminalNode *STREAM();
    StreamNameContext *streamName();
    std::vector<PulsarCreateStreamConfigContext *> pulsarCreateStreamConfig();
    PulsarCreateStreamConfigContext* pulsarCreateStreamConfig(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PulsarCreateStreamContext* pulsarCreateStream();

  class  DropStreamContext : public antlr4::ParserRuleContext {
  public:
    DropStreamContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *STREAM();
    StreamNameContext *streamName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DropStreamContext* dropStream();

  class  StartStreamContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::LiteralContext *batchLimit = nullptr;
    MemgraphCypher::LiteralContext *timeout = nullptr;
    StartStreamContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *START();
    antlr4::tree::TerminalNode *STREAM();
    StreamNameContext *streamName();
    antlr4::tree::TerminalNode *BATCH_LIMIT();
    antlr4::tree::TerminalNode *TIMEOUT();
    std::vector<LiteralContext *> literal();
    LiteralContext* literal(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StartStreamContext* startStream();

  class  StartAllStreamsContext : public antlr4::ParserRuleContext {
  public:
    StartAllStreamsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *START();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *STREAMS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StartAllStreamsContext* startAllStreams();

  class  StopStreamContext : public antlr4::ParserRuleContext {
  public:
    StopStreamContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STOP();
    antlr4::tree::TerminalNode *STREAM();
    StreamNameContext *streamName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StopStreamContext* stopStream();

  class  StopAllStreamsContext : public antlr4::ParserRuleContext {
  public:
    StopAllStreamsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STOP();
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *STREAMS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StopAllStreamsContext* stopAllStreams();

  class  ShowStreamsContext : public antlr4::ParserRuleContext {
  public:
    ShowStreamsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *STREAMS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowStreamsContext* showStreams();

  class  CheckStreamContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::LiteralContext *batchLimit = nullptr;
    MemgraphCypher::LiteralContext *timeout = nullptr;
    CheckStreamContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CHECK();
    antlr4::tree::TerminalNode *STREAM();
    StreamNameContext *streamName();
    antlr4::tree::TerminalNode *BATCH_LIMIT();
    antlr4::tree::TerminalNode *TIMEOUT();
    std::vector<LiteralContext *> literal();
    LiteralContext* literal(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CheckStreamContext* checkStream();

  class  SettingNameContext : public antlr4::ParserRuleContext {
  public:
    SettingNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SettingNameContext* settingName();

  class  SettingValueContext : public antlr4::ParserRuleContext {
  public:
    SettingValueContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SettingValueContext* settingValue();

  class  SetSettingContext : public antlr4::ParserRuleContext {
  public:
    SetSettingContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *SETTING();
    SettingNameContext *settingName();
    antlr4::tree::TerminalNode *TO();
    SettingValueContext *settingValue();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SetSettingContext* setSetting();

  class  ShowSettingContext : public antlr4::ParserRuleContext {
  public:
    ShowSettingContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *SETTING();
    SettingNameContext *settingName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowSettingContext* showSetting();

  class  ShowSettingsContext : public antlr4::ParserRuleContext {
  public:
    ShowSettingsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *DATABASE();
    antlr4::tree::TerminalNode *SETTINGS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ShowSettingsContext* showSettings();

  class  VersionQueryContext : public antlr4::ParserRuleContext {
  public:
    VersionQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *VERSION();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VersionQueryContext* versionQuery();

  class  CypherContext : public antlr4::ParserRuleContext {
  public:
    CypherContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    StatementContext *statement();
    antlr4::tree::TerminalNode *EOF();
    antlr4::tree::TerminalNode *SEMICOLON();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CypherContext* cypher();

  class  StatementContext : public antlr4::ParserRuleContext {
  public:
    StatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    QueryContext *query();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StatementContext* statement();

  class  ConstraintQueryContext : public antlr4::ParserRuleContext {
  public:
    ConstraintQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CONSTRAINT();
    antlr4::tree::TerminalNode *ON();
    ConstraintContext *constraint();
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *DROP();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ConstraintQueryContext* constraintQuery();

  class  ConstraintContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::VariableContext *nodeName = nullptr;
    ConstraintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> LPAREN();
    antlr4::tree::TerminalNode* LPAREN(size_t i);
    antlr4::tree::TerminalNode *COLON();
    LabelNameContext *labelName();
    std::vector<antlr4::tree::TerminalNode *> RPAREN();
    antlr4::tree::TerminalNode* RPAREN(size_t i);
    antlr4::tree::TerminalNode *ASSERT();
    antlr4::tree::TerminalNode *EXISTS();
    ConstraintPropertyListContext *constraintPropertyList();
    VariableContext *variable();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *UNIQUE();
    antlr4::tree::TerminalNode *NODE();
    antlr4::tree::TerminalNode *KEY();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ConstraintContext* constraint();

  class  ConstraintPropertyListContext : public antlr4::ParserRuleContext {
  public:
    ConstraintPropertyListContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<VariableContext *> variable();
    VariableContext* variable(size_t i);
    std::vector<PropertyLookupContext *> propertyLookup();
    PropertyLookupContext* propertyLookup(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ConstraintPropertyListContext* constraintPropertyList();

  class  StorageInfoContext : public antlr4::ParserRuleContext {
  public:
    StorageInfoContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STORAGE();
    antlr4::tree::TerminalNode *INFO();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StorageInfoContext* storageInfo();

  class  IndexInfoContext : public antlr4::ParserRuleContext {
  public:
    IndexInfoContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *INDEX();
    antlr4::tree::TerminalNode *INFO();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IndexInfoContext* indexInfo();

  class  ConstraintInfoContext : public antlr4::ParserRuleContext {
  public:
    ConstraintInfoContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CONSTRAINT();
    antlr4::tree::TerminalNode *INFO();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ConstraintInfoContext* constraintInfo();

  class  InfoQueryContext : public antlr4::ParserRuleContext {
  public:
    InfoQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SHOW();
    StorageInfoContext *storageInfo();
    IndexInfoContext *indexInfo();
    ConstraintInfoContext *constraintInfo();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  InfoQueryContext* infoQuery();

  class  ExplainQueryContext : public antlr4::ParserRuleContext {
  public:
    ExplainQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXPLAIN();
    CypherQueryContext *cypherQuery();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ExplainQueryContext* explainQuery();

  class  ProfileQueryContext : public antlr4::ParserRuleContext {
  public:
    ProfileQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PROFILE();
    CypherQueryContext *cypherQuery();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ProfileQueryContext* profileQuery();

  class  CypherQueryContext : public antlr4::ParserRuleContext {
  public:
    CypherQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SingleQueryContext *singleQuery();
    std::vector<CypherUnionContext *> cypherUnion();
    CypherUnionContext* cypherUnion(size_t i);
    QueryMemoryLimitContext *queryMemoryLimit();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CypherQueryContext* cypherQuery();

  class  IndexQueryContext : public antlr4::ParserRuleContext {
  public:
    IndexQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    CreateIndexContext *createIndex();
    DropIndexContext *dropIndex();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IndexQueryContext* indexQuery();

  class  SingleQueryContext : public antlr4::ParserRuleContext {
  public:
    SingleQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<ClauseContext *> clause();
    ClauseContext* clause(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SingleQueryContext* singleQuery();

  class  CypherUnionContext : public antlr4::ParserRuleContext {
  public:
    CypherUnionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNION();
    antlr4::tree::TerminalNode *ALL();
    SingleQueryContext *singleQuery();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CypherUnionContext* cypherUnion();

  class  CypherMatchContext : public antlr4::ParserRuleContext {
  public:
    CypherMatchContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MATCH();
    PatternContext *pattern();
    antlr4::tree::TerminalNode *OPTIONAL();
    WhereContext *where();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CypherMatchContext* cypherMatch();

  class  UnwindContext : public antlr4::ParserRuleContext {
  public:
    UnwindContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNWIND();
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *AS();
    VariableContext *variable();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  UnwindContext* unwind();

  class  MergeContext : public antlr4::ParserRuleContext {
  public:
    MergeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MERGE();
    PatternPartContext *patternPart();
    std::vector<MergeActionContext *> mergeAction();
    MergeActionContext* mergeAction(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MergeContext* merge();

  class  MergeActionContext : public antlr4::ParserRuleContext {
  public:
    MergeActionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *MATCH();
    SetContext *set();
    antlr4::tree::TerminalNode *CREATE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MergeActionContext* mergeAction();

  class  CreateContext : public antlr4::ParserRuleContext {
  public:
    CreateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    PatternContext *pattern();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CreateContext* create();

  class  SetContext : public antlr4::ParserRuleContext {
  public:
    SetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    std::vector<SetItemContext *> setItem();
    SetItemContext* setItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SetContext* set();

  class  SetItemContext : public antlr4::ParserRuleContext {
  public:
    SetItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PropertyExpressionContext *propertyExpression();
    antlr4::tree::TerminalNode *EQ();
    ExpressionContext *expression();
    VariableContext *variable();
    antlr4::tree::TerminalNode *PLUS_EQ();
    NodeLabelsContext *nodeLabels();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SetItemContext* setItem();

  class  CypherDeleteContext : public antlr4::ParserRuleContext {
  public:
    CypherDeleteContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DELETE();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode *DETACH();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CypherDeleteContext* cypherDelete();

  class  RemoveContext : public antlr4::ParserRuleContext {
  public:
    RemoveContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *REMOVE();
    std::vector<RemoveItemContext *> removeItem();
    RemoveItemContext* removeItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RemoveContext* remove();

  class  RemoveItemContext : public antlr4::ParserRuleContext {
  public:
    RemoveItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VariableContext *variable();
    NodeLabelsContext *nodeLabels();
    PropertyExpressionContext *propertyExpression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RemoveItemContext* removeItem();

  class  WithContext : public antlr4::ParserRuleContext {
  public:
    WithContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH();
    ReturnBodyContext *returnBody();
    antlr4::tree::TerminalNode *DISTINCT();
    WhereContext *where();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  WithContext* with();

  class  CypherReturnContext : public antlr4::ParserRuleContext {
  public:
    CypherReturnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RETURN();
    ReturnBodyContext *returnBody();
    antlr4::tree::TerminalNode *DISTINCT();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CypherReturnContext* cypherReturn();

  class  CallProcedureContext : public antlr4::ParserRuleContext {
  public:
    CallProcedureContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL();
    ProcedureNameContext *procedureName();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    ProcedureMemoryLimitContext *procedureMemoryLimit();
    YieldProcedureResultsContext *yieldProcedureResults();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CallProcedureContext* callProcedure();

  class  ProcedureNameContext : public antlr4::ParserRuleContext {
  public:
    ProcedureNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SymbolicNameContext *> symbolicName();
    SymbolicNameContext* symbolicName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ProcedureNameContext* procedureName();

  class  YieldProcedureResultsContext : public antlr4::ParserRuleContext {
  public:
    YieldProcedureResultsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *YIELD();
    antlr4::tree::TerminalNode *ASTERISK();
    std::vector<ProcedureResultContext *> procedureResult();
    ProcedureResultContext* procedureResult(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  YieldProcedureResultsContext* yieldProcedureResults();

  class  MemoryLimitContext : public antlr4::ParserRuleContext {
  public:
    MemoryLimitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MEMORY();
    antlr4::tree::TerminalNode *UNLIMITED();
    antlr4::tree::TerminalNode *LIMIT();
    LiteralContext *literal();
    antlr4::tree::TerminalNode *MB();
    antlr4::tree::TerminalNode *KB();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MemoryLimitContext* memoryLimit();

  class  QueryMemoryLimitContext : public antlr4::ParserRuleContext {
  public:
    QueryMemoryLimitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *QUERY();
    MemoryLimitContext *memoryLimit();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  QueryMemoryLimitContext* queryMemoryLimit();

  class  ProcedureMemoryLimitContext : public antlr4::ParserRuleContext {
  public:
    ProcedureMemoryLimitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PROCEDURE();
    MemoryLimitContext *memoryLimit();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ProcedureMemoryLimitContext* procedureMemoryLimit();

  class  ProcedureResultContext : public antlr4::ParserRuleContext {
  public:
    ProcedureResultContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<VariableContext *> variable();
    VariableContext* variable(size_t i);
    antlr4::tree::TerminalNode *AS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ProcedureResultContext* procedureResult();

  class  ReturnBodyContext : public antlr4::ParserRuleContext {
  public:
    ReturnBodyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ReturnItemsContext *returnItems();
    OrderContext *order();
    SkipContext *skip();
    LimitContext *limit();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ReturnBodyContext* returnBody();

  class  ReturnItemsContext : public antlr4::ParserRuleContext {
  public:
    ReturnItemsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASTERISK();
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    std::vector<ReturnItemContext *> returnItem();
    ReturnItemContext* returnItem(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ReturnItemsContext* returnItems();

  class  ReturnItemContext : public antlr4::ParserRuleContext {
  public:
    ReturnItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *AS();
    VariableContext *variable();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ReturnItemContext* returnItem();

  class  OrderContext : public antlr4::ParserRuleContext {
  public:
    OrderContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ORDER();
    antlr4::tree::TerminalNode *BY();
    std::vector<SortItemContext *> sortItem();
    SortItemContext* sortItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  OrderContext* order();

  class  SkipContext : public antlr4::ParserRuleContext {
  public:
    SkipContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *L_SKIP();
    ExpressionContext *expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SkipContext* skip();

  class  LimitContext : public antlr4::ParserRuleContext {
  public:
    LimitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIMIT();
    ExpressionContext *expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LimitContext* limit();

  class  SortItemContext : public antlr4::ParserRuleContext {
  public:
    SortItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *ASCENDING();
    antlr4::tree::TerminalNode *ASC();
    antlr4::tree::TerminalNode *DESCENDING();
    antlr4::tree::TerminalNode *DESC();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  SortItemContext* sortItem();

  class  WhereContext : public antlr4::ParserRuleContext {
  public:
    WhereContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHERE();
    ExpressionContext *expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  WhereContext* where();

  class  PatternContext : public antlr4::ParserRuleContext {
  public:
    PatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<PatternPartContext *> patternPart();
    PatternPartContext* patternPart(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PatternContext* pattern();

  class  PatternPartContext : public antlr4::ParserRuleContext {
  public:
    PatternPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VariableContext *variable();
    antlr4::tree::TerminalNode *EQ();
    AnonymousPatternPartContext *anonymousPatternPart();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PatternPartContext* patternPart();

  class  AnonymousPatternPartContext : public antlr4::ParserRuleContext {
  public:
    AnonymousPatternPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    PatternElementContext *patternElement();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AnonymousPatternPartContext* anonymousPatternPart();

  class  PatternElementContext : public antlr4::ParserRuleContext {
  public:
    PatternElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NodePatternContext *nodePattern();
    std::vector<PatternElementChainContext *> patternElementChain();
    PatternElementChainContext* patternElementChain(size_t i);
    antlr4::tree::TerminalNode *LPAREN();
    PatternElementContext *patternElement();
    antlr4::tree::TerminalNode *RPAREN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PatternElementContext* patternElement();

  class  NodePatternContext : public antlr4::ParserRuleContext {
  public:
    NodePatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();
    VariableContext *variable();
    NodeLabelsContext *nodeLabels();
    PropertiesContext *properties();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NodePatternContext* nodePattern();

  class  PatternElementChainContext : public antlr4::ParserRuleContext {
  public:
    PatternElementChainContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    RelationshipPatternContext *relationshipPattern();
    NodePatternContext *nodePattern();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PatternElementChainContext* patternElementChain();

  class  RelationshipPatternContext : public antlr4::ParserRuleContext {
  public:
    RelationshipPatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LeftArrowHeadContext *leftArrowHead();
    std::vector<DashContext *> dash();
    DashContext* dash(size_t i);
    RightArrowHeadContext *rightArrowHead();
    RelationshipDetailContext *relationshipDetail();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RelationshipPatternContext* relationshipPattern();

  class  LeftArrowHeadContext : public antlr4::ParserRuleContext {
  public:
    LeftArrowHeadContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LT();
    antlr4::tree::TerminalNode *LeftArrowHeadPart();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LeftArrowHeadContext* leftArrowHead();

  class  RightArrowHeadContext : public antlr4::ParserRuleContext {
  public:
    RightArrowHeadContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *GT();
    antlr4::tree::TerminalNode *RightArrowHeadPart();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RightArrowHeadContext* rightArrowHead();

  class  DashContext : public antlr4::ParserRuleContext {
  public:
    DashContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MINUS();
    antlr4::tree::TerminalNode *DashPart();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DashContext* dash();

  class  RelationshipDetailContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::VariableContext *name = nullptr;
    MemgraphCypher::VariableContext *total_weight = nullptr;
    RelationshipDetailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LBRACK();
    std::vector<PropertiesContext *> properties();
    PropertiesContext* properties(size_t i);
    antlr4::tree::TerminalNode *RBRACK();
    RelationshipTypesContext *relationshipTypes();
    VariableExpansionContext *variableExpansion();
    std::vector<VariableContext *> variable();
    VariableContext* variable(size_t i);
    std::vector<RelationshipLambdaContext *> relationshipLambda();
    RelationshipLambdaContext* relationshipLambda(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RelationshipDetailContext* relationshipDetail();

  class  RelationshipLambdaContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::VariableContext *traversed_edge = nullptr;
    MemgraphCypher::VariableContext *traversed_node = nullptr;
    RelationshipLambdaContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *COMMA();
    antlr4::tree::TerminalNode *PIPE();
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *RPAREN();
    std::vector<VariableContext *> variable();
    VariableContext* variable(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RelationshipLambdaContext* relationshipLambda();

  class  VariableExpansionContext : public antlr4::ParserRuleContext {
  public:
    VariableExpansionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ASTERISK();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode *DOTS();
    antlr4::tree::TerminalNode *BFS();
    antlr4::tree::TerminalNode *WSHORTEST();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VariableExpansionContext* variableExpansion();

  class  PropertiesContext : public antlr4::ParserRuleContext {
  public:
    PropertiesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    MapLiteralContext *mapLiteral();
    ParameterContext *parameter();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PropertiesContext* properties();

  class  RelationshipTypesContext : public antlr4::ParserRuleContext {
  public:
    RelationshipTypesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> COLON();
    antlr4::tree::TerminalNode* COLON(size_t i);
    std::vector<RelTypeNameContext *> relTypeName();
    RelTypeNameContext* relTypeName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> PIPE();
    antlr4::tree::TerminalNode* PIPE(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RelationshipTypesContext* relationshipTypes();

  class  NodeLabelsContext : public antlr4::ParserRuleContext {
  public:
    NodeLabelsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<NodeLabelContext *> nodeLabel();
    NodeLabelContext* nodeLabel(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NodeLabelsContext* nodeLabels();

  class  NodeLabelContext : public antlr4::ParserRuleContext {
  public:
    NodeLabelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COLON();
    LabelNameContext *labelName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NodeLabelContext* nodeLabel();

  class  LabelNameContext : public antlr4::ParserRuleContext {
  public:
    LabelNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LabelNameContext* labelName();

  class  RelTypeNameContext : public antlr4::ParserRuleContext {
  public:
    RelTypeNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RelTypeNameContext* relTypeName();

  class  ExpressionContext : public antlr4::ParserRuleContext {
  public:
    ExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression12Context *expression12();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ExpressionContext* expression();

  class  Expression12Context : public antlr4::ParserRuleContext {
  public:
    Expression12Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Expression11Context *> expression11();
    Expression11Context* expression11(size_t i);
    std::vector<antlr4::tree::TerminalNode *> OR();
    antlr4::tree::TerminalNode* OR(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression12Context* expression12();

  class  Expression11Context : public antlr4::ParserRuleContext {
  public:
    Expression11Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Expression10Context *> expression10();
    Expression10Context* expression10(size_t i);
    std::vector<antlr4::tree::TerminalNode *> XOR();
    antlr4::tree::TerminalNode* XOR(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression11Context* expression11();

  class  Expression10Context : public antlr4::ParserRuleContext {
  public:
    Expression10Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Expression9Context *> expression9();
    Expression9Context* expression9(size_t i);
    std::vector<antlr4::tree::TerminalNode *> AND();
    antlr4::tree::TerminalNode* AND(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression10Context* expression10();

  class  Expression9Context : public antlr4::ParserRuleContext {
  public:
    Expression9Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression8Context *expression8();
    std::vector<antlr4::tree::TerminalNode *> NOT();
    antlr4::tree::TerminalNode* NOT(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression9Context* expression9();

  class  Expression8Context : public antlr4::ParserRuleContext {
  public:
    Expression8Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression7Context *expression7();
    std::vector<PartialComparisonExpressionContext *> partialComparisonExpression();
    PartialComparisonExpressionContext* partialComparisonExpression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression8Context* expression8();

  class  Expression7Context : public antlr4::ParserRuleContext {
  public:
    Expression7Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Expression6Context *> expression6();
    Expression6Context* expression6(size_t i);
    std::vector<antlr4::tree::TerminalNode *> PLUS();
    antlr4::tree::TerminalNode* PLUS(size_t i);
    std::vector<antlr4::tree::TerminalNode *> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression7Context* expression7();

  class  Expression6Context : public antlr4::ParserRuleContext {
  public:
    Expression6Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Expression5Context *> expression5();
    Expression5Context* expression5(size_t i);
    std::vector<antlr4::tree::TerminalNode *> ASTERISK();
    antlr4::tree::TerminalNode* ASTERISK(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SLASH();
    antlr4::tree::TerminalNode* SLASH(size_t i);
    std::vector<antlr4::tree::TerminalNode *> PERCENT();
    antlr4::tree::TerminalNode* PERCENT(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression6Context* expression6();

  class  Expression5Context : public antlr4::ParserRuleContext {
  public:
    Expression5Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<Expression4Context *> expression4();
    Expression4Context* expression4(size_t i);
    std::vector<antlr4::tree::TerminalNode *> CARET();
    antlr4::tree::TerminalNode* CARET(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression5Context* expression5();

  class  Expression4Context : public antlr4::ParserRuleContext {
  public:
    Expression4Context(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression3aContext *expression3a();
    std::vector<antlr4::tree::TerminalNode *> PLUS();
    antlr4::tree::TerminalNode* PLUS(size_t i);
    std::vector<antlr4::tree::TerminalNode *> MINUS();
    antlr4::tree::TerminalNode* MINUS(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression4Context* expression4();

  class  Expression3aContext : public antlr4::ParserRuleContext {
  public:
    Expression3aContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression3bContext *expression3b();
    std::vector<StringAndNullOperatorsContext *> stringAndNullOperators();
    StringAndNullOperatorsContext* stringAndNullOperators(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression3aContext* expression3a();

  class  StringAndNullOperatorsContext : public antlr4::ParserRuleContext {
  public:
    StringAndNullOperatorsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression3bContext *expression3b();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *CYPHERNULL();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *SIM();
    antlr4::tree::TerminalNode *IN();
    antlr4::tree::TerminalNode *STARTS();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *ENDS();
    antlr4::tree::TerminalNode *CONTAINS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  StringAndNullOperatorsContext* stringAndNullOperators();

  class  Expression3bContext : public antlr4::ParserRuleContext {
  public:
    Expression3bContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression2aContext *expression2a();
    std::vector<ListIndexingOrSlicingContext *> listIndexingOrSlicing();
    ListIndexingOrSlicingContext* listIndexingOrSlicing(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression3bContext* expression3b();

  class  ListIndexingOrSlicingContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::ExpressionContext *lower_bound = nullptr;
    MemgraphCypher::ExpressionContext *upper_bound = nullptr;
    ListIndexingOrSlicingContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LBRACK();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode *RBRACK();
    antlr4::tree::TerminalNode *DOTS();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ListIndexingOrSlicingContext* listIndexingOrSlicing();

  class  Expression2aContext : public antlr4::ParserRuleContext {
  public:
    Expression2aContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    Expression2bContext *expression2b();
    NodeLabelsContext *nodeLabels();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression2aContext* expression2a();

  class  Expression2bContext : public antlr4::ParserRuleContext {
  public:
    Expression2bContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AtomContext *atom();
    std::vector<PropertyLookupContext *> propertyLookup();
    PropertyLookupContext* propertyLookup(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  Expression2bContext* expression2b();

  class  AtomContext : public antlr4::ParserRuleContext {
  public:
    AtomContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    LiteralContext *literal();
    ParameterContext *parameter();
    CaseExpressionContext *caseExpression();
    antlr4::tree::TerminalNode *COUNT();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *ASTERISK();
    antlr4::tree::TerminalNode *RPAREN();
    ListComprehensionContext *listComprehension();
    PatternComprehensionContext *patternComprehension();
    antlr4::tree::TerminalNode *FILTER();
    FilterExpressionContext *filterExpression();
    antlr4::tree::TerminalNode *EXTRACT();
    ExtractExpressionContext *extractExpression();
    antlr4::tree::TerminalNode *REDUCE();
    ReduceExpressionContext *reduceExpression();
    antlr4::tree::TerminalNode *COALESCE();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *NONE();
    antlr4::tree::TerminalNode *SINGLE();
    RelationshipsPatternContext *relationshipsPattern();
    ParenthesizedExpressionContext *parenthesizedExpression();
    FunctionInvocationContext *functionInvocation();
    VariableContext *variable();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  AtomContext* atom();

  class  LiteralContext : public antlr4::ParserRuleContext {
  public:
    LiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NumberLiteralContext *numberLiteral();
    antlr4::tree::TerminalNode *StringLiteral();
    BooleanLiteralContext *booleanLiteral();
    antlr4::tree::TerminalNode *CYPHERNULL();
    MapLiteralContext *mapLiteral();
    ListLiteralContext *listLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  LiteralContext* literal();

  class  BooleanLiteralContext : public antlr4::ParserRuleContext {
  public:
    BooleanLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRUE();
    antlr4::tree::TerminalNode *FALSE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  BooleanLiteralContext* booleanLiteral();

  class  ListLiteralContext : public antlr4::ParserRuleContext {
  public:
    ListLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LBRACK();
    antlr4::tree::TerminalNode *RBRACK();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ListLiteralContext* listLiteral();

  class  PartialComparisonExpressionContext : public antlr4::ParserRuleContext {
  public:
    PartialComparisonExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQ();
    Expression7Context *expression7();
    antlr4::tree::TerminalNode *NEQ1();
    antlr4::tree::TerminalNode *NEQ2();
    antlr4::tree::TerminalNode *LT();
    antlr4::tree::TerminalNode *GT();
    antlr4::tree::TerminalNode *LTE();
    antlr4::tree::TerminalNode *GTE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PartialComparisonExpressionContext* partialComparisonExpression();

  class  ParenthesizedExpressionContext : public antlr4::ParserRuleContext {
  public:
    ParenthesizedExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LPAREN();
    ExpressionContext *expression();
    antlr4::tree::TerminalNode *RPAREN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParenthesizedExpressionContext* parenthesizedExpression();

  class  RelationshipsPatternContext : public antlr4::ParserRuleContext {
  public:
    RelationshipsPatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    NodePatternContext *nodePattern();
    std::vector<PatternElementChainContext *> patternElementChain();
    PatternElementChainContext* patternElementChain(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  RelationshipsPatternContext* relationshipsPattern();

  class  FilterExpressionContext : public antlr4::ParserRuleContext {
  public:
    FilterExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdInCollContext *idInColl();
    WhereContext *where();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FilterExpressionContext* filterExpression();

  class  ReduceExpressionContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::VariableContext *accumulator = nullptr;
    MemgraphCypher::ExpressionContext *initial = nullptr;
    ReduceExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EQ();
    antlr4::tree::TerminalNode *COMMA();
    IdInCollContext *idInColl();
    antlr4::tree::TerminalNode *PIPE();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    VariableContext *variable();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ReduceExpressionContext* reduceExpression();

  class  ExtractExpressionContext : public antlr4::ParserRuleContext {
  public:
    ExtractExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    IdInCollContext *idInColl();
    antlr4::tree::TerminalNode *PIPE();
    ExpressionContext *expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ExtractExpressionContext* extractExpression();

  class  IdInCollContext : public antlr4::ParserRuleContext {
  public:
    IdInCollContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    VariableContext *variable();
    antlr4::tree::TerminalNode *IN();
    ExpressionContext *expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IdInCollContext* idInColl();

  class  FunctionInvocationContext : public antlr4::ParserRuleContext {
  public:
    FunctionInvocationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    FunctionNameContext *functionName();
    antlr4::tree::TerminalNode *LPAREN();
    antlr4::tree::TerminalNode *RPAREN();
    antlr4::tree::TerminalNode *DISTINCT();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FunctionInvocationContext* functionInvocation();

  class  FunctionNameContext : public antlr4::ParserRuleContext {
  public:
    FunctionNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<SymbolicNameContext *> symbolicName();
    SymbolicNameContext* symbolicName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> DOT();
    antlr4::tree::TerminalNode* DOT(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  FunctionNameContext* functionName();

  class  ListComprehensionContext : public antlr4::ParserRuleContext {
  public:
    ListComprehensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LBRACK();
    FilterExpressionContext *filterExpression();
    antlr4::tree::TerminalNode *RBRACK();
    antlr4::tree::TerminalNode *PIPE();
    ExpressionContext *expression();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ListComprehensionContext* listComprehension();

  class  PatternComprehensionContext : public antlr4::ParserRuleContext {
  public:
    PatternComprehensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LBRACK();
    RelationshipsPatternContext *relationshipsPattern();
    antlr4::tree::TerminalNode *PIPE();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    antlr4::tree::TerminalNode *RBRACK();
    VariableContext *variable();
    antlr4::tree::TerminalNode *EQ();
    antlr4::tree::TerminalNode *WHERE();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PatternComprehensionContext* patternComprehension();

  class  PropertyLookupContext : public antlr4::ParserRuleContext {
  public:
    PropertyLookupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DOT();
    PropertyKeyNameContext *propertyKeyName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PropertyLookupContext* propertyLookup();

  class  CaseExpressionContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::ExpressionContext *test = nullptr;
    MemgraphCypher::ExpressionContext *else_expression = nullptr;
    CaseExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *END();
    antlr4::tree::TerminalNode *ELSE();
    antlr4::tree::TerminalNode *CASE();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    std::vector<CaseAlternativesContext *> caseAlternatives();
    CaseAlternativesContext* caseAlternatives(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CaseExpressionContext* caseExpression();

  class  CaseAlternativesContext : public antlr4::ParserRuleContext {
  public:
    MemgraphCypher::ExpressionContext *when_expression = nullptr;
    MemgraphCypher::ExpressionContext *then_expression = nullptr;
    CaseAlternativesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHEN();
    antlr4::tree::TerminalNode *THEN();
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CaseAlternativesContext* caseAlternatives();

  class  VariableContext : public antlr4::ParserRuleContext {
  public:
    VariableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  VariableContext* variable();

  class  NumberLiteralContext : public antlr4::ParserRuleContext {
  public:
    NumberLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    DoubleLiteralContext *doubleLiteral();
    IntegerLiteralContext *integerLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  NumberLiteralContext* numberLiteral();

  class  MapLiteralContext : public antlr4::ParserRuleContext {
  public:
    MapLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LBRACE();
    antlr4::tree::TerminalNode *RBRACE();
    std::vector<PropertyKeyNameContext *> propertyKeyName();
    PropertyKeyNameContext* propertyKeyName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COLON();
    antlr4::tree::TerminalNode* COLON(size_t i);
    std::vector<ExpressionContext *> expression();
    ExpressionContext* expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> COMMA();
    antlr4::tree::TerminalNode* COMMA(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  MapLiteralContext* mapLiteral();

  class  ParameterContext : public antlr4::ParserRuleContext {
  public:
    ParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DOLLAR();
    SymbolicNameContext *symbolicName();
    antlr4::tree::TerminalNode *DecimalLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  ParameterContext* parameter();

  class  PropertyExpressionContext : public antlr4::ParserRuleContext {
  public:
    PropertyExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    AtomContext *atom();
    std::vector<PropertyLookupContext *> propertyLookup();
    PropertyLookupContext* propertyLookup(size_t i);

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PropertyExpressionContext* propertyExpression();

  class  PropertyKeyNameContext : public antlr4::ParserRuleContext {
  public:
    PropertyKeyNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    SymbolicNameContext *symbolicName();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  PropertyKeyNameContext* propertyKeyName();

  class  IntegerLiteralContext : public antlr4::ParserRuleContext {
  public:
    IntegerLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DecimalLiteral();
    antlr4::tree::TerminalNode *OctalLiteral();
    antlr4::tree::TerminalNode *HexadecimalLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  IntegerLiteralContext* integerLiteral();

  class  CreateIndexContext : public antlr4::ParserRuleContext {
  public:
    CreateIndexContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *INDEX();
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *COLON();
    LabelNameContext *labelName();
    antlr4::tree::TerminalNode *LPAREN();
    PropertyKeyNameContext *propertyKeyName();
    antlr4::tree::TerminalNode *RPAREN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CreateIndexContext* createIndex();

  class  DropIndexContext : public antlr4::ParserRuleContext {
  public:
    DropIndexContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *INDEX();
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *COLON();
    LabelNameContext *labelName();
    antlr4::tree::TerminalNode *LPAREN();
    PropertyKeyNameContext *propertyKeyName();
    antlr4::tree::TerminalNode *RPAREN();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DropIndexContext* dropIndex();

  class  DoubleLiteralContext : public antlr4::ParserRuleContext {
  public:
    DoubleLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FloatingLiteral();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  DoubleLiteralContext* doubleLiteral();

  class  CypherKeywordContext : public antlr4::ParserRuleContext {
  public:
    CypherKeywordContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALL();
    antlr4::tree::TerminalNode *AND();
    antlr4::tree::TerminalNode *ANY();
    antlr4::tree::TerminalNode *AS();
    antlr4::tree::TerminalNode *ASC();
    antlr4::tree::TerminalNode *ASCENDING();
    antlr4::tree::TerminalNode *ASSERT();
    antlr4::tree::TerminalNode *BFS();
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *CALL();
    antlr4::tree::TerminalNode *CASE();
    antlr4::tree::TerminalNode *CONSTRAINT();
    antlr4::tree::TerminalNode *CONTAINS();
    antlr4::tree::TerminalNode *COUNT();
    antlr4::tree::TerminalNode *CREATE();
    antlr4::tree::TerminalNode *CYPHERNULL();
    antlr4::tree::TerminalNode *DELETE();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *DESCENDING();
    antlr4::tree::TerminalNode *DETACH();
    antlr4::tree::TerminalNode *DISTINCT();
    antlr4::tree::TerminalNode *ELSE();
    antlr4::tree::TerminalNode *END();
    antlr4::tree::TerminalNode *ENDS();
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *EXPLAIN();
    antlr4::tree::TerminalNode *EXTRACT();
    antlr4::tree::TerminalNode *FALSE();
    antlr4::tree::TerminalNode *FILTER();
    antlr4::tree::TerminalNode *IN();
    antlr4::tree::TerminalNode *INDEX();
    antlr4::tree::TerminalNode *INFO();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *KEY();
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *L_SKIP();
    antlr4::tree::TerminalNode *MATCH();
    antlr4::tree::TerminalNode *MERGE();
    antlr4::tree::TerminalNode *NODE();
    antlr4::tree::TerminalNode *NONE();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *OPTIONAL();
    antlr4::tree::TerminalNode *OR();
    antlr4::tree::TerminalNode *ORDER();
    antlr4::tree::TerminalNode *PROCEDURE();
    antlr4::tree::TerminalNode *PROFILE();
    antlr4::tree::TerminalNode *QUERY();
    antlr4::tree::TerminalNode *REDUCE();
    antlr4::tree::TerminalNode *REMOVE();
    antlr4::tree::TerminalNode *RETURN();
    antlr4::tree::TerminalNode *SET();
    antlr4::tree::TerminalNode *SHOW();
    antlr4::tree::TerminalNode *SINGLE();
    antlr4::tree::TerminalNode *STARTS();
    antlr4::tree::TerminalNode *STORAGE();
    antlr4::tree::TerminalNode *THEN();
    antlr4::tree::TerminalNode *TRUE();
    antlr4::tree::TerminalNode *UNION();
    antlr4::tree::TerminalNode *UNIQUE();
    antlr4::tree::TerminalNode *UNWIND();
    antlr4::tree::TerminalNode *WHEN();
    antlr4::tree::TerminalNode *WHERE();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *WSHORTEST();
    antlr4::tree::TerminalNode *XOR();
    antlr4::tree::TerminalNode *YIELD();

    virtual void enterRule(antlr4::tree::ParseTreeListener *listener) override;
    virtual void exitRule(antlr4::tree::ParseTreeListener *listener) override;

    virtual std::any accept(antlr4::tree::ParseTreeVisitor *visitor) override;
   
  };

  CypherKeywordContext* cypherKeyword();


  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:
};

}  // namespace antlropencypher
