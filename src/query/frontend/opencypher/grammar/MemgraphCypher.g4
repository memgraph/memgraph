/*
 * Copyright 2021 Memgraph Ltd.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
 * License, and you may not use this file except in compliance with the Business Source License.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */

/* Memgraph specific part of Cypher grammar with enterprise features. */

parser grammar MemgraphCypher ;

options { tokenVocab=MemgraphCypherLexer; }

import Cypher ;

/* Also update src/query/frontend/stripped_lexer_constants.hpp */
memgraphCypherKeyword : cypherKeyword
                      | ACTIVE
                      | ADD
                      | AFTER
                      | ALTER
                      | ANALYZE
                      | ANY
                      | ASYNC
                      | AT
                      | AUTH
                      | BAD
                      | BATCH_INTERVAL
                      | BATCH_LIMIT
                      | BATCH_SIZE
                      | BEFORE
                      | BOOLEAN
                      | BOOTSTRAP_SERVERS
                      | BUILD
                      | CALL
                      | CHECK
                      | CLEAR
                      | CLUSTER
                      | COMMIT
                      | COMMITTED
                      | CONFIG
                      | CONFIGS
                      | CONSTRAINTS
                      | CONSUMER_GROUP
                      | CONTAINING
                      | COORDINATOR
                      | CREDENTIALS
                      | CSV
                      | CURRENT
                      | DATA
                      | DATABASE
                      | DATABASES
                      | DATE
                      | DELIMITER
                      | DEMOTE
                      | DENY
                      | DIRECTORY
                      | DISABLE
                      | DO
                      | DROP
                      | DUMP
                      | DURABILITY
                      | DURATION
                      | EDGE
                      | EDGES
                      | EDGE_TYPES
                      | ENABLE
                      | ENUM
                      | ENUMS
                      | EVERY
                      | EXACTLY
                      | EXECUTE
                      | FAILOVER
                      | FLOAT
                      | FOR
                      | FORCE
                      | FOREACH
                      | FREE
                      | FREE_MEMORY
                      | FROM
                      | GLOBAL
                      | GRANT
                      | GRANTS
                      | GRAPH
                      | HEADER
                      | HOPS
                      | IDENTIFIED
                      | IF
                      | IGNORE
                      | IMPERSONATE_USER
                      | INDEXES
                      | IMPORT
                      | IN_MEMORY_ANALYTICAL
                      | IN_MEMORY_TRANSACTIONAL
                      | INACTIVE
                      | INSTANCE
                      | INSTANCES
                      | INTEGER
                      | ISOLATION
                      | KAFKA
                      | LABELS
                      | LAG
                      | LEADERSHIP
                      | LEVEL
                      | LICENSE
                      | LIST
                      | LOAD
                      | LOCALDATETIME
                      | LOCALTIME
                      | LOCK
                      | MAIN
                      | MAP
                      | MATCHING
                      | METRICS
                      | MODE
                      | MODULE_READ
                      | MODULE_WRITE
                      | MULTI_DATABASE_EDIT
                      | MULTI_DATABASE_USE
                      | NEXT
                      | NO
                      | NODE_LABELS
                      | NODES
                      | NOTHING
                      | NULLIF
                      | OF_TOKEN
                      | OFF
                      | ON
                      | ON_DISK_TRANSACTIONAL
                      | PARQUET
                      | PASSWORD
                      | PERIODIC
                      | POINT
                      | PORT
                      | PRIVILEGES
                      | PROFILE_RESTRICTION
                      | PROFILES
                      | PULSAR
                      | QUOTE
                      | QUOTE
                      | READ
                      | READ_FILE
                      | RECOVER
                      | REGISTER
                      | RENAME
                      | REPLACE
                      | REPLICA
                      | REPLICAS
                      | REPLICATION
                      | RESET
                      | RESOURCE
                      | REVOKE
                      | ROLE
                      | ROLES
                      | ROWS
                      | SCHEMA
                      | SERVER
                      | SERVICE_URL
                      | SESSION
                      | SETTING
                      | SETTINGS
                      | SNAPSHOT
                      | SNAPSHOTS
                      | START
                      | STATE
                      | STATISTICS
                      | STATS
                      | STATUS
                      | STOP
                      | STORAGE
                      | STORAGE_MODE
                      | STREAM
                      | STREAMS
                      | STRICT_SYNC
                      | STRING
                      | SYNC
                      | TERMINATE
                      | TEXT
                      | TIMEOUT
                      | TO
                      | TOPICS
                      | TRACE
                      | TRANSACTION
                      | TRANSACTIONS
                      | TRANSACTION_MANAGEMENT
                      | TRANSFORM
                      | TRIGGER
                      | TRIGGERS
                      | TTL
                      | TYPES
                      | UNCOMMITTED
                      | UNLOCK
                      | UNREGISTER
                      | UPDATE
                      | USE
                      | USER
                      | USERS
                      | USAGE
                      | USING
                      | VALUE
                      | VALUES
                      | VECTOR
                      | VERSION
                      | WEBSOCKET
                      | YIELD
                      | ZONEDDATETIME
                      ;

symbolicName : UnescapedSymbolicName
             | EscapedSymbolicName
             | memgraphCypherKeyword
             ;

query : cypherQuery
      | indexQuery
      | edgeIndexQuery
      | pointIndexQuery
      | textIndexQuery
      | createTextEdgeIndex
      | vectorIndexQuery
      | createVectorEdgeIndex
      | explainQuery
      | profileQuery
      | databaseInfoQuery
      | systemInfoQuery
      | constraintQuery
      | authQuery
      | dumpQuery
      | analyzeGraphQuery
      | replicationQuery
      | replicationInfoQuery
      | lockPathQuery
      | freeMemoryQuery
      | triggerQuery
      | isolationLevelQuery
      | storageModeQuery
      | createSnapshotQuery
      | recoverSnapshotQuery
      | showSnapshotsQuery
      | showNextSnapshotQuery
      | streamQuery
      | settingQuery
      | versionQuery
      | showConfigQuery
      | transactionQueueQuery
      | multiDatabaseQuery
      | useDatabase
      | showDatabase
      | showDatabases
      | edgeImportModeQuery
      | coordinatorQuery
      | dropAllIndexesQuery
      | dropAllConstraintsQuery
      | dropGraphQuery
      | createEnumQuery
      | showEnumsQuery
      | alterEnumAddValueQuery
      | alterEnumUpdateValueQuery
      | alterEnumRemoveValueQuery
      | dropEnumQuery
      | showSchemaInfoQuery
      | ttlQuery
      | setSessionTraceQuery
      | userProfileQuery
      ;

cypherQuery : ( preQueryDirectives )? singleQuery ( cypherUnion )* ( queryMemoryLimit )? ;

authQuery : createRole
          | dropRole
          | showRoles
          | createUser
          | setPassword
          | changePassword
          | dropUser
          | showCurrentUser
          | showCurrentRole
          | showUsers
          | setRole
          | clearRole
          | grantPrivilege
          | denyPrivilege
          | revokePrivilege
          | showPrivileges
          | showRoleForUser
          | showUsersForRole
          | grantDatabaseToUserOrRole
          | denyDatabaseFromUserOrRole
          | revokeDatabaseFromUserOrRole
          | showDatabasePrivileges
          | setMainDatabase
          | grantImpersonateUser
          | denyImpersonateUser
          ;

replicationQuery : setReplicationRole
                 | registerReplica
                 | dropReplica
                 ;

replicationInfoQuery : showReplicationRole
                     | showReplicas
                     ;

coordinatorQuery : registerInstanceOnCoordinator
                 | unregisterInstanceOnCoordinator
                 | setInstanceToMain
                 | showInstance
                 | showInstances
                 | addCoordinatorInstance
                 | removeCoordinatorInstance
                 | forceResetClusterStateOnCoordinator
                 | demoteInstanceOnCoordinator
                 | yieldLeadership
                 | setCoordinatorSetting
                 | showCoordinatorSettings
                 | showReplicationLag
                 ;

triggerQuery : createTrigger
             | dropTrigger
             | showTriggers
             ;

clause : cypherMatch
       | unwind
       | merge
       | create
       | set
       | cypherDelete
       | remove
       | with
       | cypherReturn
       | callProcedure
       | loadCsv
       | foreach
       | callSubquery
       | loadParquet
       ;

updateClause : set
             | remove
             | create
             | merge
             | cypherDelete
             | foreach
             ;

foreach :  FOREACH '(' variable IN expression '|' updateClause+  ')' ;

preQueryDirectives: USING preQueryDirective ( ',' preQueryDirective )* ;

preQueryDirective: hopsLimit | indexHints  | periodicCommit ;

hopsLimit: HOPS LIMIT literal ;

indexHints: INDEX indexHint ( ',' indexHint )* ;

indexHint: ':' labelName ( '(' nestedPropertyKeyNames ( ',' nestedPropertyKeyNames )*  ')' )? ;

periodicCommit : PERIODIC COMMIT periodicCommitNumber=literal ;

periodicSubquery : IN TRANSACTIONS OF_TOKEN periodicCommitNumber=literal ROWS ;

callSubquery : CALL '{' cypherQuery '}' ( periodicSubquery )? ;

streamQuery : checkStream
            | createStream
            | dropStream
            | startStream
            | startAllStreams
            | stopStream
            | stopAllStreams
            | showStreams
            ;

databaseName : symbolicName ;

wildcardName : ASTERISK | symbolicName ;

settingQuery : setSetting
             | showSetting
             | showSettings
             ;

transactionQueueQuery : showTransactions
                      | terminateTransactions
                      ;

showTransactions : SHOW TRANSACTIONS ;

terminateTransactions : TERMINATE TRANSACTIONS transactionIdList;

loadCsv : LOAD CSV FROM csvFile ( WITH | NO ) HEADER
         ( IGNORE BAD ) ?
         ( DELIMITER delimiter ) ?
         ( QUOTE quote ) ?
         ( NULLIF nullif ) ?
         AS rowVar ;

loadParquet : LOAD PARQUET FROM parquetFile AS rowVar ;

csvFile : literal | parameter ;

parquetFile : literal | parameter ;

delimiter : literal ;

quote : literal ;

nullif : literal ;

rowVar : variable ;

userOrRoleName : symbolicName ;

createRole : CREATE ROLE ifNotExists? role=userOrRoleName ;

dropRole : DROP ROLE role=userOrRoleName ;

showRoles : SHOW ROLES ;

createUser : CREATE USER ifNotExists? user=userOrRoleName
             ( IDENTIFIED BY password=literal )? ;

ifNotExists : IF NOT EXISTS ;

setPassword : SET PASSWORD FOR user=userOrRoleName TO password=literal;

changePassword : SET PASSWORD TO newPassword=literal REPLACE oldPassword=literal;

dropUser : DROP USER user=userOrRoleName ;

showCurrentUser : SHOW CURRENT USER ;

showCurrentRole : SHOW CURRENT ( ROLE | ROLES ) ;

showUsers : SHOW USERS ;

setRole : SET ( ROLE | ROLES ) FOR user=userOrRoleName TO roles=listOfSymbolicNames ( ON db=listOfSymbolicNames )? ;

clearRole : CLEAR ( ROLE | ROLES ) FOR user=userOrRoleName ( ON db=listOfSymbolicNames )? ;

grantPrivilege : GRANT ( ALL PRIVILEGES | privileges=fineGrainedPrivilegesList ) TO userOrRole=userOrRoleName ;

denyPrivilege : DENY ( ALL PRIVILEGES | privileges=fineGrainedPrivilegesList ) TO userOrRole=userOrRoleName ;

revokePrivilege : REVOKE ( ALL PRIVILEGES | privileges=fineGrainedPrivilegesList ) FROM userOrRole=userOrRoleName ;

listOfSymbolicNames : symbolicName ( ',' symbolicName )* ;

wildcardListOfSymbolicNames : '*' | listOfSymbolicNames ;

grantImpersonateUser : GRANT IMPERSONATE_USER targets=wildcardListOfSymbolicNames TO userOrRole=userOrRoleName ;

denyImpersonateUser : DENY IMPERSONATE_USER targets=wildcardListOfSymbolicNames TO userOrRole=userOrRoleName ;

grantDatabaseToUserOrRole : GRANT DATABASE db=wildcardName TO userOrRole=userOrRoleName ;

denyDatabaseFromUserOrRole : DENY DATABASE db=wildcardName FROM userOrRole=userOrRoleName ;

revokeDatabaseFromUserOrRole : REVOKE DATABASE db=wildcardName FROM userOrRole=userOrRoleName ;

showDatabasePrivileges : SHOW DATABASE PRIVILEGES FOR userOrRole=userOrRoleName ;

setMainDatabase : SET MAIN DATABASE db=symbolicName FOR userOrRole=userOrRoleName ;

setSessionTraceQuery : SET SESSION TRACE (ON | OFF) ;

privilege : CREATE
          | DELETE
          | MATCH
          | MERGE
          | SET
          | REMOVE
          | INDEX
          | STATS
          | AUTH
          | CONSTRAINT
          | DUMP
          | REPLICATION
          | READ_FILE
          | FREE_MEMORY
          | TRIGGER
          | CONFIG
          | DURABILITY
          | STREAM
          | MODULE_READ
          | MODULE_WRITE
          | WEBSOCKET
          | TRANSACTION_MANAGEMENT
          | STORAGE_MODE
          | MULTI_DATABASE_EDIT
          | MULTI_DATABASE_USE
          | COORDINATOR
          | IMPERSONATE_USER
          | PROFILE_RESTRICTION
          ;

granularPrivilege : NOTHING | READ | UPDATE | CREATE | DELETE ;

privilegeOrEntityPrivileges : privilege | entityPrivileges=entityPrivilegeList ;

fineGrainedPrivilegesList : privilegeOrEntityPrivileges ( ',' privilegeOrEntityPrivileges )* ;

entityPrivilegeList : entityPrivilege ( ',' entityPrivilege )* ;

entityPrivilege : granularPrivilege ON entityTypeSpec ;

entityTypeSpec
    : NODES CONTAINING LABELS labelEntities=labelEntitiesList matchingClause?
    | EDGES CONTAINING TYPES edgeType=edgeTypeEntity
    ;

labelEntitiesList : ASTERISK | listOfColonSymbolicNames ;

edgeTypeEntity : ASTERISK | colonSymbolicName ;

matchingClause
    : MATCHING ( ANY | EXACTLY )
    ;

privilegesList : privilege ( ',' privilege )* ;

listOfColonSymbolicNames : colonSymbolicName ( ',' colonSymbolicName )* ;

colonSymbolicName : COLON symbolicName ;

showPrivileges : SHOW PRIVILEGES FOR userOrRole=userOrRoleName ( ON ( MAIN | CURRENT | DATABASE db=symbolicName ) )? ;

showRoleForUser : SHOW ( ROLE | ROLES ) FOR user=userOrRoleName ( ON ( MAIN | CURRENT | DATABASE db=symbolicName ) )? ;

showUsersForRole : SHOW USERS FOR role=userOrRoleName ;

dumpQuery : DUMP DATABASE ;

analyzeGraphQuery : ANALYZE GRAPH ( ON LABELS ( listOfColonSymbolicNames | ASTERISK ) ) ? ( DELETE STATISTICS ) ? ;

setReplicationRole : SET REPLICATION ROLE TO ( MAIN | REPLICA )
                      ( WITH PORT port=literal ) ? ;

showReplicationRole : SHOW REPLICATION ROLE ;

showInstance : SHOW INSTANCE ;
showInstances : SHOW INSTANCES ;

instanceName : symbolicName ;

socketAddress : literal ;

registerReplica : REGISTER REPLICA instanceName ( SYNC | ASYNC | STRICT_SYNC )
                TO socketAddress ;

configKeyValuePair : literal ':' literal ;

configMap : '{' ( configKeyValuePair ( ',' configKeyValuePair )* )? '}' ;

registerInstanceOnCoordinator : REGISTER INSTANCE instanceName ( AS ASYNC | AS STRICT_SYNC ) ? WITH CONFIG configsMap=configMap ;

unregisterInstanceOnCoordinator : UNREGISTER INSTANCE instanceName ;

forceResetClusterStateOnCoordinator : FORCE RESET CLUSTER STATE ;

demoteInstanceOnCoordinator : DEMOTE INSTANCE instanceName ;

setInstanceToMain : SET INSTANCE instanceName TO MAIN ;

yieldLeadership : YIELD LEADERSHIP ;

setCoordinatorSetting: SET COORDINATOR SETTING settingName TO settingValue ;

showCoordinatorSettings: SHOW COORDINATOR SETTINGS ;

showReplicationLag: SHOW REPLICATION LAG ;

coordinatorServerId : literal ;

addCoordinatorInstance : ADD COORDINATOR coordinatorServerId WITH CONFIG configsMap=configMap ;

removeCoordinatorInstance : REMOVE COORDINATOR coordinatorServerId ;

dropReplica : DROP REPLICA instanceName ;

showReplicas : SHOW REPLICAS ;

lockPathQuery : ( LOCK | UNLOCK ) DATA DIRECTORY | DATA DIRECTORY LOCK STATUS;

freeMemoryQuery : FREE MEMORY ;

triggerName : symbolicName ;

triggerStatement : .*? ;

emptyVertex : '(' ')' ;

emptyEdge : dash dash rightArrowHead ;

createTrigger : CREATE TRIGGER triggerName ( ON ( emptyVertex | emptyEdge ) ? ( CREATE | UPDATE | DELETE ) ) ?
              ( AFTER | BEFORE ) COMMIT EXECUTE triggerStatement ;

dropTrigger : DROP TRIGGER triggerName ;

showTriggers : SHOW TRIGGERS | SHOW TRIGGER INFO ;

isolationLevel : SNAPSHOT ISOLATION | READ COMMITTED | READ UNCOMMITTED ;

isolationLevelScope : GLOBAL | SESSION | NEXT ;

isolationLevelQuery : SET isolationLevelScope TRANSACTION ISOLATION LEVEL isolationLevel ;

storageMode : IN_MEMORY_ANALYTICAL | IN_MEMORY_TRANSACTIONAL | ON_DISK_TRANSACTIONAL ;

storageModeQuery : STORAGE MODE storageMode ;

createSnapshotQuery : CREATE SNAPSHOT ;

recoverSnapshotQuery : RECOVER SNAPSHOT path=literal ( FORCE )? ;

showSnapshotsQuery : SHOW SNAPSHOTS ;

showNextSnapshotQuery : SHOW NEXT SNAPSHOT ;

streamName : symbolicName ;

symbolicNameWithMinus : symbolicName ( MINUS symbolicName )* ;

symbolicNameWithDotsAndMinus : symbolicNameWithMinus ( DOT symbolicNameWithMinus )* ;

symbolicTopicNames : symbolicNameWithDotsAndMinus ( COMMA symbolicNameWithDotsAndMinus )* ;

topicNames : symbolicTopicNames | literal ;

commonCreateStreamConfig : TRANSFORM transformationName=procedureName
                         | BATCH_INTERVAL batchInterval=literal
                         | BATCH_SIZE batchSize=literal
                         ;

createStream : kafkaCreateStream | pulsarCreateStream ;

kafkaCreateStreamConfig : TOPICS topicNames
                        | CONSUMER_GROUP consumerGroup=symbolicNameWithDotsAndMinus
                        | BOOTSTRAP_SERVERS bootstrapServers=literal
                        | CONFIGS configsMap=configMap
                        | CREDENTIALS credentialsMap=configMap
                        | commonCreateStreamConfig
                        ;

kafkaCreateStream : CREATE KAFKA STREAM streamName ( kafkaCreateStreamConfig ) * ;


pulsarCreateStreamConfig : TOPICS topicNames
                         | SERVICE_URL serviceUrl=literal
                         | commonCreateStreamConfig
                         ;

pulsarCreateStream : CREATE PULSAR STREAM streamName ( pulsarCreateStreamConfig ) * ;

dropStream : DROP STREAM streamName ;

startStream : START STREAM streamName ( BATCH_LIMIT batchLimit=literal ) ? ( TIMEOUT timeout=literal ) ? ;

startAllStreams : START ALL STREAMS ;

stopStream : STOP STREAM streamName ;

stopAllStreams : STOP ALL STREAMS ;

showStreams : SHOW STREAMS ;

checkStream : CHECK STREAM streamName ( BATCH_LIMIT batchLimit=literal ) ? ( TIMEOUT timeout=literal ) ? ;

settingName : literal ;

settingValue : literal ;

setSetting : SET DATABASE SETTING settingName TO settingValue ;

showSetting : SHOW DATABASE SETTING settingName ;

showSettings : SHOW DATABASE SETTINGS ;

showConfigQuery : SHOW CONFIG ;

versionQuery : SHOW VERSION ;

transactionIdList : transactionId ( ',' transactionId )* ;

transactionId : literal ;

multiDatabaseQuery : createDatabase
                   | dropDatabase
                   | renameDatabase
                   ;

createDatabase : CREATE DATABASE databaseName ;

dropDatabase: DROP DATABASE databaseName ( FORCE)?;

renameDatabase : RENAME DATABASE databaseName TO databaseName ;

useDatabase : USE DATABASE databaseName ;

showDatabase : SHOW ( CURRENT )? DATABASE ;

showDatabases : SHOW DATABASES ;

edgeImportModeQuery : EDGE IMPORT MODE ( ACTIVE | INACTIVE ) ;

createEdgeIndex : CREATE EDGE INDEX ON ':' labelName ( '(' propertyKeyName ')' )?;

dropEdgeIndex : DROP EDGE INDEX ON ':' labelName ( '(' propertyKeyName ')' )?;

createGlobalEdgeIndex : CREATE GLOBAL EDGE INDEX ON ':' ( '(' propertyKeyName ')' )?;

dropGlobalEdgeIndex : DROP GLOBAL EDGE INDEX ON ':' ( '(' propertyKeyName ')' )?;

edgeIndexQuery : createEdgeIndex | dropEdgeIndex | createGlobalEdgeIndex | dropGlobalEdgeIndex;

indexName : symbolicName ;

createTextIndex : CREATE TEXT INDEX indexName ON ':' labelName ( '(' propertyKeyName ( ',' propertyKeyName )* ')' )* ;

dropTextIndex : DROP TEXT INDEX indexName ;

textIndexQuery : createTextIndex | dropTextIndex;

createTextEdgeIndex: CREATE TEXT EDGE INDEX indexName ON ':' labelName ( '(' propertyKeyName ( ',' propertyKeyName )* ')' )* ;

createPointIndex : CREATE POINT INDEX ON ':' labelName '(' propertyKeyName ')';

dropPointIndex : DROP POINT INDEX ON ':' labelName '(' propertyKeyName ')' ;

pointIndexQuery : createPointIndex | dropPointIndex ;

createVectorIndex : CREATE VECTOR INDEX indexName ON ':' labelName ( '(' propertyKeyName ')' )? WITH CONFIG configsMap=configMap ;

createVectorEdgeIndex: CREATE VECTOR EDGE INDEX indexName ON ':' labelName ( '(' propertyKeyName ')' )? WITH CONFIG configsMap=configMap ;

dropVectorIndex : DROP VECTOR INDEX indexName ;

vectorIndexQuery : createVectorIndex | dropVectorIndex ;

dropAllIndexesQuery : DROP ALL INDEXES ;

dropAllConstraintsQuery : DROP ALL CONSTRAINTS ;

dropGraphQuery : DROP GRAPH ;

enumName : symbolicName ;

enumValue : symbolicName ;

createEnumQuery : CREATE ENUM enumName VALUES '{' enumValue ( ',' enumValue )* '}' ;

showEnumsQuery : SHOW ENUMS ;

alterEnumAddValueQuery: ALTER ENUM enumName ADD VALUE enumValue ;

alterEnumUpdateValueQuery: ALTER ENUM enumName UPDATE VALUE old_value=enumValue TO new_value=enumValue ;

alterEnumRemoveValueQuery: ALTER ENUM enumName REMOVE VALUE removed_value=enumValue ;

dropEnumQuery: DROP ENUM enumName ;

showSchemaInfoQuery : SHOW SCHEMA INFO ;

stopTtlQuery: ( DISABLE | STOP ) TTL ;

startTtlQuery: ENABLE TTL ( ( EVERY period=literal ) ( AT time=literal )?
                           | ( AT time=literal ) ( EVERY period=literal )? )? ;

ttlQuery: stopTtlQuery
        | startTtlQuery
        ;

typeConstraintType : BOOLEAN
             | STRING
             | INTEGER
             | FLOAT
             | LIST
             | MAP
             | DATE
             | LOCALTIME
             | LOCALDATETIME
             | ZONEDDATETIME
             | DURATION
             | ENUM
             | POINT
             ;


memoryLimitValue : literal ( MB | KB ) ;

limitValue : UNLIMITED | mem_limit=memoryLimitValue | quantity=literal ;

limitKV : key=symbolicName val=limitValue ;

listOfLimits : limitKV (',' limitKV )* ;

createUserProfile : ( CREATE | UPDATE ) PROFILE profile=symbolicName ( LIMIT list=listOfLimits )? ;
dropUserProfile : DROP PROFILE profile=symbolicName ;
showUserProfiles : SHOW PROFILES ;
showUserProfile : SHOW PROFILE profile=symbolicName ;
showUserProfileForUser : SHOW PROFILE FOR user=userOrRoleName ;
showUserProfileForProfile : SHOW ( USERS | ROLES ) FOR PROFILE profile=symbolicName ;
setUserProfile : SET PROFILE FOR user=userOrRoleName TO profile=symbolicName ;
clearUserProfile : CLEAR PROFILE FOR user=userOrRoleName ;
showResourceConsumption : SHOW RESOURCE USAGE FOR user=userOrRoleName ;

userProfileQuery : createUserProfile
                 | dropUserProfile
                 | showUserProfiles
                 | showUserProfile
                 | showUserProfileForUser
                 | showUserProfileForProfile
                 | setUserProfile
                 | clearUserProfile
                 | showResourceConsumption
                 ;
