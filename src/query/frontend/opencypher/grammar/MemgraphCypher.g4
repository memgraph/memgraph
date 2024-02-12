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

memgraphCypherKeyword : cypherKeyword
                      | ADD
                      | ACTIVE
                      | AFTER
                      | ALTER
                      | ANALYZE
                      | ASYNC
                      | AUTH
                      | BAD
                      | BATCH_INTERVAL
                      | BATCH_LIMIT
                      | BATCH_SIZE
                      | BEFORE
                      | BOOTSTRAP_SERVERS
                      | BUILD
                      | CHECK
                      | CLEAR
                      | COMMIT
                      | COMMITTED
                      | CONFIG
                      | CONFIGS
                      | CONSUMER_GROUP
                      | CREATE_DELETE
                      | CREDENTIALS
                      | CSV
                      | DATA
                      | DELIMITER
                      | DATABASE
                      | DENY
                      | DROP
                      | DO
                      | DUMP
                      | EDGE
                      | EDGE_TYPES
                      | EXECUTE
                      | FAILOVER
                      | FOR
                      | FOREACH
                      | FREE
                      | FROM
                      | GLOBAL
                      | GRAPH
                      | GRANT
                      | HEADER
                      | IDENTIFIED
                      | INSTANCE
                      | INSTANCES
                      | NODE_LABELS
                      | NULLIF
                      | IMPORT
                      | INACTIVE
                      | IN_MEMORY_ANALYTICAL
                      | IN_MEMORY_TRANSACTIONAL
                      | ISOLATION
                      | KAFKA
                      | LABELS
                      | LEVEL
                      | LOAD
                      | LOCK
                      | MAIN
                      | MODE
                      | NEXT
                      | NO
                      | NOTHING
                      | PASSWORD
                      | PULSAR
                      | PORT
                      | PRIVILEGES
                      | READ
                      | REGISTER
                      | REPLICA
                      | REPLICAS
                      | REPLICATION
                      | REVOKE
                      | ROLE
                      | ROLES
                      | QUOTE
                      | SESSION
                      | SETTING
                      | SETTINGS
                      | SNAPSHOT
                      | START
                      | STATS
                      | STATUS
                      | STORAGE
                      | STREAM
                      | STREAMS
                      | SYNC
                      | TIMEOUT
                      | TO
                      | TOPICS
                      | TRANSACTION
                      | TRANSFORM
                      | TRIGGER
                      | TRIGGERS
                      | UNCOMMITTED
                      | UNLOCK
                      | UPDATE
                      | USE
                      | USER
                      | USERS
                      | USING
                      | VERSION
                      | TERMINATE
                      | TRANSACTIONS
                      ;

symbolicName : UnescapedSymbolicName
             | EscapedSymbolicName
             | memgraphCypherKeyword
             ;

query : cypherQuery
      | indexQuery
      | explainQuery
      | profileQuery
      | databaseInfoQuery
      | systemInfoQuery
      | constraintQuery
      | authQuery
      | dumpQuery
      | analyzeGraphQuery
      | replicationQuery
      | lockPathQuery
      | freeMemoryQuery
      | triggerQuery
      | isolationLevelQuery
      | storageModeQuery
      | createSnapshotQuery
      | streamQuery
      | settingQuery
      | versionQuery
      | showConfigQuery
      | transactionQueueQuery
      | multiDatabaseQuery
      | showDatabases
      | edgeImportModeQuery
      | coordinatorQuery
      ;

cypherQuery : ( indexHints )? singleQuery ( cypherUnion )* ( queryMemoryLimit )? ;

authQuery : createRole
          | dropRole
          | showRoles
          | createUser
          | setPassword
          | dropUser
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
          ;

replicationQuery : setReplicationRole
                 | showReplicationRole
                 | registerReplica
                 | dropReplica
                 | showReplicas
                 ;

coordinatorQuery : registerInstanceOnCoordinator
                 | unregisterInstanceOnCoordinator
                 | setInstanceToMain
                 | showInstances
                 | addCoordinatorInstance
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
       ;

updateClause : set
             | remove
             | create
             | merge
             | cypherDelete
             | foreach
             ;

foreach :  FOREACH '(' variable IN expression '|' updateClause+  ')' ;

indexHints: USING INDEX indexHint ( ',' indexHint )* ;

indexHint: ':' labelName ( '(' propertyKeyName ')' )? ;

callSubquery : CALL '{' cypherQuery '}' ;

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

csvFile : literal | parameter ;

delimiter : literal ;

quote : literal ;

nullif : literal ;

rowVar : variable ;

userOrRoleName : symbolicName ;

createRole : CREATE ROLE role=userOrRoleName ;

dropRole : DROP ROLE role=userOrRoleName ;

showRoles : SHOW ROLES ;

createUser : CREATE USER user=userOrRoleName
             ( IDENTIFIED BY password=literal )? ;

setPassword : SET PASSWORD FOR user=userOrRoleName TO password=literal;

dropUser : DROP USER user=userOrRoleName ;

showUsers : SHOW USERS ;

setRole : SET ROLE FOR user=userOrRoleName TO role=userOrRoleName;

clearRole : CLEAR ROLE FOR user=userOrRoleName ;

grantPrivilege : GRANT ( ALL PRIVILEGES | privileges=grantPrivilegesList ) TO userOrRole=userOrRoleName ;

denyPrivilege : DENY ( ALL PRIVILEGES | privileges=privilegesList ) TO userOrRole=userOrRoleName ;

revokePrivilege : REVOKE ( ALL PRIVILEGES | privileges=revokePrivilegesList ) FROM userOrRole=userOrRoleName ;

grantDatabaseToUserOrRole : GRANT DATABASE db=wildcardName TO userOrRole=userOrRoleName ;

denyDatabaseFromUserOrRole : DENY DATABASE db=wildcardName FROM userOrRole=userOrRoleName ;

revokeDatabaseFromUserOrRole : REVOKE DATABASE db=wildcardName FROM userOrRole=userOrRoleName ;

showDatabasePrivileges : SHOW DATABASE PRIVILEGES FOR userOrRole=userOrRoleName ;

setMainDatabase : SET MAIN DATABASE db=symbolicName FOR userOrRole=userOrRoleName ;

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
          ;

granularPrivilege : NOTHING | READ | UPDATE | CREATE_DELETE ;

entityType : LABELS | EDGE_TYPES ;

privilegeOrEntityPrivileges : privilege | entityPrivileges=entityPrivilegeList ;

grantPrivilegesList : privilegeOrEntityPrivileges ( ',' privilegeOrEntityPrivileges )* ;

entityPrivilegeList : entityPrivilege ( ',' entityPrivilege )* ;

entityPrivilege : granularPrivilege ON entityType entities=entitiesList ;

privilegeOrEntities : privilege | entityType entities=entitiesList ;

revokePrivilegesList : privilegeOrEntities ( ',' privilegeOrEntities )* ;

privilegesList : privilege ( ',' privilege )* ;

entitiesList : ASTERISK | listOfColonSymbolicNames ;

listOfColonSymbolicNames : colonSymbolicName ( ',' colonSymbolicName )* ;

colonSymbolicName : COLON symbolicName ;

showPrivileges : SHOW PRIVILEGES FOR userOrRole=userOrRoleName ;

showRoleForUser : SHOW ROLE FOR user=userOrRoleName ;

showUsersForRole : SHOW USERS FOR role=userOrRoleName ;

dumpQuery : DUMP DATABASE ;

analyzeGraphQuery : ANALYZE GRAPH ( ON LABELS ( listOfColonSymbolicNames | ASTERISK ) ) ? ( DELETE STATISTICS ) ? ;

setReplicationRole : SET REPLICATION ROLE TO ( MAIN | REPLICA )
                      ( WITH PORT port=literal ) ? ;

showReplicationRole : SHOW REPLICATION ROLE ;

showInstances : SHOW INSTANCES ;

instanceName : symbolicName ;

socketAddress : literal ;

coordinatorSocketAddress : literal ;
replicationSocketAddress : literal ;
raftSocketAddress : literal ;

registerReplica : REGISTER REPLICA instanceName ( SYNC | ASYNC )
                TO socketAddress ;

registerInstanceOnCoordinator : REGISTER INSTANCE instanceName ON coordinatorSocketAddress ( AS ASYNC ) ? WITH replicationSocketAddress ;

unregisterInstanceOnCoordinator : UNREGISTER INSTANCE instanceName ;

setInstanceToMain : SET INSTANCE instanceName TO MAIN ;

raftServerId : literal ;

addCoordinatorInstance : ADD COORDINATOR raftServerId ON raftSocketAddress ;

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

showTriggers : SHOW TRIGGERS ;

isolationLevel : SNAPSHOT ISOLATION | READ COMMITTED | READ UNCOMMITTED ;

isolationLevelScope : GLOBAL | SESSION | NEXT ;

isolationLevelQuery : SET isolationLevelScope TRANSACTION ISOLATION LEVEL isolationLevel ;

storageMode : IN_MEMORY_ANALYTICAL | IN_MEMORY_TRANSACTIONAL | ON_DISK_TRANSACTIONAL ;

storageModeQuery : STORAGE MODE storageMode ;

createSnapshotQuery : CREATE SNAPSHOT ;

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

configKeyValuePair : literal ':' literal ;

configMap : '{' ( configKeyValuePair ( ',' configKeyValuePair )* )? '}' ;

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
                   | useDatabase
                   | dropDatabase
                   | showDatabase
                   ;

createDatabase : CREATE DATABASE databaseName ;

useDatabase : USE DATABASE databaseName ;

dropDatabase : DROP DATABASE databaseName ;

showDatabase : SHOW DATABASE ;

showDatabases : SHOW DATABASES ;

edgeImportModeQuery : EDGE IMPORT MODE ( ACTIVE | INACTIVE ) ;
