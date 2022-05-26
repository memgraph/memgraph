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
                      | AFTER
                      | ALTER
                      | ASYNC
                      | AUTH
                      | BAD
                      | BATCH_INTERVAL
                      | BATCH_LIMIT
                      | BATCH_SIZE
                      | BEFORE
                      | BOOTSTRAP_SERVERS
                      | CHECK
                      | CLEAR
                      | COMMIT
                      | COMMITTED
                      | CONFIG
                      | CONFIGS
                      | CONSUMER_GROUP
                      | CREDENTIALS
                      | CSV
                      | DATA
                      | DELIMITER
                      | DATABASE
                      | DENY
                      | DROP
                      | DUMP
                      | EXECUTE
                      | FOR
                      | FOREACH
                      | FREE
                      | FROM
                      | GLOBAL
                      | GRANT
                      | HEADER
                      | IDENTIFIED
                      | ISOLATION
                      | KAFKA
                      | LEVEL
                      | LOAD
                      | LOCK
                      | MAIN
                      | MODE
                      | NEXT
                      | NO
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
                      | SCHEMA
                      | SESSION
                      | SETTING
                      | SETTINGS
                      | SNAPSHOT
                      | START
                      | STATS
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
                      | USER
                      | USERS
                      | VERSION
                      ;

symbolicName : UnescapedSymbolicName
             | EscapedSymbolicName
             | memgraphCypherKeyword
             ;

query : cypherQuery
      | indexQuery
      | explainQuery
      | profileQuery
      | infoQuery
      | constraintQuery
      | authQuery
      | dumpQuery
      | replicationQuery
      | lockPathQuery
      | freeMemoryQuery
      | triggerQuery
      | isolationLevelQuery
      | createSnapshotQuery
      | streamQuery
      | settingQuery
      | versionQuery
      | schemaQuery
      ;

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
          ;

replicationQuery : setReplicationRole
                 | showReplicationRole
                 | registerReplica
                 | dropReplica
                 | showReplicas
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
       ;

updateClause : set
             | remove
             | create
             | merge
             | cypherDelete
             | foreach
             ;

foreach :  FOREACH '(' variable IN expression '|' updateClause+  ')' ;

streamQuery : checkStream
            | createStream
            | dropStream
            | startStream
            | startAllStreams
            | stopStream
            | stopAllStreams
            | showStreams
            ;

settingQuery : setSetting
             | showSetting
             | showSettings
             ;

schemaQuery : showSchema
            | showSchemas
            | createSchema
            ;

loadCsv : LOAD CSV FROM csvFile ( WITH | NO ) HEADER
         ( IGNORE BAD ) ?
         ( DELIMITER delimiter ) ?
         ( QUOTE quote ) ?
         AS rowVar ;

csvFile : literal ;

delimiter : literal ;

quote : literal ;

rowVar : variable ;

userOrRoleName : symbolicName ;

createRole : CREATE ROLE role=userOrRoleName ;

dropRole   : DROP ROLE role=userOrRoleName ;

showRoles  : SHOW ROLES ;

createUser : CREATE USER user=userOrRoleName
             ( IDENTIFIED BY password=literal )? ;

setPassword : SET PASSWORD FOR user=userOrRoleName TO password=literal;

dropUser : DROP USER user=userOrRoleName ;

showUsers : SHOW USERS ;

setRole : SET ROLE FOR user=userOrRoleName TO role=userOrRoleName;

clearRole : CLEAR ROLE FOR user=userOrRoleName ;

grantPrivilege : GRANT ( ALL PRIVILEGES | privileges=privilegeList ) TO userOrRole=userOrRoleName ;

denyPrivilege : DENY ( ALL PRIVILEGES | privileges=privilegeList ) TO userOrRole=userOrRoleName ;

revokePrivilege : REVOKE ( ALL PRIVILEGES | privileges=privilegeList ) FROM userOrRole=userOrRoleName ;

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
          | SCHEMA
          ;

privilegeList : privilege ( ',' privilege )* ;

showPrivileges : SHOW PRIVILEGES FOR userOrRole=userOrRoleName ;

showRoleForUser : SHOW ROLE FOR user=userOrRoleName ;

showUsersForRole : SHOW USERS FOR role=userOrRoleName ;

dumpQuery: DUMP DATABASE ;

setReplicationRole  : SET REPLICATION ROLE TO ( MAIN | REPLICA )
                      ( WITH PORT port=literal ) ? ;

showReplicationRole : SHOW REPLICATION ROLE ;

replicaName : symbolicName ;

socketAddress : literal ;

registerReplica : REGISTER REPLICA replicaName ( SYNC | ASYNC )
                ( WITH TIMEOUT timeout=literal ) ?
                TO socketAddress ;

dropReplica : DROP REPLICA replicaName ;

showReplicas  : SHOW REPLICAS ;

lockPathQuery : ( LOCK | UNLOCK ) DATA DIRECTORY ;

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

createSnapshotQuery : CREATE SNAPSHOT ;

streamName : symbolicName ;

symbolicNameWithMinus : symbolicName ( MINUS symbolicName )* ;

symbolicNameWithDotsAndMinus: symbolicNameWithMinus ( DOT symbolicNameWithMinus )* ;

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

startStream : START STREAM streamName ;

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

versionQuery : SHOW VERSION ;

showSchema : SHOW SCHEMA ON ':' labelName;

showSchemas : SHOW SCHEMAS;

createSchema : CREATE SCHEMA ON ':' labelName schemaPropertyList;

schemaPropertyList : propertyKeyName propertyType ( ',' propertyKeyName propertyType )* ;

propertyType : BOOL
             | DATE
             | DURATION
             | FLOAT
             | INTEGER
             | LIST
             | LOCALDATETIME
             | LOCALTIME
             | MAP
             | STRING
             ;
