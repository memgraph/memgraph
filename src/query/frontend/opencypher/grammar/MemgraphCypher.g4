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
                      | BATCHES
                      | BATCH_INTERVAL
                      | BATCH_SIZE
                      | BEFORE
                      | CLEAR
                      | COMMIT
                      | COMMITTED
                      | CONFIG
                      | CONSUMER_GROUP
                      | CSV
                      | DATA
                      | DELIMITER
                      | DATABASE
                      | DENY
                      | DROP
                      | DUMP
                      | EXECUTE
                      | FOR
                      | FREE
                      | FROM
                      | GLOBAL
                      | GRANT
                      | HEADER
                      | IDENTIFIED
                      | ISOLATION
                      | LEVEL
                      | LOAD
                      | LOCK
                      | MAIN
                      | MODE
                      | NEXT
                      | NO
                      | PASSWORD
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
       ;

streamQuery : createStream
            | dropStream
            | startStream
            | startAllStreams
            | stopStream
            | stopAllStreams
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

symbolicNameWithDots : symbolicName ( DOT symbolicName )* ;

topicNames : symbolicNameWithDots ( COMMA symbolicNameWithDots )* ;

createStream : CREATE STREAM streamName
               TOPICS topicNames
               TRANSFORM transformationName=symbolicNameWithDots
               ( CONSUMER_GROUP consumerGroup=symbolicNameWithDots ) ?
               ( BATCH_INTERVAL batchInterval=literal ) ?
               ( BATCH_SIZE batchSize=literal ) ? ;

dropStream : DROP STREAM streamName ;

startStream : START STREAM streamName ;

startAllStreams : START ALL STREAMS ;

stopStream : STOP STREAM streamName ;

stopAllStreams : STOP ALL STREAMS ;
