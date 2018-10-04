/* Memgraph specific part of Cypher grammar with enterprise features. */

parser grammar MemgraphCypher ;

options { tokenVocab=MemgraphCypherLexer; }

import Cypher ;

memgraphCypherKeyword : cypherKeyword
                      | ALTER
                      | AUTH
                      | BATCH
                      | BATCHES
                      | CLEAR
                      | DATA
                      | DENY
                      | DROP
                      | FOR
                      | FROM
                      | GRANT
                      | IDENTIFIED
                      | INTERVAL
                      | K_TEST
                      | KAFKA
                      | LOAD
                      | PASSWORD
                      | PRIVILEGES
                      | REVOKE
                      | ROLE
                      | ROLES
                      | SIZE
                      | START
                      | STOP
                      | STREAM
                      | STREAMS
                      | TO
                      | TOPIC
                      | TRANSFORM
                      | USER
                      | USERS
                      ;

symbolicName : UnescapedSymbolicName
             | EscapedSymbolicName
             | memgraphCypherKeyword
             ;

query : regularQuery
      | authQuery
      | streamQuery
      | explainQuery
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

privilege : CREATE | DELETE | MATCH | MERGE | SET
          | REMOVE | INDEX | AUTH | STREAM ;

privilegeList : privilege ( ',' privilege )* ;

showPrivileges : SHOW PRIVILEGES FOR userOrRole=userOrRoleName ;

showRoleForUser : SHOW ROLE FOR user=userOrRoleName ;

showUsersForRole : SHOW USERS FOR role=userOrRoleName ;

streamQuery : createStream
            | dropStream
            | showStreams
            | startStopStream
            | startStopAllStreams
            | testStream
            ;

streamName : symbolicName ;

createStream : CREATE STREAM streamName AS LOAD DATA KAFKA
streamUri=literal WITH TOPIC streamTopic=literal WITH TRANSFORM
transformUri=literal ( batchIntervalOption )? ( batchSizeOption )? ;

batchIntervalOption : BATCH INTERVAL literal ;

batchSizeOption : BATCH SIZE literal ;

dropStream : DROP STREAM streamName ;

showStreams : SHOW STREAMS ;

startStopStream : ( START | STOP ) STREAM streamName ( limitBatchesOption )? ;

limitBatchesOption : LIMIT limitBatches=literal BATCHES ;

startStopAllStreams : ( START | STOP ) ALL STREAMS ;

testStream : K_TEST STREAM streamName ( limitBatchesOption )? ;
