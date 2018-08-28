/* Memgraph specific part of Cypher grammar. */

parser grammar MemgraphCypher ;

options { tokenVocab=MemgraphCypherLexer; }

import Cypher ;

memgraphCypherKeyword : cypherKeyword
                      | ALTER
                      | AUTH
                      | BATCH
                      | BATCHES
                      | DATA
                      | DENY
                      | DROP
                      | FOR
                      | FROM
                      | GRANT
                      | GRANTS
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

explainQuery : EXPLAIN regularQuery ;

authQuery : createRole
          | dropRole
          | showRoles
          | createUser
          | setPassword
          | dropUser
          | showUsers
          | grantRole
          | revokeRole
          | grantPrivilege
          | denyPrivilege
          | revokePrivilege
          | showGrants
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

grantRole : GRANT ROLE role=userOrRoleName TO user=userOrRoleName ;

revokeRole : REVOKE ROLE role=userOrRoleName FROM user=userOrRoleName ;

grantPrivilege : GRANT ( ALL PRIVILEGES | privileges=privilegeList ) TO userOrRole=userOrRoleName ;

denyPrivilege : DENY ( ALL PRIVILEGES | privileges=privilegeList ) TO userOrRole=userOrRoleName ;

revokePrivilege : REVOKE ( ALL PRIVILEGES | privileges=privilegeList ) FROM userOrRole=userOrRoleName ;

privilege : CREATE | DELETE | MATCH | MERGE | SET
          | REMOVE | INDEX | AUTH | STREAM ;

privilegeList : privilege ( ',' privilege )* ;

showGrants : SHOW GRANTS FOR userOrRole=userOrRoleName ;

showRoleForUser : SHOW ROLE FOR USER user=userOrRoleName ;

showUsersForRole : SHOW USERS FOR ROLE role=userOrRoleName ;

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
