/* Memgraph specific part of Cypher grammar. */

parser grammar MemgraphCypher ;

options { tokenVocab=MemgraphCypherLexer; }

import Cypher ;

memgraphCypherKeyword : cypherKeyword
                      | ALTER
                      | BATCH
                      | BATCHES
                      | DATA
                      | DROP
                      | INTERVAL
                      | K_TEST
                      | KAFKA
                      | LOAD
                      | PASSWORD
                      | SIZE
                      | START
                      | STOP
                      | STREAM
                      | STREAMS
                      | TOPIC
                      | TRANSFORM
                      | USER
                      ;

symbolicName : UnescapedSymbolicName
             | EscapedSymbolicName
             | memgraphCypherKeyword
             ;

query : regularQuery
      | authQuery
      | streamQuery
      ;

authQuery : modifyUser
          | dropUser
          ;

modifyUser : ( CREATE | ALTER ) USER userName=UnescapedSymbolicName
             ( WITH ( modifyUserOption )+ )? ;

modifyUserOption : passwordOption ;

passwordOption : PASSWORD literal;

dropUser : DROP USER userName+=UnescapedSymbolicName
           ( ',' userName+=UnescapedSymbolicName )* ;

streamQuery : createStream
            | dropStream
            | showStreams
            | startStopStream
            | startStopAllStreams
            | testStream
            ;

streamName : UnescapedSymbolicName ;

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
