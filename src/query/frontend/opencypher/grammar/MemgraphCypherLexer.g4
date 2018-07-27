/* Memgraph specific Cypher reserved words. */

/*
 * When changing this grammar make sure to update constants in
 * src/query/frontend/stripped_lexer_constants.hpp (kKeywords, kSpecialTokens
 * and bitsets) if needed.
 */

lexer grammar MemgraphCypherLexer ; 

import CypherLexer ;

ALTER          : A L T E R ;
BATCH          : B A T C H ;
BATCHES        : B A T C H E S ;
DATA           : D A T A ;
DROP           : D R O P ;
INTERVAL       : I N T E R V A L ;
K_TEST         : T E S T ;
KAFKA          : K A F K A ;
LOAD           : L O A D ;
PASSWORD       : P A S S W O R D ;
SIZE           : S I Z E ;
START          : S T A R T ;
STOP           : S T O P ;
STREAM         : S T R E A M ;
STREAMS        : S T R E A M S ;
TOPIC          : T O P I C ;
TRANSFORM      : T R A N S F O R M ;
USER           : U S E R ;
